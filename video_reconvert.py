#!/usr/bin/env python3
import os
import sys
import subprocess
import sqlite3
import argparse
import logging
import time
import json
import signal
import glob
import select
from pathlib import Path
from datetime import datetime
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table
from rich import print as rprint
from logging.handlers import RotatingFileHandler
import config
import video_utils

# Global variables for cleanup
current_temp_file = None
exit_requested = False
console = Console()

# Configure logging
log_level = getattr(logging, config.LOGGING['level'])
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# File handler with rotation - set to DEBUG to capture all details
file_handler = RotatingFileHandler(
    config.LOGGING['filename'],
    maxBytes=config.LOGGING['max_size'],
    backupCount=config.LOGGING['backup_count']
)
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.DEBUG)  # Always capture debug in file

# Console handler with rich formatting - use configured level
console_handler = RichHandler(rich_tracebacks=True)
console_handler.setLevel(log_level)  # Use configured level for console

# Set root logger to DEBUG to allow everything through
logging.basicConfig(
    level=logging.DEBUG,  # Capture everything, handlers will filter
    format="%(message)s",
    datefmt="[%X]",
    handlers=[console_handler, file_handler]
)
log = logging.getLogger("video_reconvert")

# Signal handler for graceful exit
def handle_exit_signal(sig, frame):
    global exit_requested, current_temp_file
    exit_requested = True
    
    log.info("Interrupt received, cleaning up and exiting gracefully...")
    console.print(f"[{config.UI['info_style']}]Interrupt received, cleaning up and exiting gracefully...[/{config.UI['info_style']}]")
    
    # Clean up current temp file if it exists
    if current_temp_file and os.path.exists(current_temp_file):
        try:
            log.info(f"Removing temp file: {current_temp_file}")
            os.remove(current_temp_file)
            console.print(f"[{config.UI['info_style']}]Removed temp file: {current_temp_file}[/{config.UI['info_style']}]")
        except Exception as e:
            log.error(f"Failed to remove temp file {current_temp_file}: {str(e)}")
    
    console.print(f"[{config.UI['header_style']}]Exiting video_reconvert...[/{config.UI['header_style']}]")
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, handle_exit_signal)  # Ctrl+C
signal.signal(signal.SIGTERM, handle_exit_signal) # Termination signal

# Function to find temp files
def find_temp_files(source_path):
    """Find any temporary files from previous interrupted runs."""
    temp_files = []
    for root, _, files in os.walk(source_path):
        for file in files:
            if file.startswith("temp_") and any(file.endswith(ext) for ext in config.FILE_SEARCH['extensions']):
                temp_files.append(os.path.join(root, file))
    return temp_files

# Function to clean up temp files
def clean_temp_files(temp_files):
    """Remove temporary files."""
    if not temp_files:
        return
    
    for temp_file in temp_files:
        try:
            log.info(f"Removing temp file: {temp_file}")
            os.remove(temp_file)
        except Exception as e:
            log.error(f"Failed to remove temp file {temp_file}: {str(e)}")

# Database setup
def setup_database():
    """Initialize the SQLite database to track conversion progress."""
    conn = sqlite3.connect(config.DATABASE['filename'])
    cursor = conn.cursor()
    
    # Create tables if they don't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS videos (
        id INTEGER PRIMARY KEY,
        filepath TEXT UNIQUE,
        original_size INTEGER,
        new_size INTEGER,
        original_duration REAL,
        original_width INTEGER,
        original_height INTEGER,
        original_bitrate INTEGER,
        new_bitrate INTEGER,
        status TEXT,
        date_added TIMESTAMP,
        date_completed TIMESTAMP,
        error_message TEXT,
        skip_reason TEXT,
        custom_settings TEXT
    )
    ''')
    
    # Create schema version table if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS schema_version (
        id INTEGER PRIMARY KEY,
        version INTEGER,
        updated_at TIMESTAMP
    )
    ''')
    
    # Check current schema version
    cursor.execute("SELECT version FROM schema_version ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()
    current_version = result[0] if result else 0
    
    # Current schema version
    SCHEMA_VERSION = 1
    
    if current_version < SCHEMA_VERSION:
        log.info(f"Updating database schema from version {current_version} to {SCHEMA_VERSION}")
        
        # Verify required columns exist
        try:
            # Check if we need to add the skip_reason column
            cursor.execute("SELECT skip_reason FROM videos LIMIT 1")
        except sqlite3.OperationalError:
            log.info("Adding skip_reason column to videos table")
            cursor.execute("ALTER TABLE videos ADD COLUMN skip_reason TEXT")
        
        try:
            # Check if we need to add the custom_settings column
            cursor.execute("SELECT custom_settings FROM videos LIMIT 1")
        except sqlite3.OperationalError:
            log.info("Adding custom_settings column to videos table")
            cursor.execute("ALTER TABLE videos ADD COLUMN custom_settings TEXT")
        
        # Update schema version
        cursor.execute('''
        INSERT INTO schema_version (version, updated_at)
        VALUES (?, ?)
        ''', (SCHEMA_VERSION, datetime.now()))
        
        log.info(f"Database schema updated to version {SCHEMA_VERSION}")
    
    conn.commit()
    return conn

def get_video_info(filepath):
    """Use ffprobe to extract video metadata. Wrapper around video_utils function."""
    return video_utils.get_video_info(filepath)

def find_videos(source_path):
    """Find all video files in the source directory and its subdirectories."""
    videos = []
    
    # Convert string extensions to lowercase if case insensitive
    extensions = set(config.FILE_SEARCH['extensions'])
    if not config.FILE_SEARCH['case_sensitive']:
        extensions = set([ext.lower() for ext in extensions])
    
    # Function to check if a file has a matching extension
    def is_video_file(filename):
        if config.FILE_SEARCH['case_sensitive']:
            return any(filename.endswith(ext) for ext in extensions)
        else:
            return any(filename.lower().endswith(ext.lower()) for ext in extensions)
    
    # Function to check if a directory should be skipped
    def should_skip_dir(dirname):
        return any(pattern in dirname for pattern in config.FILE_SEARCH['skip_dirs'])
    
    # Exclude temp files that match our temp file pattern
    def is_temp_file(filename):
        return filename.startswith('.temp_')
    
    for root, dirs, files in os.walk(source_path):
        # Remove directories to skip from the list (in-place)
        dirs[:] = [d for d in dirs if not should_skip_dir(d)]
        
        # Check depth if max_depth is set
        if config.FILE_SEARCH['max_depth'] is not None:
            current_depth = root.count(os.sep) - source_path.count(os.sep)
            if current_depth >= config.FILE_SEARCH['max_depth']:
                dirs[:] = []  # Don't go deeper
        
        for file in files:
            # Skip temp files
            if is_temp_file(file):
                continue
                
            if is_video_file(file):
                videos.append(os.path.join(root, file))
    
    return videos

def register_video(conn, video_path):
    """Add a video to the database if not already present, with sanity checks."""
    cursor = conn.cursor()
    
    # Check if video is already in database
    try:
        cursor.execute("SELECT id, status, skip_reason FROM videos WHERE filepath = ?", (video_path,))
        result = cursor.fetchone()
    except sqlite3.OperationalError as e:
        if "no such column: skip_reason" in str(e):
            log.error("Database schema is outdated. Please run update_database.py first.")
            sys.exit(1)
        else:
            raise
    
    if result:
        video_id, status, skip_reason = result
        if status == 'completed':
            log.info(f"Video already processed: {video_path}")
            return None
        elif status == 'skipped':
            log.info(f"Video previously skipped: {video_path}. Reason: {skip_reason}")
            return None
        elif status == 'error' and not config.DATABASE['retry_errors']:
            log.info(f"Skipping previously failed video: {video_path}")
            return None
        return video_id
    
    # Get comprehensive video info
    info = get_video_info(video_path)
    if not info:
        # Add to database as skipped due to info extraction failure
        cursor.execute("""
        INSERT INTO videos 
        (filepath, status, date_added, skip_reason) 
        VALUES (?, ?, ?, ?)
        """, (
            video_path, 
            'skipped', 
            datetime.now(),
            'Failed to extract video information'
        ))
        conn.commit()
        return None
    
    # Check if this file was already processed by our tool
    if video_utils.was_processed_by_tool(info) and not config.VIDEO_ENCODING['force_reconvert']:
        cursor.execute("""
        INSERT INTO videos 
        (filepath, status, date_added, skip_reason) 
        VALUES (?, ?, ?, ?)
        """, (
            video_path, 
            'skipped', 
            datetime.now(),
            'Already processed by video_reconvert'
        ))
        conn.commit()
        log.info(f"Skipping already processed video: {video_path}")
        return None
    
    # Run sanity checks with force_reconvert flag
    issues = video_utils.run_sanity_checks(info, config.VIDEO_ENCODING['force_reconvert'])
    
    # Check for critical issues (error level)
    critical_issues = [issue for issue in issues if issue['level'] == 'error']
    if critical_issues:
        skip_reason = critical_issues[0]['message']
        if critical_issues[0].get('details'):
            skip_reason += f" - {critical_issues[0]['details']}"
        cursor.execute("""
        INSERT INTO videos 
        (filepath, status, date_added, skip_reason) 
        VALUES (?, ?, ?, ?)
        """, (
            video_path, 
            'skipped', 
            datetime.now(),
            skip_reason
        ))
        conn.commit()
        log.warning(f"Skipping video due to critical issues: {video_path}. Reason: {skip_reason}")
        return None
    
    # Get optimal encoding settings based on video analysis
    optimal_settings = video_utils.get_optimal_encoding_settings(info)
    
    # Add to database with all metadata
    cursor.execute("""
    INSERT INTO videos 
    (filepath, original_size, status, date_added, original_duration, 
     original_width, original_height, original_bitrate, custom_settings) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        video_path, 
        info['size'], 
        'pending', 
        datetime.now(),
        info['duration'],
        info['video']['width'],
        info['video']['height'],
        info['bit_rate'],
        json.dumps(optimal_settings) if optimal_settings else None
    ))
    
    conn.commit()
    return cursor.lastrowid

def transcode_video(video_path, temp_path, video_id, conn, progress_callback=None):
    """Transcode the video to save space while maintaining quality."""
    global exit_requested
    
    try:
        # First, check if the source file exists
        if not os.path.exists(video_path):
            log.error(f"Source file does not exist: {video_path}")
            cursor = conn.cursor()
            cursor.execute("""
            UPDATE videos 
            SET status = 'skipped', skip_reason = ? 
            WHERE id = ?
            """, ('Source file does not exist', video_id))
            conn.commit()
            return False
            
        # Ensure temp directory exists
        temp_dir = os.path.dirname(temp_path)
        if not os.path.exists(temp_dir):
            try:
                os.makedirs(temp_dir)
            except OSError as e:
                log.error(f"Failed to create temp directory {temp_dir}: {e}")
                return False
                
        # Get video info for this specific file
        info = get_video_info(video_path)
        if not info:
            return False
            
        # Check for custom encoding settings in database
        cursor = conn.cursor()
        cursor.execute("SELECT custom_settings FROM videos WHERE id = ?", (video_id,))
        custom_settings_json = cursor.fetchone()[0]
        
        # Parse custom settings if available
        custom_settings = json.loads(custom_settings_json) if custom_settings_json else {}
        
        # Start with base parameters from config
        codec = custom_settings.get('codec', config.VIDEO_ENCODING['codec'])
        audio_codec = config.VIDEO_ENCODING['audio_codec']
        audio_bitrate = custom_settings.get('audio_bitrate', config.VIDEO_ENCODING['audio_bitrate'])
        
        # Prepare FFmpeg command using config settings and any custom overrides
        cmd = [
            'ffmpeg',
            '-i', video_path,
            '-c:v', codec,
        ]
        
        # Add audio settings
        cmd.extend([
            '-c:a', audio_codec,
            '-b:a', audio_bitrate,
        ])
        
        # Add any custom extra parameters if specified
        if 'extra_params' in custom_settings and custom_settings['extra_params']:
            cmd.extend(custom_settings['extra_params'])
        else:
            # Otherwise use the default extra params from config
            base_extra_params = config.VIDEO_ENCODING['extra_params']
            if isinstance(base_extra_params, list):
                cmd.extend(base_extra_params)
        
        # Add metadata about our processing (using more compatible tags)
        settings_str = json.dumps(custom_settings) if custom_settings else "default"
        cmd.extend([
            '-metadata', 'comment=Processed by video_reconvert v1.0',
            '-metadata', f'description=Original size: {format_size(info["size"])}. Encoded on {datetime.now().isoformat()} using {codec}. Settings: {settings_str}',
            '-metadata', 'copyright=video_reconvert',
            '-y'  # Overwrite output file
        ])
        
        # Add output path
        cmd.append(temp_path)
        
        # Log the detailed command info to debug log only
        debug_message = "\nExecuting ffmpeg command:\n"
        debug_message += "=" * 80 + "\n"
        debug_message += f"Input file: {video_path}\n"
        debug_message += f"Output file: {temp_path}\n"
        debug_message += f"Full command: {' '.join(cmd)}\n"
        if custom_settings:
            debug_message += f"Custom settings: {json.dumps(custom_settings, indent=2)}\n"
        debug_message += "=" * 80
        log.debug(debug_message)
        
        # Log a simpler message for info level
        log.info(f"Starting ffmpeg conversion of: {os.path.basename(video_path)}")
        
        # Start FFmpeg process
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Monitor progress
        duration = info['duration']
        ffmpeg_process_terminated = False
        
        # Setup non-blocking read
        import select
        
        # File descriptors to monitor
        fd_stderr = process.stderr.fileno()
        
        # Use select to wait for data
        while process.poll() is None:
            # Check if we've been asked to exit
            if exit_requested:
                log.info("Exit requested, terminating FFmpeg process")
                process.terminate()
                ffmpeg_process_terminated = True
                break
                
            # Wait for data (with timeout)
            readable, _, _ = select.select([fd_stderr], [], [], 0.5)
            
            if fd_stderr in readable:
                line = process.stderr.readline()
                if not line:
                    break
                    
                if "time=" in line:
                    time_match = line.split("time=")[1].split()[0]
                    try:
                        h, m, s = time_match.split(':')
                        current_time = float(h) * 3600 + float(m) * 60 + float(s)
                        progress = min(current_time / duration, 1.0)
                        if progress_callback:
                            progress_callback(progress)
                    except (ValueError, IndexError):
                        pass
                        
                # Check for common error messages and log them
                if "Error" in line or "Invalid" in line:
                    log.error(f"FFmpeg error: {line.strip()}")
        
        # Read any remaining output
        remaining_stderr = process.stderr.read()
        
        # Check for exit code only if we didn't terminate the process ourselves
        if not ffmpeg_process_terminated:
            process.wait()
            
            if process.returncode != 0:
                log.error(f"FFmpeg error for {video_path} (exit code {process.returncode})")
                return False
        else:
            return False
            
        # If exit was requested, don't proceed
        if exit_requested:
            return False
            
        # Get new file info
        new_info = get_video_info(temp_path)
        if not new_info:
            return False
            
        # Update database with results
        cursor = conn.cursor()
        cursor.execute("""
        UPDATE videos 
        SET new_size = ?, new_bitrate = ?, status = ?, date_completed = ? 
        WHERE id = ?
        """, (
            new_info['size'],
            new_info['bit_rate'],
            'completed',
            datetime.now(),
            video_id
        ))
        conn.commit()
        
        return True
    
    except Exception as e:
        log.exception(f"Transcoding error for {video_path}: {str(e)}")
        
        # Update database with error
        cursor = conn.cursor()
        cursor.execute("""
        UPDATE videos 
        SET status = ?, error_message = ? 
        WHERE id = ?
        """, ('error', str(e), video_id))
        conn.commit()
        
        return False

def get_status_summary(conn):
    """Get summary statistics for the conversion process."""
    cursor = conn.cursor()
    
    # Total counts
    cursor.execute("SELECT COUNT(*), SUM(original_size) FROM videos")
    total_count, total_size = cursor.fetchone()
    
    # Completed counts
    cursor.execute("SELECT COUNT(*), SUM(original_size), SUM(new_size) FROM videos WHERE status = 'completed'")
    completed_count, completed_original_size, completed_new_size = cursor.fetchone()
    
    # Error counts
    cursor.execute("SELECT COUNT(*) FROM videos WHERE status = 'error'")
    error_count = cursor.fetchone()[0]
    
    # Pending counts
    cursor.execute("SELECT COUNT(*) FROM videos WHERE status = 'pending'")
    pending_count = cursor.fetchone()[0]
    
    # Skipped counts
    cursor.execute("SELECT COUNT(*) FROM videos WHERE status = 'skipped'")
    skipped_count = cursor.fetchone()[0]
    
    # Handle None values
    total_count = total_count or 0
    total_size = total_size or 0
    completed_count = completed_count or 0
    completed_original_size = completed_original_size or 0
    completed_new_size = completed_new_size or 0
    error_count = error_count or 0
    pending_count = pending_count or 0
    skipped_count = skipped_count or 0
    
    # Calculate space savings
    space_saved = completed_original_size - completed_new_size
    percent_complete = (completed_count / (total_count - skipped_count) * 100) if (total_count - skipped_count) > 0 else 0
    percent_saved = (space_saved / completed_original_size * 100) if completed_original_size > 0 else 0
    
    return {
        'total_count': total_count,
        'total_size': total_size,
        'completed_count': completed_count,
        'completed_original_size': completed_original_size,
        'completed_new_size': completed_new_size,
        'space_saved': space_saved,
        'percent_complete': percent_complete,
        'percent_saved': percent_saved,
        'error_count': error_count,
        'pending_count': pending_count,
        'skipped_count': skipped_count
    }

def format_size(size_bytes):
    """Format file size in a human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024 or unit == 'TB':
            break
        size_bytes /= 1024
    return f"{size_bytes:.2f} {unit}"

def display_status(conn, console):
    """Display status information in the console."""
    summary = get_status_summary(conn)
    
    table = Table(show_header=False)
    table.add_column("Metric")
    table.add_column("Value")
    
    table.add_row("Total Videos Found", f"{summary['total_count']}")
    table.add_row("Total Original Size", format_size(summary['total_size']))
    
    if summary['skipped_count'] > 0:
        table.add_row("Videos Skipped", f"[{config.UI['error_style']}]{summary['skipped_count']}[/{config.UI['error_style']}]")
    
    table.add_row("Videos Processed", f"{summary['completed_count']} ({summary['percent_complete']:.1f}%)")
    
    if summary['error_count'] > 0:
        table.add_row("Videos with Errors", f"[{config.UI['error_style']}]{summary['error_count']}[/{config.UI['error_style']}]")
    
    if summary['pending_count'] > 0:
        table.add_row("Videos Pending", f"[{config.UI['info_style']}]{summary['pending_count']}[/{config.UI['info_style']}]")
    
    if summary['completed_count'] > 0:
        table.add_row("Space Saved", f"[{config.UI['header_style']}]{format_size(summary['space_saved'])} ({summary['percent_saved']:.1f}%)[/{config.UI['header_style']}]")
    
    console.print(Panel(table, title="Video Reconvert Status", border_style=config.UI['header_style']))

def analyze_video(video_path, console):
    """Display detailed analysis for a single video."""
    # Check if the file exists
    if not os.path.exists(video_path):
        console.print(f"[{config.UI['error_style']}]Cannot analyze: File does not exist: {video_path}[/{config.UI['error_style']}]")
        return
        
    video_info = get_video_info(video_path)
    if not video_info:
        console.print(f"[{config.UI['error_style']}]Failed to analyze video: {video_path}[/{config.UI['error_style']}]")
        return
    
    # Run sanity checks
    issues = video_utils.run_sanity_checks(video_info)
    
    # Get analysis and recommendations
    analysis = video_utils.analyze_conversion_potential(video_info)
    
    # Display basic info
    console.print(Panel(f"[bold]Video Analysis:[/bold] {os.path.basename(video_path)}", 
                        style=config.UI['header_style']))
    
    # Original video properties
    props_table = Table(show_header=False)
    props_table.add_column("Property")
    props_table.add_column("Value")
    
    props_table.add_row("Resolution", f"{video_info['video']['width']}x{video_info['video']['height']}")
    props_table.add_row("Duration", f"{video_info['duration']:.2f} seconds")
    props_table.add_row("Size", format_size(video_info['size']))
    props_table.add_row("Codec", video_info['video']['codec'])
    props_table.add_row("Bitrate", f"{video_info['bit_rate']/1000:.0f} kbps")
    if video_info.get('audio'):
        props_table.add_row("Audio", f"{video_info['audio']['codec']} ({video_info['audio']['channels']} ch, {video_info['audio']['sample_rate']} Hz)")
    
    console.print(props_table)
    
    # Display issues
    if issues:
        issues_table = Table(title="Potential Issues")
        issues_table.add_column("Level")
        issues_table.add_column("Issue")
        issues_table.add_column("Details")
        
        for issue in issues:
            style = {
                'error': config.UI['error_style'],
                'warning': config.UI['progress_style'],
                'info': config.UI['info_style']
            }.get(issue['level'], '')
            
            issues_table.add_row(
                f"[{style}]{issue['level'].upper()}[/{style}]",
                issue['message'],
                issue['details']
            )
        
        console.print(issues_table)
    
    # Display recommendations
    if 'recommendations' in analysis:
        rec_table = Table(title="Recommended Conversion Settings")
        rec_table.add_column("Setting")
        rec_table.add_column("Value")
        
        for key, value in analysis['recommendations'].items():
            rec_table.add_row(key, str(value))
        
        console.print(rec_table)
    
    # Display estimated savings
    if 'estimated_savings' in analysis:
        est = analysis['estimated_savings']
        savings_table = Table(title="Estimated Results")
        savings_table.add_column("Metric")
        savings_table.add_column("Value")
        
        savings_table.add_row("New Size", format_size(est['estimated_size_mb'] * 1024 * 1024))
        savings_table.add_row("Space Saved", format_size(est['estimated_saving_mb'] * 1024 * 1024))
        savings_table.add_row("Saving Percentage", f"{est['estimated_saving_pct']:.1f}%")
        
        console.print(savings_table)

def process_videos(source_path, analyze_only=False, target_video=None):
    """Main function to process all videos."""
    global current_temp_file
    
    # Check if ffmpeg is available
    if not video_utils.check_ffmpeg_availability():
        console.print(f"[{config.UI['error_style']}]Error: ffmpeg and ffprobe are required but not found.[/{config.UI['error_style']}]")
        console.print("Please install ffmpeg and make sure it's in your PATH.")
        return
    
    # Check for temp files from previous interrupted runs
    if source_path:
        temp_files = find_temp_files(source_path)
        if temp_files:
            console.print(f"[{config.UI['warning_style']}]Found {len(temp_files)} temporary files from previous interrupted runs:[/{config.UI['warning_style']}]")
            for tf in temp_files:
                console.print(f"  - {tf}")
            
            response = console.input(f"[{config.UI['info_style']}]Would you like to delete these temporary files? (y/n): [/{config.UI['info_style']}]")
            if response.lower() in ('y', 'yes'):
                clean_temp_files(temp_files)
                console.print(f"[{config.UI['header_style']}]Temporary files cleaned up.[/{config.UI['header_style']}]")
            else:
                console.print(f"[{config.UI['info_style']}]Temporary files will be ignored during processing.[/{config.UI['info_style']}]")
        
    conn = setup_database()
    
    # If target video is specified, only analyze that one
    if target_video:
        if os.path.exists(target_video):
            analyze_video(target_video, console)
        else:
            console.print(f"[{config.UI['error_style']}]Error: Video file not found: {target_video}[/{config.UI['error_style']}]")
        return
    
    # Find all videos in source path
    log.info(f"Scanning for videos in {source_path}")
    videos = find_videos(source_path)
    log.info(f"Found {len(videos)} video files")
    
    # Clean up database - mark files that no longer exist as 'missing'
    if source_path:
        cursor = conn.cursor()
        # Get all files in the database that aren't already marked as completed or skipped
        cursor.execute("SELECT id, filepath FROM videos WHERE status NOT IN ('completed', 'skipped')")
        db_files = cursor.fetchall()
        
        missing_count = 0
        for file_id, filepath in db_files:
            if not os.path.exists(filepath):
                # File no longer exists, mark as missing
                cursor.execute("""
                UPDATE videos 
                SET status = 'skipped', skip_reason = ? 
                WHERE id = ?
                """, ('File no longer exists on disk', file_id))
                missing_count += 1
                log.warning(f"File in database no longer exists: {filepath}")
        
        if missing_count > 0:
            conn.commit()
            console.print(f"[{config.UI['warning_style']}]Marked {missing_count} files as missing because they no longer exist on disk.[/{config.UI['warning_style']}]")
    
    # Register all videos in database
    for video_path in videos:
        register_video(conn, video_path)
    
    # Display initial status
    display_status(conn, console)
    
    # If analyze_only mode, don't proceed with transcoding
    if analyze_only:
        console.print(f"[{config.UI['info_style']}]Analysis complete. Not performing transcoding (--analyze-only specified).[/{config.UI['info_style']}]")
        return
    
    # Process videos that haven't been completed
    cursor = conn.cursor()
    where_clause = "status = 'pending'"
    if config.DATABASE['retry_errors']:
        where_clause += " OR status = 'error'"
    
    cursor.execute(f"SELECT id, filepath FROM videos WHERE {where_clause}")
    pending_videos = cursor.fetchall()
    
    # Filter out files that no longer exist
    valid_pending_videos = []
    for video_id, video_path in pending_videos:
        if os.path.exists(video_path):
            valid_pending_videos.append((video_id, video_path))
        else:
            # Update status in database
            cursor.execute("""
            UPDATE videos 
            SET status = 'skipped', skip_reason = ? 
            WHERE id = ?
            """, ('File no longer exists on disk', video_id))
            log.warning(f"Skipping missing file: {video_path}")
    
    # Commit any changes to database
    conn.commit()
    
    # Use the filtered list
    pending_videos = valid_pending_videos
    
    if not pending_videos:
        console.print(f"[{config.UI['header_style']}]All videos have been processed![/{config.UI['header_style']}]")
        return
    
    console.print(f"[{config.UI['info_style']}]Processing {len(pending_videos)} videos...[/{config.UI['info_style']}]")
    
    for video_id, video_path in pending_videos:
        # Check if exit has been requested
        if exit_requested:
            break
        
        # Double-check file still exists (it might have been deleted during processing)
        if not os.path.exists(video_path):
            log.warning(f"File disappeared during processing: {video_path}")
            # Update status in database
            cursor = conn.cursor()
            cursor.execute("""
            UPDATE videos 
            SET status = 'skipped', skip_reason = ? 
            WHERE id = ?
            """, ('File disappeared during processing', video_id))
            conn.commit()
            continue
            
        base_name = os.path.basename(video_path)
        directory = os.path.dirname(video_path)
        temp_path = os.path.join(directory, f"temp_{base_name}")
        
        # Update global current_temp_file for signal handler
        current_temp_file = temp_path
        
        console.print(f"[{config.UI['info_style']}]Processing: {base_name}[/{config.UI['info_style']}]")
        
        # Display detailed analysis before processing
        analyze_video(video_path, console)
        
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            task = progress.add_task(f"Transcoding {base_name}", total=1.0)
            
            def update_progress(value):
                progress.update(task, completed=value)
            
            success = transcode_video(video_path, temp_path, video_id, conn, update_progress)
            
            if success and not exit_requested:
                # Verify the temp file exists
                if os.path.exists(temp_path):
                    # Verify the source file still exists
                    if os.path.exists(video_path):
                        # Replace original with transcoded version
                        os.remove(video_path)
                        os.rename(temp_path, video_path)
                        log.info(f"Successfully transcoded: {video_path}")
                    else:
                        log.error(f"Source file disappeared during transcoding: {video_path}")
                        # Don't delete the temp file in this case, it contains the output
                        cursor = conn.cursor()
                        cursor.execute("""
                        UPDATE videos 
                        SET status = 'error', error_message = ? 
                        WHERE id = ?
                        """, ('Source file disappeared during transcoding', video_id))
                        conn.commit()
                else:
                    log.error(f"Temp file {temp_path} not found after transcoding")
            else:
                log.error(f"Failed to transcode: {video_path}")
                if os.path.exists(temp_path):
                    os.remove(temp_path)
        
        # Reset current_temp_file
        current_temp_file = None
        
        # Update status after each video
        display_status(conn, console)
        
        # Check again if exit has been requested
        if exit_requested:
            break
    
    if not exit_requested:
        console.print(f"[{config.UI['header_style']}]Video transcoding process completed![/{config.UI['header_style']}]")
        
        # Final status display
        display_status(conn, console)
    
    # Close database connection
    conn.close()

def main():
    """Parse arguments and start the video processing."""
    parser = argparse.ArgumentParser(description='Transcode video files to save space.')
    parser.add_argument('source_path', help='Path to search for video files')
    parser.add_argument('--extensions', help='Comma-separated list of file extensions to process (default: .mp4)')
    parser.add_argument('--crf', type=int, help='Constant Rate Factor (0-51, lower is better quality)')
    parser.add_argument('--preset', help='Encoding preset (e.g., medium, slow, fast)')
    parser.add_argument('--retry-errors', action='store_true', help='Retry files that previously failed')
    parser.add_argument('--no-retry-errors', action='store_false', dest='retry_errors', help='Skip files that previously failed')
    parser.add_argument('--analyze-only', action='store_true', help='Only analyze videos without transcoding')
    parser.add_argument('--analyze-video', help='Analyze a single video file without transcoding')
    parser.add_argument('--force-reconvert', action='store_true', help='Allow reconverting files that were already processed')
    
    args = parser.parse_args()
    
    # Apply command-line overrides to config
    if args.extensions:
        config.FILE_SEARCH['extensions'] = args.extensions.split(',')
    
    if args.crf is not None:
        if 0 <= args.crf <= 51:
            config.VIDEO_ENCODING['crf'] = args.crf
        else:
            print("CRF must be between 0 and 51. Using default value.")
    
    if args.preset:
        valid_presets = ['ultrafast', 'superfast', 'veryfast', 'faster', 'fast', 
                         'medium', 'slow', 'slower', 'veryslow']
        if args.preset in valid_presets:
            config.VIDEO_ENCODING['preset'] = args.preset
        else:
            print(f"Invalid preset. Must be one of: {', '.join(valid_presets)}. Using default.")
    
    if args.retry_errors is not None:
        config.DATABASE['retry_errors'] = args.retry_errors
        
    if args.force_reconvert:
        config.VIDEO_ENCODING['force_reconvert'] = True
    
    # If analyzing single video, use that mode
    if args.analyze_video:
        process_videos(None, analyze_only=True, target_video=args.analyze_video)
        return
        
    # Otherwise, process normal source path mode
    source_path = os.path.abspath(args.source_path)
    if not os.path.exists(source_path):
        print(f"Error: Source path '{source_path}' does not exist")
        sys.exit(1)
    
    process_videos(source_path, analyze_only=args.analyze_only)

if __name__ == "__main__":
    main() 