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
from rich.console import Console, Group
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table
from rich.layout import Layout
from rich.live import Live
from rich import print as rprint
from rich.prompt import Prompt
from logging.handlers import RotatingFileHandler
import config
import video_utils

# Global variables for cleanup
current_temp_file = None
exit_requested = False

class VideoReconvertUI:
    def __init__(self):
        self.layout = Layout()
        
        # Split into header and body
        self.layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body")
        )
        
        # Split body into left and right columns
        self.layout["body"].split_row(
            Layout(name="left_panel", ratio=1),
            Layout(name="logs", ratio=1)
        )
        
        # Split left panel into global stats and details
        self.layout["left_panel"].split_column(
            Layout(name="global_stats", size=10),  # Increased size to accommodate all rows including Space Saved
            Layout(name="details")  # Takes remaining space
        )
        
        # Create separate consoles
        self.dashboard_console = Console(force_terminal=True)
        self.log_console = Console(force_terminal=True)
        
        # Initialize the live display
        self.live = Live(self.layout, refresh_per_second=4, console=Console())
        
        # Keep track of log messages
        self.log_messages = []
        
        # Progress bar for individual file progress
        self.progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
        )
        self.current_task = None
        
        # Keep track of current content
        self.summary_table = None
        self.detail_content = None
        
    def start(self):
        """Start the live display"""
        self.live.start()
        self.update_header("Video Reconvert")
        self.progress.start()
        
    def stop(self):
        """Stop the live display"""
        if self.current_task is not None:
            self.stop_progress()
        self.progress.stop()
        self.live.stop()
        
    def update_header(self, text):
        """Update the header section"""
        self.layout["header"].update(
            Panel(text, style=config.UI['header_style'])
        )
        
    def update_dashboard(self, content):
        """Update the dashboard section with both summary and detail content"""
        if isinstance(content, Table):
            # Check if this is a global stats table
            if hasattr(content, 'is_global_stats'):
                self.summary_table = content
                # Update global stats panel
                self.layout["global_stats"].update(
                    Panel(
                        self.summary_table,
                        title="Global Progress",
                        border_style=config.UI['header_style']
                    )
                )
            else:
                # This is a detail table
                self.detail_content = content
                self._update_details_panel()
        elif content is not None:
            # This is other detail content
            self.detail_content = content
            self._update_details_panel()
        
    def _update_details_panel(self):
        """Update the details panel with current content and progress"""
        detail_content = []
        
        # Add detail content
        if self.detail_content:
            detail_content.append(self.detail_content)
        
        # Add progress bar at the bottom if active
        if self.current_task is not None:
            if detail_content:
                detail_content.append("")  # Add spacing
            detail_content.append(self.progress)
        
        # Update details panel
        self.layout["details"].update(
            Panel(
                Group(*detail_content) if detail_content else "",
                title="Video Details",
                border_style=config.UI['header_style']
            )
        )
        
    def start_progress(self, description):
        """Start a new progress bar"""
        self.current_task = self.progress.add_task(description, total=1.0)
        # Don't clear content when starting progress
        self.update_dashboard(self.detail_content)
        
    def update_progress(self, value):
        """Update the progress bar"""
        if self.current_task is not None:
            self.progress.update(self.current_task, completed=value)
            
    def stop_progress(self):
        """Stop the current progress bar"""
        if self.current_task is not None:
            self.progress.remove_task(self.current_task)
            self.current_task = None
            # Preserve current content when stopping progress
            self.update_dashboard(self.detail_content)
        
    def add_log(self, message):
        """Add a log message to the log section"""
        self.log_messages.append(message)
        # Keep only the last 100 messages to prevent memory issues
        if len(self.log_messages) > 100:
            self.log_messages.pop(0)
        self.update_logs()
        
    def update_logs(self):
        """Update the log section with current messages"""
        # Format log messages with timestamp and level
        formatted_messages = []
        last_timestamp = None
        
        # Calculate padding for timestamp (HH:MM:SS = 8 chars + 1 space)
        timestamp_width = 8  # HH:MM:SS
        level_width = 8      # For log level text
        timestamp_padding = " " * (timestamp_width + 1)  # Space for "HH:MM:SS "
        
        for msg in self.log_messages[-30:]:  # Show last 30 messages
            try:
                # Extract timestamp and level if present
                parts = msg.split(' - ')
                if len(parts) >= 3:
                    timestamp = parts[0]
                    level = parts[2]
                    # Join the rest of the parts back together as the message
                    message = ' - '.join(parts[3:]) if len(parts) > 3 else ''
                    
                    # Convert timestamp to simpler format (HH:MM:SS)
                    try:
                        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S,%f')
                        simple_timestamp = dt.strftime('%H:%M:%S')
                    except ValueError:
                        simple_timestamp = timestamp
                        
                    level_style = {
                        'ERROR': config.UI['error_style'],
                        'WARNING': config.UI['warning_style'],
                        'INFO': config.UI['info_style'],
                        'DEBUG': 'dim white'
                    }.get(level, '')
                    
                    # Only show timestamp if it's different from the last one
                    if simple_timestamp != last_timestamp:
                        formatted_msg = f"[dim]{simple_timestamp:>{timestamp_width}}[/dim] [{level_style}]{level:<{level_width}}[/{level_style}] {message}"
                        last_timestamp = simple_timestamp
                    else:
                        formatted_msg = f"{timestamp_padding}[{level_style}]{level:<{level_width}}[/{level_style}] {message}"
                    
                    formatted_messages.append(formatted_msg)
                else:
                    formatted_messages.append(msg)
            except Exception:
                # If parsing fails, just show the original message
                formatted_messages.append(msg)
                
        log_content = "\n".join(formatted_messages)
        self.layout["logs"].update(
            Panel(log_content, title="Log Output", border_style=config.UI['info_style'])
        )
        
    def prompt(self, message):
        """Show a prompt to the user"""
        self.live.stop()
        result = Prompt.ask(message)
        self.live.start()
        return result

# Create global UI instance
ui = VideoReconvertUI()

# Custom Rich handler that updates our UI
class UIRichHandler(RichHandler):
    def emit(self, record):
        try:
            # Format the message with timestamp and level
            msg = self.format(record)
            # Add to our UI's log section
            ui.add_log(msg)
        except Exception as e:
            # Fallback to standard output if UI fails
            print(f"Log handler error: {e}")
            print(f"Original message: {record.getMessage()}")

# Configure logging
log_level = getattr(logging, config.LOGGING['level'])
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
                                datefmt='%Y-%m-%d %H:%M:%S')

# File handler with rotation - set to DEBUG to capture all details
file_handler = RotatingFileHandler(
    config.LOGGING['filename'],
    maxBytes=config.LOGGING['max_size'],
    backupCount=config.LOGGING['backup_count']
)
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.DEBUG)  # Always capture debug in file

# Console handler with rich formatting - use configured level
console_handler = UIRichHandler(
    rich_tracebacks=True,
    console=ui.log_console,
    show_time=True,
    show_level=True,
    show_path=False,
    enable_link_path=False
)
console_handler.setFormatter(log_formatter)
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

def display_status(conn):
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
    
    # Mark this table as a global stats table
    setattr(table, 'is_global_stats', True)
    ui.update_dashboard(table)

def analyze_video(video_path):
    """Display detailed analysis for a single video."""
    # Check if the file exists
    if not os.path.exists(video_path):
        log.error(f"Cannot analyze: File does not exist: {video_path}")
        return
        
    video_info = get_video_info(video_path)
    if not video_info:
        log.error(f"Failed to analyze video: {video_path}")
        return
    
    # Run sanity checks
    issues = video_utils.run_sanity_checks(video_info)
    
    # Get analysis and recommendations
    analysis = video_utils.analyze_conversion_potential(video_info)
    
    # Create tables for display
    tables = []
    
    # Basic info header
    tables.append(f"[bold]Video Analysis:[/bold] {os.path.basename(video_path)}")
    
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
    
    tables.append(props_table)
    
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
        
        tables.append(issues_table)
    
    # Display recommendations
    if 'recommendations' in analysis:
        rec_table = Table(title="Recommended Conversion Settings")
        rec_table.add_column("Setting")
        rec_table.add_column("Value")
        
        for key, value in analysis['recommendations'].items():
            rec_table.add_row(key, str(value))
        
        tables.append(rec_table)
    
    # Display estimated savings
    if 'estimated_savings' in analysis:
        est = analysis['estimated_savings']
        savings_table = Table(title="Estimated Results")
        savings_table.add_column("Metric")
        savings_table.add_column("Value")
        
        savings_table.add_row("New Size", format_size(est['estimated_size_mb'] * 1024 * 1024))
        savings_table.add_row("Space Saved", format_size(est['estimated_saving_mb'] * 1024 * 1024))
        savings_table.add_row("Saving Percentage", f"{est['estimated_saving_pct']:.1f}%")
        
        tables.append(savings_table)
    
    # Update dashboard with all tables
    ui.update_dashboard(Group(*tables))

def process_videos(source_path, analyze_only=False, target_video=None):
    """Main function to process all videos."""
    global current_temp_file
    
    # Start the UI
    ui.start()
    
    try:
        # Check if ffmpeg is available
        if not video_utils.check_ffmpeg_availability():
            log.error("Error: ffmpeg and ffprobe are required but not found.")
            log.error("Please install ffmpeg and make sure it's in your PATH.")
            return
        
        # Check for temp files from previous interrupted runs
        if source_path:
            temp_files = find_temp_files(source_path)
            if temp_files:
                log.warning(f"Found {len(temp_files)} temporary files from previous interrupted runs:")
                for tf in temp_files:
                    log.info(f"  - {tf}")
                
                response = ui.prompt("Would you like to delete these temporary files? (y/n): ")
                if response.lower() in ('y', 'yes'):
                    clean_temp_files(temp_files)
                    log.info("Temporary files cleaned up.")
                else:
                    log.info("Temporary files will be ignored during processing.")
        
        conn = setup_database()
        
        # If target video is specified, only analyze that one
        if target_video:
            if os.path.exists(target_video):
                analyze_video(target_video)
            else:
                log.error(f"Error: Video file not found: {target_video}")
            return
        
        # Find all videos in source path
        log.info(f"Scanning for videos in {source_path}")
        videos = find_videos(source_path)
        log.info(f"Found {len(videos)} video files")
        
        # Clean up database entries for missing files
        if source_path:
            cursor = conn.cursor()
            cursor.execute("SELECT id, filepath FROM videos WHERE status NOT IN ('completed', 'skipped')")
            db_files = cursor.fetchall()
            
            missing_count = 0
            for file_id, filepath in db_files:
                if not os.path.exists(filepath):
                    cursor.execute("""
                    UPDATE videos 
                    SET status = 'skipped', skip_reason = ? 
                    WHERE id = ?
                    """, ('File no longer exists on disk', file_id))
                    missing_count += 1
                    log.warning(f"File in database no longer exists: {filepath}")
            
            if missing_count > 0:
                conn.commit()
                log.warning(f"Marked {missing_count} files as missing because they no longer exist on disk.")
        
        # Register all videos in database
        for video_path in videos:
            register_video(conn, video_path)
        
        # Display initial status
        display_status(conn)
        
        # If analyze_only mode, don't proceed with transcoding
        if analyze_only:
            log.info("Analysis complete. Not performing transcoding (--analyze-only specified).")
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
                cursor.execute("""
                UPDATE videos 
                SET status = 'skipped', skip_reason = ? 
                WHERE id = ?
                """, ('File no longer exists on disk', video_id))
                log.warning(f"Skipping missing file: {video_path}")
        
        conn.commit()
        pending_videos = valid_pending_videos
        
        if not pending_videos:
            log.info("All videos have been processed!")
            return
        
        log.info(f"Processing {len(pending_videos)} videos...")
        
        for video_id, video_path in pending_videos:
            if exit_requested:
                break
            
            # Double-check file still exists
            if not os.path.exists(video_path):
                log.warning(f"File disappeared during processing: {video_path}")
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
            
            current_temp_file = temp_path
            log.info(f"Processing: {base_name}")
            
            # Display detailed analysis
            analyze_video(video_path)
            
            # Start progress tracking
            ui.start_progress(f"Transcoding {base_name}")
            
            success = transcode_video(video_path, temp_path, video_id, conn, ui.update_progress)
            
            # Stop progress tracking
            ui.stop_progress()
            
            if success and not exit_requested:
                if os.path.exists(temp_path):
                    if os.path.exists(video_path):
                        os.remove(video_path)
                        os.rename(temp_path, video_path)
                        log.info(f"Successfully transcoded: {video_path}")
                    else:
                        log.error(f"Source file disappeared during transcoding: {video_path}")
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
            
            current_temp_file = None
            display_status(conn)
            
            if exit_requested:
                break
        
        if not exit_requested:
            log.info("Video transcoding process completed!")
            display_status(conn)
    
    finally:
        ui.stop()
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