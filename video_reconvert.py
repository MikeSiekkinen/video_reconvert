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
from typing import Dict, Any
import re
import traceback

# Global variables for cleanup
current_temp_file = None
exit_requested = False

class FileRolodex:
    def __init__(self, files, max_display=7):
        self.files = files
        self.max_display = max_display
        self.current_index = 0
        self.center_position = max_display // 2
        self.file_status = {}  # Track status of each file: None=pending, True=success, False=failure, 'warning'=bloated
        self.file_sizes = {}   # Track original and new sizes: {filename: (original_size, new_size)}
        for file in files:
            self.file_status[file] = None
            self.file_sizes[file] = (None, None)
        
    def get_display_window(self):
        """Calculate which files should be visible in the rolodex window"""
        total_files = len(self.files)
        
        # If we have fewer files than max display, show them all
        if total_files <= self.max_display:
            start_idx = 0
            end_idx = total_files
        else:
            # For first few files, keep display anchored at top
            if self.current_index <= self.center_position:
                start_idx = 0
                end_idx = self.max_display
            # For last few files, keep display anchored at bottom
            elif self.current_index >= total_files - (self.max_display - self.center_position):
                start_idx = total_files - self.max_display
                end_idx = total_files
            # For middle files, keep current file centered
            else:
                start_idx = self.current_index - self.center_position
                end_idx = start_idx + self.max_display
        
        return self.files[start_idx:end_idx]
    
    def set_file_size(self, filename, original_size, new_size=None):
        """Set the size information for a file"""
        if filename in self.file_sizes:
            self.file_sizes[filename] = (original_size, new_size)
    
    def create_table(self):
        """Create a rich Table showing the current window of files"""
        table = Table(show_header=True, box=None, padding=(0, 1))
        table.add_column("Files to Process", style="bold")
        
        display_files = self.get_display_window()
        start_idx = self.files.index(display_files[0])
        
        for i, file in enumerate(display_files, start=start_idx):
            # Determine if this is the active file
            is_active = (i == self.current_index)
            # Use normal style for active file, dim for others
            style = "" if is_active else "dim"
            
            # Add status indicator if file has been processed
            status = self.file_status.get(file)
            if status is True:
                status_indicator = "[green]✓[/green] "
            elif status is False:
                status_indicator = "[red]✗[/red] "
            elif status == 'warning':
                status_indicator = "[yellow]⚠[/yellow] "
            else:
                status_indicator = "  "  # Two spaces for alignment
            
            # Format size information
            orig_size, new_size = self.file_sizes.get(file, (None, None))
            if orig_size is not None:
                orig_size_str = format_size(orig_size)
                if new_size is not None:
                    # Show both sizes and percentage
                    new_size_str = format_size(new_size)
                    percentage = (new_size / orig_size * 100) if orig_size > 0 else 0
                    size_info = f"({orig_size_str} → {new_size_str}, {percentage:.1f}%)"
                else:
                    # Show only original size
                    size_info = f"({orig_size_str})"
            else:
                size_info = ""
                
            table.add_row(f"{status_indicator}{file} {size_info}", style=style)
            
        return table
    
    def set_file_status(self, filename, success):
        """Set the status of a file after processing"""
        if filename in self.file_status:
            self.file_status[filename] = success

    def advance_rolodex(self):
        """Move to the next file"""
        if self.current_index < len(self.files) - 1:
            self.current_index += 1
            
    def set_current_file(self, filename):
        """Set the current file by name"""
        try:
            self.current_index = self.files.index(filename)
        except ValueError:
            pass  # File not found in list, keep current index

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
            Layout(name="global_stats", size=12),  # Increased size to accommodate horizontal layout
            Layout(name="details")  # Takes remaining space
        )
        
        # Create separate consoles
        self.dashboard_console = Console(force_terminal=True)
        self.log_console = Console(force_terminal=True)
        
        # Initialize the live display
        self.live = Live(self.layout, refresh_per_second=4, console=Console())
        
        # Keep track of log messages
        self.log_messages = []
        
        # Progress bar for individual file progress - don't start it yet
        self.progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            auto_refresh=False,  # Disable auto refresh since we're managing it
            get_time=None,  # Use None to prevent progress creating its own timer
        )
        self.current_task = None
        
        # Keep track of current content
        self.summary_table = None
        self.detail_content = None
        self.rolodex = None
        
    def update_dashboard(self, content):
        """Update the dashboard section with both summary and detail content"""
        if isinstance(content, Table):
            # Check if this is a global stats table
            if hasattr(content, 'is_global_stats'):
                self.summary_table = content
                
                # Create a horizontal layout for the global stats panel
                global_layout = Layout()
                global_layout.split_row(
                    Layout(name="stats", ratio=1),
                    Layout(name="files", ratio=1)
                )
                
                # Add the summary table to the left side
                global_layout["stats"].update(self.summary_table)
                
                # Add the rolodex to the right side if we have one
                if self.rolodex:
                    rolodex_table = self.rolodex.create_table()
                    global_layout["files"].update(rolodex_table)
                
                # Update the global stats panel with the combined layout
                self.layout["global_stats"].update(
                    Panel(
                        global_layout,
                        title="Global Progress",
                        border_style=config.UI['header_style'],
                        height=12  # Fixed height to accommodate both tables
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
            
    def initialize_rolodex(self, files, source_files=None):
        """Initialize the file rolodex with list of files"""
        self.rolodex = FileRolodex(files)
        
        # Map basenames to full paths if source_files is provided
        file_map = {}
        if source_files:
            for full_path in source_files:
                file_map[os.path.basename(full_path)] = full_path
        
        # Initialize file sizes using just os.path.getsize() for speed
        total_measured = 0
        total_size = 0
        for file in files:
            try:
                # Get the full path if available
                full_path = file_map.get(file) if file_map else None
                if full_path and os.path.exists(full_path):
                    size = os.path.getsize(full_path)
                    total_size += size
                    self.rolodex.set_file_size(file, size)
                    total_measured += 1
                    
                    # Log progress every 100 files
                    if total_measured % 100 == 0:
                        log.info(f"Measured {total_measured}/{len(files)} files ({format_size(total_size)} total)")
            except Exception as e:
                log.debug(f"Failed to get size for {file}: {str(e)}")
                pass  # Skip if we can't get size info
        
        # Log final count
        if total_measured > 0:
            log.info(f"Completed measuring {total_measured}/{len(files)} files ({format_size(total_size)} total)")
        
        # Force a refresh of the dashboard to show the rolodex
        if self.summary_table:
            self.update_dashboard(self.summary_table)
        
    def set_current_file(self, filename):
        """Update the current file in the rolodex"""
        if self.rolodex:
            self.rolodex.set_current_file(filename)
            if self.summary_table:
                self.update_dashboard(self.summary_table)
            
    def advance_rolodex(self):
        """Move to the next file in the rolodex"""
        if self.rolodex:
            self.rolodex.advance_rolodex()
            if self.summary_table:
                self.update_dashboard(self.summary_table)
            
    def start(self):
        """Start the live display"""
        self.live.start()
        self.update_header("Video Reconvert")
        # Remove self.progress.start() - we don't want it to start independently
        
    def stop(self):
        """Stop the live display"""
        if self.current_task is not None:
            self.stop_progress()
        # Remove self.progress.stop() - we don't want to stop it independently
        self.live.stop()
        
    def update_header(self, text):
        """Update the header section"""
        self.layout["header"].update(
            Panel(text, style=config.UI['header_style'])
        )
        
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
        
    def update_progress(self, video_id, value):
        """Update the progress bar"""
        if self.current_task is not None:
            self.progress.update(self.current_task, completed=value)
            # Force a refresh of the display after updating progress
            self.update_dashboard(self.detail_content)
            
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
        
        # Calculate padding for timestamp (YYYY-MM-DD HH:MM:SS = 19 chars + 1 space)
        timestamp_width = 19  # YYYY-MM-DD HH:MM:SS
        level_width = 8      # For log level text
        
        for msg in self.log_messages[-30:]:  # Show last 30 messages
            try:
                # Extract timestamp and level if present
                parts = msg.split(' - ')
                if len(parts) >= 3:
                    timestamp = parts[0]
                    level = parts[2]
                    # Join the rest of the parts back together as the message
                    message = ' - '.join(parts[3:]) if len(parts) > 3 else ''
                    
                    level_style = {
                        'ERROR': config.UI['error_style'],
                        'WARNING': config.UI['warning_style'],
                        'INFO': config.UI['info_style'],
                        'DEBUG': 'dim white'
                    }.get(level, '')
                    
                    # Always show timestamp with consistent width
                    formatted_msg = f"[dim]{timestamp:<{timestamp_width}}[/dim] [{level_style}]{level:<{level_width}}[/{level_style}] {message}"
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

    def set_file_status(self, filename, success):
        """Set the status of a file in the rolodex"""
        if self.rolodex:
            self.rolodex.set_file_status(filename, success)

    def update_file_size(self, filename, new_size):
        """Update the new size of a file in the rolodex"""
        if self.rolodex:
            orig_size, _ = self.rolodex.file_sizes.get(filename, (None, None))
            if orig_size is not None:
                self.rolodex.set_file_size(filename, orig_size, new_size)

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
    enable_link_path=False,
    omit_repeated_times=False  # Ensure timestamps are always shown
)
console_handler.setFormatter(log_formatter)
console_handler.setLevel(log_level)

# Set root logger to DEBUG to allow everything through
logging.basicConfig(
    level=logging.DEBUG,  # Capture everything, handlers will filter
    format="%(message)s",
    datefmt="[%X]",
    handlers=[console_handler, file_handler]
)
log = logging.getLogger("video_reconvert")

# Create console instance at module level
console = Console()

def handle_exit_signal(sig, frame):
    global exit_requested, current_temp_file
    exit_requested = True
    
    log.info("Interrupt received, cleaning up and exiting gracefully...")
    log.info("Interrupt received, cleaning up and exiting gracefully...")
    
    # Clean up current temp file if it exists
    if current_temp_file and os.path.exists(current_temp_file):
        try:
            log.info(f"Removing temp file: {current_temp_file}")
            os.remove(current_temp_file)
            log.info(f"Removed temp file: {current_temp_file}")
        except Exception as e:
            log.error(f"Failed to remove temp file {current_temp_file}: {str(e)}")
    
    log.info("Exiting video_reconvert...")
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
        custom_settings TEXT,
        is_bloated BOOLEAN DEFAULT 0
    )
    ''')
    
    # Create indexes for commonly queried fields
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_videos_filepath ON videos(filepath)')
    
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
    SCHEMA_VERSION = 2
    
    if current_version < SCHEMA_VERSION:
        log.info(f"Updating database schema from version {current_version} to {SCHEMA_VERSION}")
        
        # Add new columns if they don't exist
        try:
            cursor.execute("SELECT is_bloated FROM videos LIMIT 1")
        except sqlite3.OperationalError:
            log.info("Adding is_bloated column to videos table")
            cursor.execute("ALTER TABLE videos ADD COLUMN is_bloated BOOLEAN DEFAULT 0")
        
        try:
            cursor.execute("SELECT skip_reason FROM videos LIMIT 1")
        except sqlite3.OperationalError:
            log.info("Adding skip_reason column to videos table")
            cursor.execute("ALTER TABLE videos ADD COLUMN skip_reason TEXT")
        
        try:
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
        return filename.startswith('temp_') or filename.startswith('.temp_') or filename.startswith('.progress_')
    
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
    log.debug(f"Checking if video exists in database: {video_path}")
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
            log.debug(f"Video already processed: {video_path}")
            return None
        elif status == 'skipped':
            log.debug(f"Video previously skipped: {video_path}. Reason: {skip_reason}")
            return None
        elif status == 'error' and not config.DATABASE['retry_errors']:
            log.debug(f"Skipping previously failed video: {video_path}")
            return None
        return video_id
    
    # Add to database as pending first
    log.debug(f"Registering new video in database: {video_path}")
    cursor.execute("""
    INSERT INTO videos 
    (filepath, status, date_added) 
    VALUES (?, ?, ?)
    """, (
        video_path, 
        'pending', 
        datetime.now()
    ))
    video_id = cursor.lastrowid
    conn.commit()
    
    return video_id

def calculate_target_resolution(width, height):
    """Calculate target resolution maintaining aspect ratio."""
    # Detect orientation
    is_vertical = height > width
    aspect_ratio = height / width if is_vertical else width / height
    
    if is_vertical:
        # Vertical video (shorts/reels format)
        target_width = 606  # Base width for vertical videos
        target_height = int(target_width * aspect_ratio)
    else:
        # Landscape video
        target_height = 720  # Standard height for landscape
        target_width = int(target_height * aspect_ratio)
    
    # Ensure dimensions are even (required for some codecs)
    target_width = (target_width // 2) * 2
    target_height = (target_height // 2) * 2
    
    return target_width, target_height

def calculate_target_bitrate(info):
    """Calculate target bitrate based on video properties."""
    input_bitrate = info.get('bit_rate', 0)
    width = info['video'].get('width', 0)
    height = info['video'].get('height', 0)
    fps = eval(info['video'].get('r_frame_rate', '30/1'))
    
    # Calculate default target based on resolution and fps
    pixels = width * height
    if pixels > 2073600:  # > 1080p
        default_target = 6000000  # 6Mbps
    elif pixels > 921600:  # > 720p
        default_target = 4000000  # 4Mbps
    else:
        default_target = 2500000  # 2.5Mbps
    
    # Adjust for frame rate
    if fps > 30:
        default_target *= 1.5
    
    # Never exceed input bitrate
    return min(input_bitrate, default_target) if input_bitrate > 0 else default_target

def get_optimal_encoding_settings(video_info: Dict[str, Any]) -> Dict[str, Any]:
    """Determine optimal encoding settings for a specific video."""
    if not video_info or 'video' not in video_info:
        return {}
    
    # Calculate target resolution
    target_width, target_height = calculate_target_resolution(
        video_info['video']['width'],
        video_info['video']['height']
    )
    
    # Calculate target bitrate
    target_bitrate = calculate_target_bitrate(video_info)
    
    # Start with base settings
    settings = {
        'codec': 'hevc_videotoolbox',  # Using hardware encoding for speed
        'extra_params': []
    }
    
    # Add scaling parameters
    if (target_width != video_info['video']['width'] or 
        target_height != video_info['video']['height']):
        settings['extra_params'].extend([
            '-vf', f'scale={target_width}:{target_height}'
        ])
    
    # Set audio bitrate (scale with video bitrate but cap at 128k)
    audio_bitrate = min(int(target_bitrate * 0.05 / 1000), 128)
    settings['audio_bitrate'] = f"{audio_bitrate}k"
    
    # Add quality settings for hardware encoder
    settings['extra_params'].extend([
        '-q:v', '35',  # Quality setting for videotoolbox (higher number = lower quality/size)
        '-b:v', f'{target_bitrate}',  # Target video bitrate
        '-maxrate', f'{target_bitrate}',  # Maximum bitrate
        '-bufsize', f'{target_bitrate * 2}',  # Buffer size (2x target for flexibility)
        '-tag:v', 'hvc1'  # Better compatibility
    ])
    
    # Handle HDR content if present
    if (video_info['video'].get('color_transfer') in ['arib-std-b67', 'smpte2084', 'smpte2086'] or
        video_info['video'].get('color_primaries') in ['bt2020']):
        settings['extra_params'].extend([
            '-allow_sw', '1',
            '-pix_fmt', 'p010le'
        ])
    
    return settings

def transcode_video(video_path, temp_path, video_id, conn, progress_callback=None):
    """Transcode the video to save space while maintaining quality."""
    try:
        # Get video info
        info = get_video_info(video_path)
        if not info:
            log.error(f"Failed to get video info for {video_path}")
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE videos 
                SET status = 'error',
                    error_message = ?
                WHERE id = ?
            """, ('Failed to get video info', video_id))
            conn.commit()
            return False
        
        # Get optimal encoding settings
        settings = get_optimal_encoding_settings(info)
        if not settings:
            log.error(f"Failed to determine encoding settings for {video_path}")
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE videos 
                SET status = 'error',
                    error_message = ?
                WHERE id = ?
            """, ('Failed to determine encoding settings', video_id))
            conn.commit()
            return False
        
        # Create a temp directory for progress files if it doesn't exist
        temp_dir = os.path.join(os.path.dirname(video_path), '.video_reconvert_temp')
        os.makedirs(temp_dir, exist_ok=True)
        
        # Prepare FFmpeg command
        cmd = [
            'ffmpeg',
            '-i', video_path,
            '-c:v', settings['codec'],
            '-c:a', 'aac',
            '-b:a', settings['audio_bitrate'],
            '-progress', 'pipe:1'  # Write progress to stdout
        ]
        
        # Add all extra parameters
        cmd.extend(settings['extra_params'])
        
        # Add metadata
        settings_str = json.dumps(settings)
        cmd.extend([
            '-metadata', f'comment=Processed by video_reconvert v1.0',
            '-metadata', f'description=Original size: {format_size(info["size"])}',
            '-metadata', f'encoding_settings={settings_str}',
            '-metadata', f'copyright=video_reconvert',
            '-y',  # Overwrite output file
            temp_path
        ])
        
        # Log the detailed command info
        debug_message = "\nExecuting ffmpeg command:\n"
        debug_message += "=" * 80 + "\n"
        debug_message += f"Input file: {video_path}\n"
        debug_message += f"Output file: {temp_path}\n"
        debug_message += f"Full command: {' '.join(cmd)}\n"
        debug_message += f"Settings: {json.dumps(settings, indent=2)}\n"
        debug_message += "=" * 80
        log.debug(debug_message)
        
        # Create process with pipes for both stdout and stderr
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1  # Line buffered
        )
        
        # Monitor progress
        duration = float(info['duration'])
        current_time = 0
        progress_data = {}
        stderr_output = []
        
        while True:
            # Check if process has ended
            if process.poll() is not None:
                break
                
            # Use select to check which pipes have data
            rlist, _, _ = select.select([process.stdout, process.stderr], [], [], 0.1)
            
            for ready_pipe in rlist:
                if ready_pipe == process.stdout:
                    # Handle progress data
                    line = process.stdout.readline().strip()
                    if line:
                        if '=' in line:
                            key, value = line.split('=', 1)
                            progress_data[key] = value
                            
                            if key == 'out_time_ms' and value.isdigit():
                                current_time = float(value) / 1000000  # Convert microseconds to seconds
                                if duration > 0 and progress_callback:
                                    progress = min(1.0, current_time / duration)
                                    progress_callback(video_id, progress)
                        
                        if line == 'progress=end':
                            break
                            
                elif ready_pipe == process.stderr:
                    # Collect stderr output
                    line = process.stderr.readline()
                    if line:
                        stderr_output.append(line)
        
        # Wait for process to complete with timeout
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            log.error("FFmpeg process had to be killed after timeout")
            return False
        
        # Clean up temp directory if empty
        try:
            os.rmdir(temp_dir)
        except OSError:
            # Directory not empty or other error, just leave it
            pass
        
        # Check process result
        if process.returncode != 0:
            error_msg = f'FFmpeg failed with return code {process.returncode}: {"".join(stderr_output)}'
            log.error(error_msg)
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE videos 
                SET status = 'error',
                    error_message = ?
                WHERE id = ?
            """, (error_msg, video_id))
            conn.commit()
            return False
            
        # Verify output file exists and has non-zero size
        if not os.path.exists(temp_path) or os.path.getsize(temp_path) == 0:
            log.error(f"Output file {temp_path} is missing or empty")
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE videos 
                SET status = 'error',
                    error_message = ?
                WHERE id = ?
            """, ('Output file is missing or empty', video_id))
            conn.commit()
            return False
            
        # Compare sizes and check for bloat
        original_size = os.path.getsize(video_path)
        new_size = os.path.getsize(temp_path)
        is_bloated = new_size > original_size
        
        # Get new video info to extract bitrate
        new_info = get_video_info(temp_path)
        new_bitrate = new_info['bit_rate'] if new_info else None
        
        # Update database with results
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE videos 
            SET status = ?,
                original_size = ?, 
                new_size = ?,
                new_bitrate = ?,
                is_bloated = ?,
                date_completed = ?
            WHERE id = ?
        """, ('completed', original_size, new_size, new_bitrate, is_bloated, datetime.now(), video_id))
        conn.commit()
        
        # Calculate size change percentage
        change_pct = (new_size/original_size - 1) * 100
        
        # Log success with condensed size information
        log.info(f"Successfully transcoded: {os.path.basename(video_path)} [{format_size(original_size)} → {format_size(new_size)} ({change_pct:.1f}%)]")
        
        return True
        
    except Exception as e:
        log.error(f"Error transcoding {video_path}: {str(e)}")
        traceback.print_exc()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE videos 
            SET status = 'error',
                error_message = ?
            WHERE id = ?
        """, (str(e), video_id))
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

def update_video_metadata(conn, video_id, video_path):
    """Update video metadata in the database after analysis."""
    cursor = conn.cursor()
    
    # Get comprehensive video info
    info = get_video_info(video_path)
    if not info:
        # Update as skipped due to info extraction failure
        cursor.execute("""
        UPDATE videos 
        SET status = 'skipped',
            skip_reason = ?
        WHERE id = ?
        """, ('Failed to extract video information', video_id))
        conn.commit()
        return False
    
    # Check if this file was already processed by our tool
    if video_utils.was_processed_by_tool(info) and not config.VIDEO_ENCODING['force_reconvert']:
        cursor.execute("""
        UPDATE videos 
        SET status = 'skipped',
            skip_reason = ?
        WHERE id = ?
        """, ('Already processed by video_reconvert', video_id))
        conn.commit()
        log.info(f"Skipping already processed video: {video_path}")
        return False
    
    # Run sanity checks with force_reconvert flag
    issues = video_utils.run_sanity_checks(info, config.VIDEO_ENCODING['force_reconvert'])
    
    # Check for critical issues (error level)
    critical_issues = [issue for issue in issues if issue['level'] == 'error']
    if critical_issues:
        skip_reason = critical_issues[0]['message']
        if critical_issues[0].get('details'):
            skip_reason += f" - {critical_issues[0]['details']}"
        cursor.execute("""
        UPDATE videos 
        SET status = 'skipped',
            skip_reason = ?
        WHERE id = ?
        """, (skip_reason, video_id))
        conn.commit()
        log.warning(f"Skipping video due to critical issues: {video_path}. Reason: {skip_reason}")
        return False
    
    # Get optimal encoding settings based on video analysis
    optimal_settings = video_utils.get_optimal_encoding_settings(info)
    
    # Update database with metadata
    cursor.execute("""
    UPDATE videos 
    SET original_size = ?,
        original_duration = ?,
        original_width = ?,
        original_height = ?,
        original_bitrate = ?,
        custom_settings = ?
    WHERE id = ?
    """, (
        info['size'],
        info['duration'],
        info['video']['width'],
        info['video']['height'],
        info['bit_rate'],
        json.dumps(optimal_settings) if optimal_settings else None,
        video_id
    ))
    conn.commit()
    return True

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
        
        # Initialize the rolodex with all video filenames (basename only) and their full paths
        video_basenames = [os.path.basename(v) for v in videos]
        ui.initialize_rolodex(video_basenames, source_files=videos)  # Pass both basenames and full paths
        
        # Clean up database entries for missing files
        if source_path:
            log.debug("Starting cleanup of database entries for missing files")
            cursor = conn.cursor()
            cursor.execute("SELECT id, filepath FROM videos WHERE status NOT IN ('completed', 'skipped')")
            db_files = cursor.fetchall()
            log.debug(f"Found {len(db_files)} files to check for existence")
            
            missing_count = 0
            for file_id, filepath in db_files:
                log.debug(f"Checking if file exists: {filepath}")
                if not os.path.exists(filepath):
                    log.debug(f"File does not exist, marking as skipped: {filepath}")
                    cursor.execute("""
                    UPDATE videos 
                    SET status = 'skipped', skip_reason = ? 
                    WHERE id = ?
                    """, ('File no longer exists on disk', file_id))
                    missing_count += 1
                    log.warning(f"File in database no longer exists: {filepath}")
            
            if missing_count > 0:
                log.debug(f"Committing changes for {missing_count} missing files")
                conn.commit()
                log.warning(f"Marked {missing_count} files as missing because they no longer exist on disk.")
        
        # Register all videos in database
        log.debug(f"Starting registration of {len(videos)} videos")
        registration_count = 0
        for video_path in videos:
            log.debug(f"Registering video {registration_count + 1}/{len(videos)}: {video_path}")
            register_video(conn, video_path)
            registration_count += 1
            if registration_count % 100 == 0:
                log.debug(f"Registered {registration_count}/{len(videos)} videos")
        log.debug("Completed video registration")
        
        # Display initial status
        log.debug("Gathering statistics for initial status display")
        display_status(conn)
        
        # If analyze_only mode, don't proceed with transcoding
        if analyze_only:
            log.info("Analysis complete. Not performing transcoding (--analyze-only specified).")
            return
        
        # Process videos that haven't been completed
        log.debug("Querying for pending videos")
        cursor = conn.cursor()
        where_clause = "status = 'pending'"
        if config.DATABASE['retry_errors']:
            where_clause += " OR status = 'error'"
        
        cursor.execute(f"SELECT id, filepath FROM videos WHERE {where_clause}")
        pending_videos = cursor.fetchall()
        log.debug(f"Found {len(pending_videos)} pending videos to process")
        
        # Filter out files that no longer exist
        log.debug("Filtering out non-existent files from pending videos")
        valid_pending_videos = []
        for video_id, video_path in pending_videos:
            log.debug(f"Checking if pending video exists: {video_path}")
            if os.path.exists(video_path):
                # Update metadata and check if we should process this video
                log.debug(f"Updating metadata for: {video_path}")
                if update_video_metadata(conn, video_id, video_path):
                    valid_pending_videos.append((video_id, video_path))
            else:
                log.debug(f"Pending video no longer exists, marking as skipped: {video_path}")
                cursor.execute("""
                UPDATE videos 
                SET status = 'skipped', skip_reason = ? 
                WHERE id = ?
                """, ('File no longer exists on disk', video_id))
                log.warning(f"Skipping missing file: {video_path}")
        
        log.debug("Committing changes after filtering pending videos")
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
            
            # Update the rolodex to show current file
            ui.set_current_file(base_name)
            
            current_temp_file = temp_path
            log.info(f"Processing: {base_name}")
            
            # Display detailed analysis
            analyze_video(video_path)
            
            # Start progress tracking
            ui.start_progress(f"Transcoding {base_name}")
            
            success = transcode_video(video_path, temp_path, video_id, conn, ui.update_progress)
            
            # Stop progress tracking
            ui.stop_progress()
            
            # Check if transcoding was successful
            if success and not exit_requested:
                if os.path.exists(temp_path):
                    # Get original and new file sizes
                    orig_info = get_video_info(video_path)
                    new_info = get_video_info(temp_path)
                    
                    if orig_info and new_info and 'size' in orig_info and 'size' in new_info:
                        orig_size = orig_info['size']
                        new_size = new_info['size']
                        
                        # Check if the new file is larger than the original
                        if new_size > orig_size:
                            # Mark as warning status in rolodex
                            ui.set_file_status(base_name, 'warning')
                            ui.update_file_size(base_name, new_size)
                            
                            # Rename temp file to bloated prefix
                            bloated_name = f"bloated_{base_name}"
                            bloated_path = os.path.join(directory, bloated_name)
                            os.rename(temp_path, bloated_path)
                            
                            # Log warning
                            log.warning(f"Transcoded file is larger than original: {base_name} "
                                      f"(Original: {format_size(orig_size)}, New: {format_size(new_size)}). "
                                      f"Keeping original and saving bloated version as {bloated_name}")
                            
                            # Update database
                            cursor = conn.cursor()
                            cursor.execute("""
                            UPDATE videos 
                            SET status = 'skipped', skip_reason = ?, new_size = ? 
                            WHERE id = ?
                            """, ('Transcoded file larger than original', new_size, video_id))
                            conn.commit()
                        else:
                            # Normal success case - file is smaller
                            if os.path.exists(video_path):
                                os.remove(video_path)
                                os.rename(temp_path, video_path)
                                ui.set_file_status(base_name, True)
                                ui.update_file_size(base_name, new_size)
                                # Advance the rolodex after successful processing
                                ui.advance_rolodex()
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
                        log.error(f"Failed to get file info for size comparison: {video_path}")
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                else:
                    log.error(f"Temp file {temp_path} not found after transcoding")
            else:
                log.error(f"Failed to transcode: {video_path}")
                ui.set_file_status(base_name, False)
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