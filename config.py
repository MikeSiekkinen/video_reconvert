#!/usr/bin/env python3
"""
Configuration settings for the video_reconvert tool.
These settings can be adjusted to customize the transcoding process.
"""

import platform
import subprocess

# Check if running on macOS
IS_MACOS = platform.system() == 'Darwin'

# Check if we're on Apple Silicon
def is_apple_silicon():
    if not IS_MACOS:
        return False
    try:
        output = subprocess.check_output(['sysctl', '-n', 'machdep.cpu.brand_string']).decode().strip()
        return 'Apple' in output
    except:
        return False

HAS_APPLE_SILICON = is_apple_silicon()

# Default video encoding settings
VIDEO_ENCODING = {
    # Video codec settings
    'codec': 'hevc_videotoolbox' if HAS_APPLE_SILICON else 'libx265',
    
    # Audio settings
    'audio_codec': 'aac',        # More widely compatible than opus
    'audio_bitrate': '96k',      # Good balance of quality and size
    
    # Additional ffmpeg parameters for better quality/compression
    'extra_params': [
        '-movflags', '+faststart',  # Optimize for web streaming
        '-vf', 'scale=1280:720',    # 720p resolution
    ] + (
        # Apple Silicon specific settings
        [
            '-q:v', '26',           # Quality setting for videotoolbox (0-100, lower is better quality)
            '-tag:v', 'hvc1',       # Tag for broader compatibility
        ] if HAS_APPLE_SILICON else [
            # Non-Apple Silicon settings (software encoding)
            '-crf', '28',           # Constant Rate Factor for software encoding
            '-preset', 'medium',     # Balance between speed and compression
            '-pix_fmt', 'yuv420p',  # Standard pixel format
            '-tag:v', 'hvc1',       # Better compatibility
        ]
    ),
}

# Database settings
DATABASE = {
    'filename': 'video_conversion.db',
    
    # When True, will reprocess files marked as 'error' in previous runs
    'retry_errors': True,
}

# Logging settings
LOGGING = {
    'filename': 'video_reconvert.log',
    'level': 'INFO',  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    
    # Maximum log file size in bytes before rotation (5 MB default)
    'max_size': 5 * 1024 * 1024,
    
    # Number of backup log files to keep
    'backup_count': 3,
}

# File search settings
FILE_SEARCH = {
    # File extensions to look for (default: mp4 only)
    'extensions': ['.mp4', '.mkv', '.webm'],  # Added more formats
    
    # Set to True to also search for .MP4 (case-sensitive search)
    'case_sensitive': False,
    
    # Maximum depth for recursive search (None for unlimited)
    'max_depth': None,
    
    # Skip directories that match these patterns
    'skip_dirs': ['.git', 'node_modules', '__pycache__', '.temp'],
}

# UI settings
UI = {
    # Display theme colors
    'header_style': 'green',
    'error_style': 'bold red',
    'info_style': 'blue',
    'progress_style': 'yellow',
    'warning_style': 'bold yellow',
    
    # Update frequency for progress bar (in seconds)
    'update_frequency': 0.5,
} 