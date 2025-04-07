#!/usr/bin/env python3
"""
Configuration settings for the video_reconvert tool.
These settings can be adjusted to customize the transcoding process.
"""

import platform
import subprocess
import multiprocessing

# Check if running on macOS
IS_MACOS = platform.system() == 'Darwin'

# Get CPU count for VP9 threading
CPU_COUNT = multiprocessing.cpu_count()
VP9_TILE_COLUMNS = min(int.bit_length(CPU_COUNT - 1), 6)  # Must be power of 2, max 6

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
    'codec': 'libvpx-vp9',  # Using VP9 for better compression
    
    # Audio settings
    'audio_codec': 'aac',        # More widely compatible than opus
    'audio_bitrate': '128k',     # Increased for better quality
    
    # Processing control
    'force_reconvert': False,    # When True, allows reconverting files that were already processed
    
    # VP9 settings with Apple Silicon optimizations
    'extra_params': [
        # Core VP9 settings
        '-cpu-used', '2',                    # Balance between speed and quality (0-8)
        '-row-mt', '1',                      # Enable row-based multithreading
        '-tile-columns', str(VP9_TILE_COLUMNS),  # Parallel frame processing
        '-tile-rows', '2',                   # Increased parallelization
        '-frame-parallel', '1',              # Enable parallel frame processing
        '-threads', str(CPU_COUNT),          # Use all available CPU threads
        
        # Apple Silicon specific optimizations
        '-deadline', 'realtime' if HAS_APPLE_SILICON else 'good',  # Faster encoding on Apple Silicon
        '-speed', '4' if HAS_APPLE_SILICON else '2',  # Higher speed setting on Apple Silicon
        '-row-mt', '1',                      # Row-based multithreading
        '-frame-parallel', '1',              # Parallel frame processing
        
        # Quality settings
        '-auto-alt-ref', '1',                # Enable alternative reference frames
        '-lag-in-frames', '25',              # Lookahead buffer
        '-aq-mode', '2',                     # Adaptive quantization mode
        '-crf', '33',                        # Higher value = more compression
        '-b:v', '0',                         # Let CRF control quality
        '-movflags', '+faststart',           # Optimize for web streaming
    ],
    
    # Resolution settings
    'resolution': {
        'vertical_target_width': 720,        # Target width for vertical videos
        'horizontal_target_height': 720,     # Target height for horizontal videos
        'maintain_aspect_ratio': True,       # Always maintain aspect ratio
        'force_even_dimensions': True,       # Ensure dimensions are even numbers
    },
    
    # Bitrate control
    'bitrate': {
        'target_percentage': 0.35,           # Target 35% of original bitrate
        'max_ratio': 1.2,                    # Maximum bitrate ratio
        'min_ratio': 0.8,                    # Minimum bitrate ratio
    }
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
    'level': 'DEBUG',  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    
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