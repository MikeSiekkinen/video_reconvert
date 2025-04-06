# Video Reconvert

A command-line tool for transcoding video files to save disk space while maintaining decent quality.

## Features

- Finds and transcodes all MP4 files in a source directory
- Uses ffmpeg/ffprobe for efficient transcoding
- Maintains aspect ratio and reasonable quality
- Tracks progress in SQLite database for fault tolerance
- Colorful terminal UI with progress bars
- Detailed logging for debugging
- Status updates with space-saving statistics
- Intelligent video analysis and per-file encoding optimization
- Sanity checks to identify problematic videos before processing

## Requirements

- Python 3.6+
- FFmpeg and FFprobe installed on your system
- Python dependencies (see requirements.txt)

## Installation

1. Ensure you have FFmpeg installed:
   ```
   # On macOS with Homebrew
   brew install ffmpeg
   
   # On Ubuntu/Debian
   sudo apt-get install ffmpeg
   
   # On Windows with Chocolatey
   choco install ffmpeg
   ```

2. Clone this repository and install dependencies:
   ```
   git clone https://github.com/yourusername/video_reconvert.git
   cd video_reconvert
   pip install -r requirements.txt
   ```

## Usage

Run the script with the source directory as a parameter:

```
python video_reconvert.py /path/to/videos
```

### Command-line Options

- `--extensions EXT1,EXT2,...` - Specify file extensions to process (default: .mp4)
- `--crf VALUE` - Set Constant Rate Factor (0-51, lower is better quality, default: 28 for HEVC)
- `--preset PRESET` - Set encoding preset (ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow)
- `--retry-errors` - Retry files that previously failed
- `--no-retry-errors` - Skip files that previously failed
- `--analyze-only` - Only analyze videos without transcoding
- `--analyze-video PATH` - Analyze a single video file without transcoding

Examples:
```bash
# Process all videos in a directory
python video_reconvert.py /path/to/videos

# Only analyze videos without transcoding them
python video_reconvert.py /path/to/videos --analyze-only

# Analyze a single video file
python video_reconvert.py --analyze-video /path/to/video.mp4
```

## Intelligent Video Analysis

The tool performs detailed analysis on each video file to:

1. **Detect potential issues** - Identifies problems that might affect transcoding
2. **Optimize encoding settings** - Customizes parameters based on content type
3. **Estimate space savings** - Provides projections of file size reduction

Issues detected include:
- Non-standard dimensions
- Interlaced content
- Variable frame rates
- HDR content requiring special handling
- Videos already in target format

## Configuration

The `config.py` file contains various settings that can be adjusted:

### Video Encoding Settings

```python
VIDEO_ENCODING = {
    'crf': 28,               # Quality level (0-51, lower is better)
    'preset': 'medium',      # Encoding speed vs compression ratio
    'codec': 'hevc_videotoolbox', # Hardware-accelerated HEVC on Apple Silicon
    'audio_codec': 'libopus', # Audio codec
    'audio_bitrate': '96k',   # Audio bitrate
    'extra_params': [...],    # Additional FFmpeg parameters
}
```

### File Search Settings

```python
FILE_SEARCH = {
    'extensions': ['.mp4', '.mkv', '.webm'],  # File extensions to look for
    'case_sensitive': False,                  # Case sensitive file matching
    'max_depth': None,                        # Maximum directory depth
    'skip_dirs': ['.git', 'node_modules', '__pycache__', '.temp'], # Directories to skip
}
```

### Database Settings

```python
DATABASE = {
    'filename': 'video_conversion.db',  # Database file path
    'retry_errors': True,               # Retry files that failed in previous runs
}
```

### Logging Settings

```python
LOGGING = {
    'filename': 'video_reconvert.log',  # Log file path
    'level': 'INFO',                    # Log level
    'max_size': 5 * 1024 * 1024,        # Max log file size before rotation
    'backup_count': 3,                  # Number of backup log files
}
```

## Database

The program uses SQLite to track conversion progress and statistics. The database file `video_conversion.db` will be created in the current directory and contains:

- Original video metadata (resolution, bitrate, etc.)
- Conversion status (pending, completed, error, skipped)
- File sizes before and after conversion
- Reasons for skipping files
- Custom encoding settings per file
- Date information and error messages if any

## Logs

Detailed logs are written to `video_reconvert.log` in the current directory. Log rotation is enabled to prevent the log file from growing too large.

## License

MIT 