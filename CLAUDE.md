# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands
- Run the app: `python video_reconvert.py /path/to/videos`
- Run tests: `python -m unittest discover tests`
- Run with analyze only: `python video_reconvert.py /path/to/videos --analyze-only`
- Analyze single video: `python video_reconvert.py --analyze-video /path/to/video.mp4`
- Reset database: `./test_utils.sh --reset-db`

## Style Guidelines
- **Imports**: Group standard library first, then third-party, then local imports
- **Formatting**: Follow PEP 8 conventions for Python code
- **Types**: Use type hints for function parameters and return values
- **Naming**: Use snake_case for variables/functions, CamelCase for classes
- **Error Handling**: Use try/except blocks with specific exception types
- **Logging**: Use the configured logger (`log`) instead of print statements
- **Documentation**: Add docstrings to functions and classes
- **Configuration**: Modify settings in `config.py` instead of hardcoding values

## File Organization
- `video_reconvert.py`: Main application entry point
- `video_utils.py`: Utility functions for video analysis
- `config.py`: Configuration settings
- `test_utils.sh`: Helper script for testing