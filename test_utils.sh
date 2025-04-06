#!/bin/zsh

# Default values
MAX_FILES=5
MAX_SIZE="1G"
MIN_SIZE="100M"
SOURCE_PATH="/Volumes/docker/TubeArchivist/media/UCQANb2YPwAtK-IQJrLaaUFw"
DEST_PATH="vids"
RESET_DB=false

# Help message
show_help() {
    cat << EOF
Usage: $0 [options] <source_path> <destination_path>

Options:
    -n, --max-files NUM     Maximum number of files to copy (default: 5)
    -s, --max-size SIZE     Maximum size per file (e.g., "500M", "1G") (default: 1G)
    -m, --min-size SIZE     Minimum size per file (e.g., "100M", "1G") (default: 100M)
    -r, --reset-db          Reset the SQLite database
    -h, --help             Show this help message

Example:
    $0 -n 10 -s 500M -m 200M /path/to/source /path/to/destination
    $0 -r  # Only reset the database
EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--max-files)
            MAX_FILES="$2"
            shift 2
            ;;
        -s|--max-size)
            MAX_SIZE="$2"
            shift 2
            ;;
        -m|--min-size)
            MIN_SIZE="$2"
            shift 2
            ;;
        -r|--reset-db)
            RESET_DB=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            if [[ -z "$SOURCE_PATH" ]]; then
                SOURCE_PATH="$1"
            elif [[ -z "$DEST_PATH" ]]; then
                DEST_PATH="$1"
            else
                echo "Error: Unexpected argument: $1"
                show_help
                exit 1
            fi
            shift
            ;;
    esac
done

# Function to reset the database
reset_database() {
    echo "Resetting SQLite database..."
    if [[ -f video_conversion.db ]]; then
        rm video_conversion.db
        echo "Existing database deleted."
    fi
    # Create fresh database using the Python script
    python3 video_reconvert.py --analyze-only .
    echo "Database reset complete."
}

# Reset database if requested
if [[ "$RESET_DB" = true ]]; then
    reset_database
fi

# Exit if we're only resetting the database
if [[ -z "$SOURCE_PATH" || -z "$DEST_PATH" ]]; then
    if [[ "$RESET_DB" = true ]]; then
        exit 0
    else
        echo "Error: Source and destination paths are required for file seeding."
        show_help
        exit 1
    fi
fi

# Validate source path
if [[ ! -d "$SOURCE_PATH" ]]; then
    echo "Error: Source path does not exist or is not a directory: $SOURCE_PATH"
    exit 1
fi

# Create destination directory if it doesn't exist
mkdir -p "$DEST_PATH"

# Find video files, filter by size, and select random sample
echo "Finding video files between $MIN_SIZE and $MAX_SIZE in size..."
find "$SOURCE_PATH" -type f \( -name "*.mp4" -o -name "*.mkv" -o -name "*.mov" -o -name "*.avi" \) -size +"$MIN_SIZE" -size -"$MAX_SIZE" | \
    sort -R | \
    head -n "$MAX_FILES" > /tmp/files_to_copy.txt

# Count files found
FILE_COUNT=$(wc -l < /tmp/files_to_copy.txt)
if [[ $FILE_COUNT -eq 0 ]]; then
    echo "No video files found matching the criteria!"
    exit 1
fi

echo "Found $FILE_COUNT files to copy..."

# Copy files using rsync
echo "Copying files to destination..."
while IFS= read -r file; do
    rsync -ah --progress "$file" "$DEST_PATH/"
done < /tmp/files_to_copy.txt

# Cleanup
rm /tmp/files_to_copy.txt

echo "Done! Copied $FILE_COUNT files to $DEST_PATH" 
