#\!/bin/bash
# Script to reset custom_settings for pending videos
echo "Resetting custom_settings for pending and error videos in database..."
sqlite3 video_conversion.db "UPDATE videos SET custom_settings=NULL WHERE status='pending' OR status='error';"
echo "Done. The next run will use updated VP9 settings."
