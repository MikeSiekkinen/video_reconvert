#!/usr/bin/env python3
"""
Utility functions for video file validation and analysis.
"""

import os
import json
import subprocess
import logging
from typing import Dict, Any, Tuple, Optional, List

# Get logger
log = logging.getLogger("video_reconvert")

def check_ffmpeg_availability() -> bool:
    """Check if ffmpeg and ffprobe are available in the system."""
    try:
        subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True)
        subprocess.run(['ffprobe', '-version'], capture_output=True, check=True)
        return True
    except (subprocess.SubprocessError, FileNotFoundError):
        return False

def get_video_info(filepath: str) -> Optional[Dict[str, Any]]:
    """Use ffprobe to extract comprehensive video metadata."""
    try:
        # Only check file existence and size right before accessing the file
        # This avoids unnecessary I/O operations when working with large datasets
        if not os.path.exists(filepath):
            log.error(f"File does not exist: {filepath}")
            return None
            
        # Check if the file is accessible and not zero size
        if os.path.getsize(filepath) == 0:
            log.error(f"File is empty (zero bytes): {filepath}")
            return None
            
        result = subprocess.run([
            'ffprobe', 
            '-v', 'quiet', 
            '-print_format', 'json', 
            '-show_format', 
            '-show_streams', 
            filepath
        ], capture_output=True, text=True, check=True)
        
        info = json.loads(result.stdout)
        
        # Extract relevant information
        video_stream = next((s for s in info['streams'] if s['codec_type'] == 'video'), None)
        audio_stream = next((s for s in info['streams'] if s['codec_type'] == 'audio'), None)
        
        if not video_stream:
            log.error(f"No video stream found in {filepath}")
            return None
            
        # Build and return a comprehensive info dictionary
        file_info = {
            'filepath': filepath,
            'filename': os.path.basename(filepath),
            'size': os.path.getsize(filepath),
            'format': info['format'].get('format_name', ''),
            'duration': float(info['format'].get('duration', 0)),
            'bit_rate': int(info['format'].get('bit_rate', 0)),
            'video': {
                'codec': video_stream.get('codec_name', ''),
                'profile': video_stream.get('profile', ''),
                'width': int(video_stream.get('width', 0)),
                'height': int(video_stream.get('height', 0)),
                'display_aspect_ratio': video_stream.get('display_aspect_ratio', ''),
                'pix_fmt': video_stream.get('pix_fmt', ''),
                'color_space': video_stream.get('color_space', ''),
                'color_transfer': video_stream.get('color_transfer', ''),
                'color_primaries': video_stream.get('color_primaries', ''),
                'field_order': video_stream.get('field_order', ''),
                'refs': int(video_stream.get('refs', 0)),
                'is_avc': video_stream.get('is_avc', ''),
                'frame_rate': eval_fraction(video_stream.get('avg_frame_rate', '0/1')),
                'bit_rate': int(video_stream.get('bit_rate', 0)) if 'bit_rate' in video_stream else 0,
            },
            'audio': {} if not audio_stream else {
                'codec': audio_stream.get('codec_name', ''),
                'sample_rate': int(audio_stream.get('sample_rate', 0)),
                'channels': int(audio_stream.get('channels', 0)),
                'bit_rate': int(audio_stream.get('bit_rate', 0)) if 'bit_rate' in audio_stream else 0,
            }
        }
        
        return file_info
    except (subprocess.SubprocessError, json.JSONDecodeError, KeyError) as e:
        log.error(f"Failed to get video info for {filepath}: {str(e)}")
        return None

def eval_fraction(fraction_str: str) -> float:
    """Evaluate a fraction string like '24000/1001' to a float."""
    try:
        if '/' in fraction_str:
            num, denom = map(int, fraction_str.split('/'))
            return num / denom if denom != 0 else 0
        return float(fraction_str)
    except (ValueError, ZeroDivisionError):
        return 0.0

def run_sanity_checks(video_info: Dict[str, Any], force_reconvert: bool = False) -> List[Dict[str, Any]]:
    """
    Run a series of sanity checks on a video file to identify potential issues.
    
    Args:
        video_info: Dictionary containing video metadata
        force_reconvert: Whether to allow reconverting already processed files
        
    Returns:
        List of issues detected, each as a dict with 'level', 'message', and 'details' keys.
        Empty list means no issues detected.
    """
    issues = []
    
    # Only proceed if we have valid video info
    if not video_info:
        return [{'level': 'error', 'message': 'Failed to extract video information', 'details': ''}]
    
    # Check if already processed by our tool
    if 'format' in video_info and 'tags' in video_info['format']:
        tags = video_info['format']['tags']
        if 'comment' in tags and tags['comment'].startswith('Processed by video_reconvert'):
            level = 'error' if not force_reconvert else 'info'
            issues.append({
                'level': level,
                'message': 'Video was already processed by video_reconvert',
                'details': 'Use --force-reconvert to process again' if not force_reconvert else 'Forcing reconversion as requested'
            })
            if not force_reconvert:
                return issues  # Stop checking if we won't process it anyway
    
    # Check if file is actually a video
    if not video_info.get('video', {}).get('codec'):
        issues.append({
            'level': 'error', 
            'message': 'No valid video codec detected', 
            'details': f"File: {video_info.get('filename')}"
        })
        return issues  # No point continuing checks
    
    # Check for already optimized codecs
    if video_info['video'].get('codec') == 'hevc':
        level = 'error' if not force_reconvert else 'info'
        issues.append({
            'level': level,
            'message': 'Video is already encoded with HEVC codec',
            'details': 'Use --force-reconvert to process again' if not force_reconvert else 'Re-encoding might not provide significant size reduction'
        })
        if not force_reconvert:
            return issues  # Stop checking if we won't process it anyway
    
    # Check for VP9 or AV1 codecs (which are often more efficient than HEVC)
    if video_info['video'].get('codec') in ['vp9', 'av1']:
        issues.append({
            'level': 'warning',  # Warning, not error, so processing still occurs
            'message': f"Video uses efficient {video_info['video'].get('codec')} codec",
            'details': f"Using aggressive bitrate targeting to ensure size reduction"
        })
    
    # Check video resolution and dimensions
    width = video_info['video'].get('width', 0)
    height = video_info['video'].get('height', 0)
    
    if width == 0 or height == 0:
        issues.append({
            'level': 'error',
            'message': 'Invalid video dimensions',
            'details': f"Width: {width}, Height: {height}"
        })
    
    # Check odd dimensions which can cause problems with some codecs
    if width % 2 != 0 or height % 2 != 0:
        issues.append({
            'level': 'warning',
            'message': 'Video has odd dimensions which might cause issues with some encoders',
            'details': f"Width: {width}, Height: {height}"
        })
    
    # Check if already in target resolution or smaller
    if height <= 1080 and width <= 1920:
        issues.append({
            'level': 'info',
            'message': 'Video is already at or below target resolution (1080p)',
            'details': f"Current resolution: {width}x{height}"
        })
    
    # Check for extremely short duration
    if video_info.get('duration', 0) < 1.0:
        issues.append({
            'level': 'warning',
            'message': 'Video has extremely short duration',
            'details': f"Duration: {video_info.get('duration', 0):.2f} seconds"
        })
    
    # Check for very high bitrate which might indicate inefficient encoding
    if video_info.get('bit_rate', 0) > 20000000:  # 20 Mbps
        issues.append({
            'level': 'info',
            'message': 'Video has very high bitrate',
            'details': f"Bitrate: {video_info.get('bit_rate', 0) / 1000000:.2f} Mbps"
        })
    
    # Check for variable frame rate
    if 'video' in video_info and 'r_frame_rate' in video_info['video'] and 'avg_frame_rate' in video_info['video']:
        if video_info['video']['r_frame_rate'] != video_info['video']['avg_frame_rate']:
            issues.append({
                'level': 'warning',
                'message': 'Video has variable frame rate which might cause issues',
                'details': f"Frame rates: r={video_info['video']['r_frame_rate']}, avg={video_info['video']['avg_frame_rate']}"
            })
    
    # Check for non-standard pixel format
    standard_pix_fmts = ['yuv420p', 'yuv422p', 'yuv444p', 'yuvj420p', 'yuvj422p', 'yuvj444p', 
                        'yuv420p10le', 'yuv422p10le', 'yuv444p10le']
    if video_info['video'].get('pix_fmt') not in standard_pix_fmts:
        issues.append({
            'level': 'warning',
            'message': 'Video has non-standard pixel format',
            'details': f"Format: {video_info['video'].get('pix_fmt')}"
        })
    
    # Check for color space/transfer/primaries inconsistencies
    color_space = video_info['video'].get('color_space')
    color_transfer = video_info['video'].get('color_transfer')
    color_primaries = video_info['video'].get('color_primaries')
    
    if (color_space and color_transfer and color_primaries and
        not (color_space == color_transfer == color_primaries)):
        issues.append({
            'level': 'warning',
            'message': 'Video has inconsistent color specifications',
            'details': f"Space: {color_space}, Transfer: {color_transfer}, Primaries: {color_primaries}"
        })
    
    # Check file size efficiency (bits per pixel)
    if width > 0 and height > 0 and video_info.get('duration', 0) > 0:
        bpp = (video_info.get('bit_rate', 0) / (width * height * video_info.get('duration', 0)))
        if bpp > 0.2:  # Arbitrary threshold, adjust based on experience
            issues.append({
                'level': 'info',
                'message': 'Video has high bits per pixel ratio',
                'details': f"BPP: {bpp:.6f} (potential for good compression)"
            })
    
    # Check audio issues
    if not video_info.get('audio'):
        issues.append({
            'level': 'warning',
            'message': 'Video has no audio stream',
            'details': 'This might be intentional, but worth checking'
        })
    
    # Check interlaced content
    field_order = video_info['video'].get('field_order', '').lower()
    if field_order not in ['progressive', 'unknown', '']:
        issues.append({
            'level': 'warning',
            'message': 'Video appears to be interlaced',
            'details': f"Field order: {field_order}"
        })
    
    # Check if duration matches between container and video stream
    format_duration = float(video_info.get('duration', 0))
    video_duration = float(video_info['video'].get('duration', 0)) if 'duration' in video_info['video'] else 0
    
    if video_duration > 0 and abs(format_duration - video_duration) > 1.0:
        issues.append({
            'level': 'warning',
            'message': 'Container duration differs from video stream duration',
            'details': f"Container: {format_duration:.2f}s, Video: {video_duration:.2f}s"
        })
    
    return issues

def analyze_conversion_potential(video_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyze a video file to estimate potential space savings and optimal conversion settings.
    
    Returns:
        Dictionary with analysis results and recommendations.
    """
    if not video_info:
        return {'error': 'No valid video information available'}
    
    # Extract relevant info
    original_size = video_info.get('size', 0)
    duration = video_info.get('duration', 0)
    width = video_info['video'].get('width', 0) 
    height = video_info['video'].get('height', 0)
    original_bitrate = video_info.get('bit_rate', 0)
    
    if duration == 0 or width == 0 or height == 0:
        return {'error': 'Invalid video properties (zero duration or dimensions)'}
    
    # Calculate pixels per frame
    pixels = width * height
    
    # Initial analysis
    analysis = {
        'original': {
            'size_bytes': original_size,
            'size_mb': original_size / (1024 * 1024),
            'duration_sec': duration,
            'resolution': f"{width}x{height}",
            'bitrate_kbps': original_bitrate / 1000,
        },
        'recommendations': {},
        'estimated_savings': {}
    }
    
    # Determine target resolution
    target_height = 1080
    if height <= 720:
        target_height = height  # Keep original if already 720p or lower
    elif height <= 1080:
        target_height = height  # Keep original if already 1080p or lower
    
    # Calculate scaling factor and new dimensions
    scale_factor = target_height / height if height > 0 else 1
    target_width = int(width * scale_factor / 2) * 2  # Ensure even width
    target_height = int(height * scale_factor / 2) * 2
    
    # Determine target bitrate for HEVC
    # HEVC typically requires ~50-60% bitrate of H.264 for same quality
    # Start with a base rate depending on resolution
    if target_height <= 480:
        base_bitrate = 1000  # 1 Mbps for SD
    elif target_height <= 720:
        base_bitrate = 2500  # 2.5 Mbps for 720p
    elif target_height <= 1080:
        base_bitrate = 4000  # 4 Mbps for 1080p
    else:
        base_bitrate = 8000  # 8 Mbps for higher resolutions
    
    # Adjust for content complexity (using original bitrate as a proxy)
    complexity_factor = min(max(original_bitrate / (width * height * 0.15), 0.8), 1.5)
    target_bitrate = int(base_bitrate * complexity_factor)
    
    # For archival, we could go lower with CRF-based encoding
    # Recommend CRF value based on content
    if complexity_factor < 0.9:
        # Simple content (like animation)
        crf_value = 30
    elif complexity_factor < 1.2:
        # Normal content
        crf_value = 28
    else:
        # Complex content
        crf_value = 26
    
    # Calculate more accurate size estimation
    # HEVC typically achieves 40-50% better compression than H.264
    hevc_compression_ratio = 0.6  # 40% better = 60% of original size
    
    # Calculate the quality factor impact
    # q:v 35 is a moderate quality setting, should reduce size by ~30%
    quality_factor = 0.7
    
    # Calculate resolution impact if we're downscaling
    if target_width < width or target_height < height:
        resolution_factor = (target_width * target_height) / (width * height)
    else:
        resolution_factor = 1.0
    
    # Calculate estimated size
    estimated_size_bytes = int(
        original_size *  # Start with original size
        hevc_compression_ratio *  # HEVC compression benefit
        quality_factor *  # Quality setting impact
        resolution_factor  # Resolution change impact
    )
    
    # Calculate savings percentage
    estimated_saving_pct = (1 - (estimated_size_bytes / original_size)) * 100 if original_size > 0 else 0
    
    # Build recommendations
    analysis['recommendations'] = {
        'target_resolution': f"{target_width}x{target_height}",
        'target_bitrate_kbps': target_bitrate,
        'recommended_crf': crf_value,
        'codec': 'hevc_videotoolbox' if video_info['video'].get('codec') != 'hevc' else 'copy',
        'audio_bitrate': min(video_info.get('audio', {}).get('bit_rate', 0) / 1000, 128) if video_info.get('audio') else 96,
    }
    
    analysis['estimated_savings'] = {
        'estimated_size_mb': estimated_size_bytes / (1024 * 1024),
        'estimated_saving_pct': estimated_saving_pct,
        'estimated_saving_mb': (original_size - estimated_size_bytes) / (1024 * 1024),
    }
    
    return analysis

def get_optimal_encoding_settings(video_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Determine optimal encoding settings for a specific video.
    
    Returns:
        Dictionary with customized encoding parameters.
    """
    # Run initial analysis
    analysis = analyze_conversion_potential(video_info)
    if 'error' in analysis:
        return {}
    
    # Start with default parameters
    settings = {
        'codec': analysis['recommendations']['codec'],
        'crf': analysis['recommendations']['recommended_crf'],
        'audio_bitrate': f"{int(analysis['recommendations']['audio_bitrate'])}k",
        'extra_params': []
    }
    
    # Get target resolution
    target_width, target_height = map(int, analysis['recommendations']['target_resolution'].split('x'))
    
    # Add scaling if needed
    if (target_width != video_info['video']['width'] or 
        target_height != video_info['video']['height']):
        settings['extra_params'].append('-vf')
        settings['extra_params'].append(f'scale={target_width}:{target_height}')
    
    # Handle high dynamic range (HDR) content
    is_hdr = (
        video_info['video'].get('color_transfer') in ['arib-std-b67', 'smpte2084', 'smpte2086'] or
        video_info['video'].get('color_primaries') in ['bt2020']
    )
    
    if is_hdr:
        # HDR content needs special handling to preserve HDR metadata
        if settings['codec'] == 'hevc_videotoolbox':
            # VideoToolbox can preserve HDR, but needs special flags
            settings['extra_params'].extend([
                '-allow_sw', '1',
                '-pix_fmt', 'p010le',  # 10-bit 4:2:0 format for HDR
                '-color_primaries', 'bt2020',
                '-color_trc', video_info['video'].get('color_transfer', 'smpte2084'),
                '-colorspace', 'bt2020nc'
            ])
        else:
            # Software encoding needs to preserve HDR metadata
            settings['extra_params'].extend([
                '-pix_fmt', 'yuv420p10le',
                '-color_primaries', 'bt2020',
                '-color_trc', video_info['video'].get('color_transfer', 'smpte2084'),
                '-colorspace', 'bt2020nc'
            ])
    
    # Handle interlaced content
    field_order = video_info['video'].get('field_order', '').lower()
    if field_order not in ['progressive', 'unknown', '']:
        # Deinterlace the content
        # Remove any existing scale filter
        for i, param in enumerate(settings['extra_params']):
            if param == '-vf' and i < len(settings['extra_params']) - 1:
                scale_param = settings['extra_params'][i+1]
                settings['extra_params'][i+1] = f'yadif=0:-1:0,{scale_param}'
                break
        else:
            # If no scale filter exists
            settings['extra_params'].extend(['-vf', 'yadif=0:-1:0'])
    
    # Handle variable frame rate
    if 'video' in video_info and 'r_frame_rate' in video_info['video'] and 'avg_frame_rate' in video_info['video']:
        if video_info['video']['r_frame_rate'] != video_info['video']['avg_frame_rate']:
            # Force constant frame rate
            target_fps = round(eval_fraction(video_info['video'].get('avg_frame_rate', '24/1')))
            settings['extra_params'].extend(['-r', str(target_fps)])
    
    # Special case for animation content
    is_animation = False
    # This is a simple heuristic; a more complex analysis would involve scene detection
    if (video_info['video'].get('bit_rate', 0) > 0 and 
        video_info['video'].get('width', 0) > 0 and 
        video_info['video'].get('height', 0) > 0):
        
        # Calculate bits per pixel
        bpp = (video_info['video'].get('bit_rate', 0) / 
              (video_info['video'].get('width', 0) * video_info['video'].get('height', 0) * 
               video_info.get('duration', 1)))
        
        # Animation typically has a lower bits per pixel
        if bpp < 0.05:
            is_animation = True
    
    if is_animation:
        # Animation compresses extremely well with higher CRF
        settings['crf'] = min(settings['crf'] + 2, 32)
    
    return settings 

def was_processed_by_tool(info):
    """Check if a video was already processed by video_reconvert."""
    if not info or 'format' not in info or 'tags' not in info['format']:
        return False
    
    tags = info['format']['tags']
    return 'comment' in tags and tags['comment'].startswith('Processed by video_reconvert') 