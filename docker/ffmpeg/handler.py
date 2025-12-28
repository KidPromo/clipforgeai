"""
ClipForge FFmpeg Handler for RunPod
Processes video clips with FFmpeg on GPU-accelerated workers
"""

import runpod
import subprocess
import json
import os
import base64
import tempfile
import shutil
import requests
from pathlib import Path
from typing import Dict, Any

def download_file(url: str, destination: str) -> str:
    """Download file from URL with progress tracking"""
    print(f"[Download] Fetching video from {url}")

    try:
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0

        with open(destination, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        if downloaded % (1024 * 1024) == 0:  # Log every MB
                            print(f"[Download] Progress: {progress:.1f}%")

        print(f"[Download] Complete: {downloaded / 1024 / 1024:.2f} MB")
        return destination
    except Exception as e:
        raise Exception(f"Download failed: {str(e)}")

def get_video_duration(input_path: str) -> float:
    """Get video duration using ffprobe"""
    try:
        cmd = [
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            input_path
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        duration = float(result.stdout.strip())
        print(f"[Info] Video duration: {duration:.2f}s")
        return duration
    except Exception as e:
        print(f"[Warning] Could not get video duration: {e}")
        return 0.0

def extract_clip(
    input_path: str,
    output_path: str,
    start_time: float,
    duration: float,
    quality: str = 'high'
) -> str:
    """
    Extract clip using FFmpeg with optimized settings

    Quality presets:
    - high: CRF 23, slower preset (best quality, slower)
    - medium: CRF 25, fast preset (good quality, faster)
    - low: CRF 28, veryfast preset (lower quality, fastest)
    """

    print(f"[FFmpeg] Extracting clip: start={start_time}s, duration={duration}s, quality={quality}")

    # Quality settings
    quality_presets = {
        'high': {'crf': '23', 'preset': 'fast'},
        'medium': {'crf': '25', 'preset': 'faster'},
        'low': {'crf': '28', 'preset': 'veryfast'},
    }

    settings = quality_presets.get(quality, quality_presets['medium'])

    # FFmpeg command with web optimization
    cmd = [
        'ffmpeg',
        '-ss', str(start_time),           # Start time (input seeking for speed)
        '-i', input_path,                  # Input file
        '-t', str(duration),               # Duration
        '-c:v', 'libx264',                 # Video codec (H.264)
        '-preset', settings['preset'],     # Encoding speed/compression
        '-crf', settings['crf'],           # Quality (lower = better, 23 is high quality)
        '-c:a', 'aac',                     # Audio codec
        '-b:a', '128k',                    # Audio bitrate
        '-ar', '44100',                    # Audio sample rate
        '-movflags', '+faststart',         # Enable progressive download
        '-pix_fmt', 'yuv420p',            # Pixel format for compatibility
        '-y',                              # Overwrite output
        output_path
    ]

    print(f"[FFmpeg] Command: {' '.join(cmd)}")

    # Run FFmpeg with progress tracking
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )

        if result.returncode != 0:
            print(f"[FFmpeg] stderr output:\n{result.stderr}")
            raise Exception(f"FFmpeg processing failed: {result.stderr}")

        # Check if output file exists and has content
        if not os.path.exists(output_path):
            raise Exception("Output file was not created")

        file_size = os.path.getsize(output_path)
        if file_size == 0:
            raise Exception("Output file is empty")

        print(f"[FFmpeg] ✅ Clip extracted successfully: {file_size / 1024 / 1024:.2f} MB")
        return output_path

    except subprocess.TimeoutExpired:
        raise Exception("FFmpeg processing timed out after 10 minutes")
    except Exception as e:
        raise Exception(f"FFmpeg error: {str(e)}")

def generate_thumbnail(
    input_path: str,
    output_path: str,
    time: float
) -> str:
    """Generate thumbnail at specific timestamp"""

    print(f"[Thumbnail] Generating at {time}s")

    cmd = [
        'ffmpeg',
        '-ss', str(time),
        '-i', input_path,
        '-vframes', '1',                   # Single frame
        '-q:v', '2',                       # JPEG quality (2 = high)
        '-vf', 'scale=-2:720',            # Scale to 720p height
        '-y',
        output_path
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

        if result.returncode != 0:
            raise Exception(f"Thumbnail generation failed: {result.stderr}")

        if not os.path.exists(output_path):
            raise Exception("Thumbnail file was not created")

        file_size = os.path.getsize(output_path)
        print(f"[Thumbnail] ✅ Generated: {file_size / 1024:.2f} KB")
        return output_path

    except Exception as e:
        raise Exception(f"Thumbnail error: {str(e)}")

def handler(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    RunPod handler function

    Expected input:
    {
        "input": {
            "operation": "extract_clip" | "generate_thumbnail",
            "video_url": "https://...",
            "start_time": 10.5,           # Required for extract_clip
            "duration": 30.0,              # Required for extract_clip
            "time": 15.0,                  # Required for generate_thumbnail
            "quality": "high",             # Optional: high, medium, low
            "output_format": "mp4"         # Optional, default: mp4
        }
    }
    """

    print(f"[Handler] Received job: {json.dumps(event.get('input', {}), indent=2)}")

    temp_dir = None

    try:
        job_input = event.get('input', {})

        # Validate required fields
        operation = job_input.get('operation', 'extract_clip')
        video_url = job_input.get('video_url')

        if not video_url:
            raise ValueError("video_url is required")

        # Create temp directory
        temp_dir = tempfile.mkdtemp()
        print(f"[Handler] Working directory: {temp_dir}")

        # Determine input format from URL
        url_path = video_url.split('?')[0]  # Remove query params
        input_ext = os.path.splitext(url_path)[1] or '.mp4'

        input_path = os.path.join(temp_dir, f'input{input_ext}')

        # Download video
        download_file(video_url, input_path)

        # Get video info
        video_duration = get_video_duration(input_path)

        if operation == 'generate_thumbnail':
            # Generate thumbnail
            time = float(job_input.get('time', 0))
            output_path = os.path.join(temp_dir, 'thumbnail.jpg')

            generate_thumbnail(input_path, output_path, time)

            # Read output file
            with open(output_path, 'rb') as f:
                output_data = f.read()

            # Encode as base64
            output_base64 = base64.b64encode(output_data).decode('utf-8')

            result = {
                "success": True,
                "operation": "generate_thumbnail",
                "thumbnail_data": output_base64,
                "file_size": len(output_data),
                "format": "jpg",
                "time": time
            }

        else:
            # Extract clip
            start_time = float(job_input.get('start_time', 0))
            duration = float(job_input.get('duration', 30))
            quality = job_input.get('quality', 'medium')
            output_format = job_input.get('output_format', 'mp4')

            # Validate times
            if start_time < 0:
                raise ValueError("start_time must be >= 0")
            if duration <= 0:
                raise ValueError("duration must be > 0")
            if video_duration > 0 and start_time + duration > video_duration:
                print(f"[Warning] Requested clip extends beyond video duration, adjusting...")
                duration = video_duration - start_time

            output_path = os.path.join(temp_dir, f'output.{output_format}')

            # Extract clip
            extract_clip(input_path, output_path, start_time, duration, quality)

            # Read output file
            with open(output_path, 'rb') as f:
                output_data = f.read()

            # Encode as base64
            output_base64 = base64.b64encode(output_data).decode('utf-8')

            result = {
                "success": True,
                "operation": "extract_clip",
                "clip_data": output_base64,
                "file_size": len(output_data),
                "format": output_format,
                "start_time": start_time,
                "duration": duration,
                "quality": quality,
                "video_duration": video_duration
            }

        print(f"[Handler] ✅ Job completed successfully")
        return result

    except Exception as e:
        error_msg = str(e)
        print(f"[Handler] ❌ Job failed: {error_msg}")
        return {
            "success": False,
            "error": error_msg,
            "operation": event.get('input', {}).get('operation', 'unknown')
        }

    finally:
        # Clean up temp directory
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                print(f"[Handler] Cleaned up temp directory")
            except Exception as e:
                print(f"[Handler] Warning: Failed to clean up temp directory: {e}")

# Start the RunPod serverless handler
if __name__ == "__main__":
    print("[RunPod] Starting ClipForge FFmpeg handler...")
    print("[RunPod] Ready to process video jobs")
    runpod.serverless.start({"handler": handler})
