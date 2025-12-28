# ClipForge FFmpeg Processing Service
# Optimized for RunPod Serverless GPU instances

FROM python:3.11-slim

# Install FFmpeg and required system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir \
    runpod==1.7.6 \
    requests==2.32.3

# Copy handler script
COPY handler.py /app/handler.py

# Set working directory
WORKDIR /app

# Expose port for health checks
EXPOSE 8080

# Run the handler
CMD ["python3", "handler.py"]
