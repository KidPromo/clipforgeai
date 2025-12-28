# ClipForge FFmpeg Processing Service
# Optimized for RunPod Serverless GPU instances

FROM jrottenberg/ffmpeg:4.4-alpine

# Install Python and required packages
RUN apk add --no-cache \
    python3 \
    py3-pip \
    && pip3 install --no-cache-dir --break-system-packages \
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
