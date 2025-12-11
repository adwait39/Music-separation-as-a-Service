FROM python:3.9-slim

WORKDIR /app

# Install system dependencies for Demucs and audio processing
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    redis \
    minio \
    jsonpickle \
    demucs \
    torch \
    torchaudio \
    soundfile

# Copy worker script
COPY worker.py .

# Run the worker with unbuffered output
CMD ["python", "-u", "worker.py"]