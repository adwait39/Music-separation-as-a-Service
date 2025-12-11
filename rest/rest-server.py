from flask import Flask, request, jsonify, Response
import redis
import json
import hashlib
from minio import Minio
from minio.error import S3Error
import io
import time
from datetime import timedelta

app = Flask(__name__)

# Configuration
REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_DB = 0

MINIO_HOST = "minio-proj.minio-ns.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "193i3rUfuAZpj2RAwfuO"
MINIO_SECRET_KEY = "zoewcjhzPIg9GlZBTUhUbF3aokeFCKCVQOHYtGb0"
MINIO_QUEUE_BUCKET = "queue"
MINIO_OUTPUT_BUCKET = "output"

# Initialize Redis
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=False)

# Initialize MinIO
minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def log_message(message):
    """Send log message to Redis logs queue"""
    try:
        redis_client.lpush("logs", f"[REST] {message}")
        print(f"[LOG] {message}")
    except Exception as e:
        print(f"[LOG ERROR] {e}")

@app.route('/apiv1/separate', methods=['POST'])
def separate_track():
    """
    Handle MP3 upload and queue separation job
    
    Expected: multipart/form-data with 'mp3' file field
    Returns: JSON with song hash and callback URL
    """
    try:
        log_message("Received separation request")
        
        # Check if file is in request
        if 'mp3' not in request.files:
            log_message("ERROR: No MP3 file in request")
            return jsonify({"error": "No MP3 file provided"}), 400
        
        file = request.files['mp3']
        
        if file.filename == '':
            log_message("ERROR: Empty filename")
            return jsonify({"error": "Empty filename"}), 400
        
        # Read file data
        file_data = file.read()
        file_size = len(file_data)
        log_message(f"Processing file: {file.filename} ({file_size} bytes)")
        
        # Generate hash for the song
        song_hash = hashlib.sha256(file_data).hexdigest()
        log_message(f"Generated hash: {song_hash}")
        
        # Store MP3 in MinIO queue bucket
        try:
            minio_client.put_object(
                MINIO_QUEUE_BUCKET,
                f"{song_hash}.mp3",
                io.BytesIO(file_data),
                length=file_size,
                content_type='audio/mpeg'
            )
            log_message(f"✓ Stored MP3 in MinIO: {song_hash}.mp3")
        except S3Error as e:
            log_message(f"ERROR storing in MinIO: {e}")
            return jsonify({"error": f"Storage error: {str(e)}"}), 500
        
        # Create job message for worker
        job_data = {
            "hash": song_hash,
            "filename": file.filename,
            "bucket": MINIO_QUEUE_BUCKET,
            "object_name": f"{song_hash}.mp3",
            "timestamp": time.time()
        }
        
        # Add job to Redis queue
        try:
            redis_client.lpush("toWorker", json.dumps(job_data))
            log_message(f"✓ Added job to queue: {song_hash}")
        except Exception as e:
            log_message(f"ERROR adding to Redis queue: {e}")
            return jsonify({"error": f"Queue error: {str(e)}"}), 500
        
        # Return response
        response_data = {
            "hash": song_hash,
            "reason": "Song enqueued for separation",
            "callback": f"/apiv1/queue?hash={song_hash}"
        }
        
        log_message(f"✓ Successfully queued: {song_hash}")
        return jsonify(response_data), 200
        
    except Exception as e:
        log_message(f"ERROR in separate_track: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/apiv1/queue', methods=['GET'])
def check_queue():
    """
    Check status of a song separation job
    
    Query params: hash=<song_hash>
    Returns: JSON with status and track URLs if complete
    """
    try:
        song_hash = request.args.get('hash')
        
        if not song_hash:
            return jsonify({"error": "No hash provided"}), 400
        
        log_message(f"Checking status for: {song_hash}")
        
        # Check if all tracks exist in output bucket
        tracks = ['drums', 'bass', 'other', 'vocals']
        track_urls = {}
        all_complete = True
        
        for track in tracks:
            object_name = f"{song_hash}-{track}.mp3"
            try:
                # Check if object exists
                minio_client.stat_object(MINIO_OUTPUT_BUCKET, object_name)
                # Generate presigned URL (valid for 1 hour)
                url = minio_client.presigned_get_object(
                    MINIO_OUTPUT_BUCKET,
                    object_name,
                    expires=timedelta(seconds=3600)
                )
                track_urls[track] = url
            except S3Error:
                all_complete = False
                break
        
        if all_complete:
            log_message(f"✓ All tracks ready for: {song_hash}")
            return jsonify({
                "hash": song_hash,
                "status": "complete",
                "tracks": track_urls
            }), 200
        else:
            log_message(f"Processing in progress for: {song_hash}")
            return jsonify({
                "hash": song_hash,
                "status": "processing"
            }), 200
            
    except Exception as e:
        log_message(f"ERROR in check_queue: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/apiv1/track/<track_type>/<song_hash>', methods=['GET'])
def get_track(track_type, song_hash):
    """
    Download a specific separated track
    
    URL params: track_type (drums|bass|vocals|other), song_hash
    Returns: MP3 file stream
    """
    try:
        log_message(f"Download request: {song_hash}-{track_type}.mp3")
        
        object_name = f"{song_hash}-{track_type}.mp3"
        
        # Get object from MinIO
        response = minio_client.get_object(MINIO_OUTPUT_BUCKET, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        
        log_message(f"✓ Serving: {object_name} ({len(data)} bytes)")
        
        return Response(
            data,
            mimetype='audio/mpeg',
            headers={
                'Content-Disposition': f'attachment; filename={object_name}'
            }
        )
        
    except S3Error as e:
        log_message(f"ERROR: Track not found - {object_name}")
        return jsonify({"error": "Track not found"}), 404
    except Exception as e:
        log_message(f"ERROR in get_track: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    log_message("REST API starting up...")
    app.run(host='0.0.0.0', port=5000, debug=True)