import redis
import json
import os
from minio import Minio
import subprocess
import time

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
MINIO_HOST = os.getenv('MINIO_HOST', 'minio-proj.minio-ns.svc.cluster.local:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', '193i3rUfuAZpj2RAwfuO')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'zoewcjhzPIg9GlZBTUhUbF3aokeFCKCVQOHYtGb0')

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=False)
minio_client = Minio(MINIO_HOST, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

def log_message(message):
    try:
        redis_client.lpush('logs', f'[WORKER] {message}'.encode())
        print(f'[WORKER] {message}')
    except:
        print(f'[WORKER] {message}')

def process_song(song_hash, model='htdemucs'):
    try:
        log_message(f'Processing {song_hash}')
        
        # Download MP3 from queue bucket
        input_file = f'/tmp/{song_hash}.mp3'
        minio_client.fget_object('queue', f'{song_hash}.mp3', input_file)
        log_message(f'Downloaded MP3 from queue bucket')
        
        # Run Demucs separation
        output_dir = '/tmp/separated'
        os.makedirs(output_dir, exist_ok=True)
        
        cmd = ['python3', '-m', 'demucs', '-n', model, '-o', output_dir, input_file]
        log_message(f'Running Demucs with model: {model}')
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            log_message(f'Demucs completed successfully')
            
            # Find separated tracks
            song_basename = os.path.splitext(os.path.basename(input_file))[0]
            separated_dir = os.path.join(output_dir, model, song_basename)
            
            if os.path.exists(separated_dir):
                # Upload each track to output bucket
                for track in ['vocals', 'drums', 'bass', 'other']:
                    track_file = os.path.join(separated_dir, f'{track}.wav')
                    if os.path.exists(track_file):
                        output_name = f'{song_hash}-{track}.mp3'
                        minio_client.fput_object(
                            'output', 
                            output_name, 
                            track_file, 
                            content_type='audio/mpeg'
                        )
                        log_message(f'✓ Uploaded {track} track')
                
                # Cleanup
                subprocess.run(['rm', '-rf', separated_dir])
                os.remove(input_file)
                log_message(f'✓ Job completed successfully for {song_hash}')
                return True
            else:
                log_message(f'ERROR: Separated directory not found: {separated_dir}')
                return False
        else:
            log_message(f'ERROR: Demucs failed with code {result.returncode}')
            log_message(f'STDERR: {result.stderr}')
            return False
            
    except Exception as e:
        log_message(f'ERROR processing song: {str(e)}')
        return False

def main():
    log_message('Worker started, waiting for jobs...')
    
    while True:
        try:
            # Wait for work from Redis queue
            work_item = redis_client.brpop('toWorker', timeout=5)
            
            if work_item:
                _, work_data = work_item
                work = json.loads(work_data.decode('utf-8'))
                
                # Get song hash (REST API sends it as "hash")
                song_hash = work.get('hash') or work.get('songhash')
                model = work.get('model', 'htdemucs')
                
                if song_hash:
                    log_message(f'Received job for hash: {song_hash}')
                    process_song(song_hash, model)
                else:
                    log_message('ERROR: No hash in job data')
                    
        except KeyboardInterrupt:
            log_message('Worker shutting down...')
            break
        except Exception as e:
            log_message(f'Worker error: {str(e)}')
            time.sleep(1)

if __name__ == '__main__':
    main()