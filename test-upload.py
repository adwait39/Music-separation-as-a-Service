import requests
import time

# Configuration
REST_API = "http://localhost:5000"
MP3_FILE = "data/short-dreams.mp3"

def test_music_separation():
    print("=" * 60)
    print("TESTING MUSIC SEPARATION SERVICE")
    print("=" * 60)
    
    # Step 1: Upload MP3 file
    print(f"\n1. Uploading MP3 file: {MP3_FILE}")
    
    with open(MP3_FILE, 'rb') as f:
        files = {'mp3': f}
        response = requests.post(f"{REST_API}/apiv1/separate", files=files)
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ Upload successful!")
        print(f"  Song hash: {result['hash']}")
        print(f"  Callback URL: {result['callback']}")
        
        song_hash = result['hash']
        
        # Step 2: Check status periodically
        print(f"\n2. Checking separation status...")
        max_attempts = 30
        attempt = 0
        
        while attempt < max_attempts:
            attempt += 1
            print(f"  Attempt {attempt}/{max_attempts}...", end=" ")
            
            response = requests.get(f"{REST_API}/apiv1/queue?hash={song_hash}")
            
            if response.status_code == 200:
                status_data = response.json()
                
                if status_data['status'] == 'complete':
                    print("✓ COMPLETE!")
                    print(f"\n3. Separated tracks available:")
                    for track_name, url in status_data['tracks'].items():
                        print(f"  - {track_name}: {url[:80]}...")
                    print(f"\n✓ SUCCESS! All tracks separated!")
                    return True
                else:
                    print("Processing...")
                    time.sleep(5)
            else:
                print(f"Error: {response.status_code}")
                break
        
        print(f"\n✗ Timeout: Separation took too long")
        return False
        
    else:
        print(f"✗ Upload failed: {response.status_code}")
        print(f"  Response: {response.text}")
        return False

if __name__ == "__main__":
    test_music_separation()