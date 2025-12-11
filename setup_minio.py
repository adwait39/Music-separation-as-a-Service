from minio import Minio
from minio.error import S3Error

# MinIO configuration with actual credentials
minio_client = Minio(
    "localhost:9000",
     access_key="193i3rUfuAZpj2RAwfuO",
     secret_key="zoewcjhzPIg9GlZBTUhUbF3aokeFCKCVQOHYtGb0",
     secure=False
)

# Buckets to create
buckets = ["demucs-bucket", "queue", "output"]

print("Setting up MinIO buckets...")

for bucket_name in buckets:
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"✓ Bucket '{bucket_name}' created successfully")
        else:
            print(f"✓ Bucket '{bucket_name}' already exists")
    except S3Error as err:
        print(f"✗ Error with bucket '{bucket_name}': {err}")

print("\nMinIO setup complete!")