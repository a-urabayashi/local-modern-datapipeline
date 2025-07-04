
import os
import boto3
from botocore.exceptions import ClientError

# Get configuration from environment variables
endpoint_url = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
region_name = os.getenv('MINIO_REGION', 'us-east-1')

print(f"Initializing MinIO client: {endpoint_url}")

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region_name,
)

# Required buckets for the pipeline
required_buckets = ['datalake']

# Required folders to create
folders_to_create = [
    ('datalake', 'raw/clinic_patients_data/'),
]

def create_bucket_if_not_exists(bucket_name: str):
    """Create bucket if it doesn't exist."""
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' already exists")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            try:
                s3.create_bucket(Bucket=bucket_name)
                print(f"✅ Created bucket '{bucket_name}'")
                return True
            except Exception as create_error:
                print(f"❌ Failed to create bucket '{bucket_name}': {create_error}")
                return False
        else:
            print(f"❌ Error checking bucket '{bucket_name}': {e}")
            return False
    except Exception as e:
        print(f"❌ Unexpected error with bucket '{bucket_name}': {e}")
        return False

def create_folder(bucket_name: str, folder_path: str):
    """Create folder by uploading empty object with trailing slash."""
    try:
        # Ensure folder path ends with slash
        key = folder_path if folder_path.endswith('/') else folder_path + '/'
        s3.put_object(Bucket=bucket_name, Key=key, Body=b'')
        print(f"📁 Created folder: s3a://{bucket_name}/{key}")
        return True
    except Exception as e:
        print(f"⚠️ Failed to create folder s3a://{bucket_name}/{folder_path}: {e}")
        return False

# Main setup process
print("🚀 Starting MinIO bucket and folder setup...")

all_success = True

# Create required buckets
for bucket_name in required_buckets:
    if not create_bucket_if_not_exists(bucket_name):
        all_success = False

# Create required folders
print("📂 Creating folder structure...")
for bucket_name, folder_path in folders_to_create:
    if not create_folder(bucket_name, folder_path):
        all_success = False

if all_success:
    print("\n🎉 MinIO setup completed successfully!")
    print("\n📋 Created structure:")
    print("  📦 datalake/")
    print("    📁 raw/wikipedia/")
    print("    📁 raw/fineweb/")
    print("  📦 processed/")
    print("    📁 wikipedia_delta/")
    print("    📁 fineweb_delta/")
else:
    print("❌ MinIO setup completed with errors")
    exit(1)
