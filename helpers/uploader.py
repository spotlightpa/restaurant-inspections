import boto3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS credentials
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

def upload_to_s3(file_path):
    """Uploads a file to AWS S3 using environment variables for bucket name and credentials."""
    try:
        if not AWS_ACCESS_KEY or not AWS_SECRET_KEY or not S3_BUCKET_NAME or not AWS_REGION:
            raise ValueError("❌ Missing AWS credentials or S3 bucket name in environment variables.")

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )

        # Use the file name as the S3 key
        s3_file_name = os.path.basename(file_path)
        s3_object_key = f"2025/restaurant-inspections/{s3_file_name}"

        # Upload the file
        s3_client.upload_file(file_path, S3_BUCKET_NAME, s3_object_key)
        print(f"✅ File uploaded to S3 bucket.")

    except boto3.exceptions.S3UploadFailedError as e:
        print(f"❌ S3 upload failed: {e}")
    except Exception as e:
        print(f"❌ Unexpected error during S3 upload: {e}")
