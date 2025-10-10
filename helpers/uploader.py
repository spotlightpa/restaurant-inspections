import boto3
import os
import gzip
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS credentials
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

def upload_to_s3(file_path):
    """
    Uploads a file to AWS S3, converting XLSX to gzipped CSV.
    Ensures proper UTF-8 encoding.
    """
    try:
        if not AWS_ACCESS_KEY or not AWS_SECRET_KEY or not S3_BUCKET_NAME or not AWS_REGION:
            raise ValueError("❌ Missing AWS credentials or S3 bucket name in environment variables.")

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )

        # Convert XLSX to CSV
        print("Converting XLSX to CSV...")
        df = pd.read_excel(file_path)
        
        # Generate file paths
        csv_file_path = file_path.replace('.xlsx', '.csv')
        csv_gz_file_path = csv_file_path + '.gz'
        
        # Export to CSV with UTF-8 encoding
        df.to_csv(
            csv_file_path, 
            index=False,
            encoding='utf-8',
            escapechar='\\',
            doublequote=True,
            lineterminator='\n'
        )
        
        print(f"CSV created: {csv_file_path}")
        
        # Compress CSV with gzip
        print("🗜️  Compressing CSV with gzip...")
        with open(csv_file_path, 'rb') as f_in:
            with gzip.open(csv_gz_file_path, 'wb', compresslevel=9) as f_out:
                f_out.writelines(f_in)
        
        # Get file sizes for comparison
        xlsx_size = os.path.getsize(file_path) / (1024 * 1024)
        csv_size = os.path.getsize(csv_file_path) / (1024 * 1024)
        csv_gz_size = os.path.getsize(csv_gz_file_path) / (1024 * 1024)
        
        print(f"File size comparison:")
        print(f"   XLSX:     {xlsx_size:.2f}MB")
        print(f"   CSV:      {csv_size:.2f}MB")
        print(f"   CSV.GZ:   {csv_gz_size:.2f}MB ⭐ (saved {xlsx_size - csv_gz_size:.2f}MB)")

        # Upload gzipped CSV to S3
        csv_gz_file_name = os.path.basename(csv_gz_file_path)
        s3_object_key = f"2025/restaurant-inspections/{csv_gz_file_name}"

        s3_client.upload_file(
            csv_gz_file_path, 
            S3_BUCKET_NAME, 
            s3_object_key,
            ExtraArgs={
                'ACL': 'public-read',
                'ContentType': 'text/csv',
                'ContentEncoding': 'gzip',
                'CacheControl': 'max-age=300'
            }
        )
        
        csv_gz_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_object_key}"
        print(f"Gzipped CSV uploaded to S3 with public read access.")
        print(f"CSV.GZ URL: {csv_gz_url}")

        # Also upload the original XLSX as backup
        xlsx_file_name = os.path.basename(file_path)
        xlsx_s3_key = f"2025/restaurant-inspections/{xlsx_file_name}"
        
        s3_client.upload_file(
            file_path, 
            S3_BUCKET_NAME, 
            xlsx_s3_key,
            ExtraArgs={
                'ACL': 'public-read',
                'ContentType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'CacheControl': 'max-age=300'
            }
        )
        
        xlsx_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{xlsx_s3_key}"
        print(f"XLSX uploaded to S3 as backup.")
        print(f"XLSX URL: {xlsx_url}")
        
        # Clean up temporary files
        if os.path.exists(csv_file_path):
            os.remove(csv_file_path)
            print(f"Cleaned up temporary CSV file")
        
        return csv_gz_url

    except boto3.exceptions.S3UploadFailedError as e:
        print(f"❌ S3 upload failed: {e}")
    except Exception as e:
        print(f"❌ Unexpected error during S3 upload: {e}")
        raise