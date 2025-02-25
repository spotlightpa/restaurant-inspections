import os
import io
import boto3
import pandas as pd


def geocode(local_inspections_file):

    # Load inspections.xlsx
    try:
        inspections_df = pd.read_excel(local_inspections_file, dtype=str)
        print(f"✅ Loaded local inspections file: {local_inspections_file}")
    except FileNotFoundError:
        print(f"❌ Could not find local file: {local_inspections_file}")
        return
    except Exception as e:
        print(f"❌ Error reading {local_inspections_file}: {e}")
        return

    # Download addresses.csv from S3
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
    AWS_REGION = os.getenv("AWS_REGION")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

    # Construct the S3 key for addresses.csv
    addresses_s3_key = "2025/restaurant-inspections/addresses.csv"

    try:
        s3_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=addresses_s3_key)
        addresses_df = pd.read_csv(io.BytesIO(s3_obj["Body"].read()), dtype=str)
        print("✅ 'addresses.csv' downloaded from S3 and loaded successfully.")
    except s3_client.exceptions.NoSuchKey:
        print(f"❌ 'addresses.csv' not found at s3://{S3_BUCKET_NAME}/{addresses_s3_key}")
        return
    except Exception as e:
        print(f"❌ Error retrieving 'addresses.csv' from S3: {e}")
        return

    # Make sure DataFrame columns match the expected names
    expected_cols = {"Address", "Latitude", "Longitude"}
    if not expected_cols.issubset(set(addresses_df.columns)):
        print(f"❌ 'addresses.csv' must contain at least these columns: {expected_cols}")
        return

    # Convert lat/long columns to numeric
    addresses_df["Latitude"] = pd.to_numeric(addresses_df["Latitude"], errors="coerce")
    addresses_df["Longitude"] = pd.to_numeric(addresses_df["Longitude"], errors="coerce")

    # Merge addresses with inspections
    merged_df = pd.merge(
        inspections_df,
        addresses_df,
        how="left",
        left_on="address",
        right_on="Address"
    )

    # Remove the redundant 'Address' column from addresses.csv
    if "Address" in merged_df.columns:
        merged_df.drop(columns=["Address"], inplace=True)

    # Reorder columns
    desired_order = [
        "isp", "inspection_date", "inspection_reason", "facility",
        "address", "city", "violation_code", "violation_description",
        "comment", "Latitude", "Longitude"
    ]
    remaining_cols = [c for c in merged_df.columns if c not in desired_order]
    final_cols = desired_order + remaining_cols

    # Filter out only columns that actually exist in merged_df
    final_cols = [c for c in final_cols if c in merged_df.columns]
    merged_df = merged_df[final_cols]

    # Identify rows with missing latitude and longitude
    missing_mask = merged_df["Latitude"].isna() & merged_df["Longitude"].isna()
    missing_addresses_df = merged_df[missing_mask]

    # Drop all columns except 'address', remove duplicates and save
    if not missing_addresses_df.empty:
        missing_addresses_df = missing_addresses_df[["address"]].drop_duplicates()
        missing_file = "missing_addresses.csv"
        missing_addresses_df.to_csv(missing_file, index=False)
        print(f"✅ Missing addresses saved to: {missing_file}")
    else:
        print("✅ No missing addresses found.")

    # Save the merged dataframe
    merged_file = "inspections.xlsx"
    try:
        merged_df.to_excel(merged_file, index=False)
        print(f"✅ Merged data with lat/long saved as: {merged_file}")
    except Exception as e:
        print(f"❌ Error saving merged Excel file: {e}")