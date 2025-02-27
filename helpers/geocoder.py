import os
import io
import boto3
import pandas as pd
from geocodio import Client as GeocodioClient


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

        # Geocode missing addresses with Geocodio and overwrite file
        geocodio_api_key = os.getenv("GEOCODIO_API_KEY")
        if not geocodio_api_key:
            print("❌ Missing GEOCODIO_API_KEY in environment; cannot geocode.")
            return

        client = GeocodioClient(geocodio_api_key)

        # Add columns for lat/long to store results
        missing_addresses_df["Latitude"] = None
        missing_addresses_df["Longitude"] = None

        for idx, row in missing_addresses_df.iterrows():
            address_str = row["address"]
            try:
                response = client.geocode(address_str)
                result = response.json()  # Convert response to JSON
                if "results" in result and result["results"]:
                    location = result["results"][0]["location"]
                    missing_addresses_df.at[idx, "Latitude"] = location.get("lat")
                    missing_addresses_df.at[idx, "Longitude"] = location.get("lng")
            except Exception as e:
                print(f"❌ Error geocoding '{address_str}': {e}")

        # Overwrite missing_addresses.csv with new columns
        missing_addresses_df.to_csv(missing_file, index=False)
        print(f"✅ Missing addresses updated with coordinates in: {missing_file}")

        # Merge geocoded addresses back into merged_df
        merged_df = merged_df.merge(
            missing_addresses_df, on="address", how="left", suffixes=("", "_geocoded")
        )
        merged_df["Latitude"] = pd.to_numeric(
            merged_df["Latitude"].fillna(merged_df["Latitude_geocoded"]), errors="coerce"
        )
        merged_df["Longitude"] = pd.to_numeric(
            merged_df["Longitude"].fillna(merged_df["Longitude_geocoded"]), errors="coerce"
        )
        merged_df.drop(columns=["Latitude_geocoded", "Longitude_geocoded"], inplace=True)

        # Rename "address" to "Address" so it matches addresses_df
        new_addresses_df = missing_addresses_df.rename(columns={"address": "Address"}).copy()

        # Append these to existing addresses DataFrame
        addresses_df = pd.concat([addresses_df, new_addresses_df], ignore_index=True)

        # Remove duplicates in case some addresses were already present
        addresses_df.drop_duplicates(subset=["Address"], keep="last", inplace=True)

        # Re-upload updated addresses.csv to S3
        updated_csv_buf = io.StringIO()
        addresses_df.to_csv(updated_csv_buf, index=False)

        try:
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=addresses_s3_key,
                Body=updated_csv_buf.getvalue()
            )
            print("✅ Updated addresses.csv re-uploaded to S3.")
        except Exception as e:
            print(f"❌ Error uploading updated addresses.csv to S3: {e}")

    else:
        print("✅ No missing addresses found.")

    # Save the merged dataframe
    merged_file = "inspections.xlsx"
    try:
        merged_df.to_excel(merged_file, index=False)
        print(f"✅ Merged data with lat/long saved as: {merged_file}")
    except Exception as e:
        print(f"❌ Error saving merged Excel file: {e}")
