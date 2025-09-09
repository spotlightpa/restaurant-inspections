import os
import io
import boto3
import pandas as pd

S3_KEY = "2025/restaurant-inspections/categories.csv"

def _composite_key(df: pd.DataFrame) -> pd.Series:
    """Internal stable key for dedupe/merge; not persisted."""
    f = df["facility"].fillna("").astype(str).str.strip()
    a = df["address"].fillna("").astype(str).str.strip()
    c = df["city"].fillna("").astype(str).str.strip()
    return f + "||" + a + "||" + c

def upsert_categories(local_inspections_file: str) -> str:
    """
    Create/merge a unique categories file with columns:
      facility,address,city,ai_category

    New rows start with blank ai_category. Existing values are preserved.
    Returns the local path written.
    """
    try:
        df = pd.read_excel(local_inspections_file, dtype=str)
    except Exception as e:
        print(f"❌ Could not read {local_inspections_file}: {e}")
        return ""

    required = {"facility", "address", "city"}
    missing = required - set(df.columns)
    if missing:
        print(f"❌ {local_inspections_file} missing columns: {missing}")
        return ""

    core = df[["facility", "address", "city"]].copy()
    for col in core.columns:
        core[col] = core[col].fillna("").astype(str).str.strip()

    uniques = core.drop_duplicates().reset_index(drop=True)
    if "ai_category" not in uniques.columns:
        uniques["ai_category"] = ""

    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
    AWS_REGION     = os.getenv("AWS_REGION")

    if not (AWS_ACCESS_KEY and AWS_SECRET_KEY and S3_BUCKET_NAME and AWS_REGION):
        print("❌ Missing AWS env vars; cannot read/write categories.csv in S3.")
        local_only_path = "categories.csv"
        uniques[["facility","address","city","ai_category"]].to_csv(local_only_path, index=False)
        print(f"📝 Wrote local (not S3-backed) {local_only_path} with {len(uniques)} rows.")
        return local_only_path

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )

    existing = pd.DataFrame(columns=["facility", "address", "city", "ai_category"])
    try:
        s3_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_KEY)
        existing = pd.read_csv(io.BytesIO(s3_obj["Body"].read()), dtype=str)
        for col in ["facility","address","city","ai_category"]:
            if col not in existing.columns:
                existing[col] = ""
            existing[col] = existing[col].fillna("").astype(str).str.strip()
        print(f"✅ Loaded existing categories.csv from S3 with {len(existing)} rows.")
    except s3_client.exceptions.NoSuchKey:
        print(f"ℹ️ No existing categories.csv at s3://{S3_BUCKET_NAME}/{S3_KEY}; will create it.")
    except Exception as e:
        print(f"❌ Error reading categories.csv from S3: {e}")

    if not existing.empty:
        existing["_key"] = _composite_key(existing)
    uniques["_key"] = _composite_key(uniques)

    if existing.empty:
        combined = uniques.copy()
    else:
        new_rows = uniques.loc[~uniques["_key"].isin(existing["_key"]), :]
        missing_cols = [c for c in ["facility","address","city","ai_category","_key"] if c not in new_rows.columns]
        for c in missing_cols:
            new_rows[c] = "" if c != "_key" else new_rows["_key"]
        combined = pd.concat([existing, new_rows[existing.columns]], ignore_index=True)

    if "_key" in combined.columns:
        combined.drop(columns=["_key"], inplace=True)

    combined = combined[["facility","address","city","ai_category"]].drop_duplicates().sort_values(
        ["facility","address","city"]
    ).reset_index(drop=True)

    local_path = "categories.csv"
    combined.to_csv(local_path, index=False)
    print(f"📝 Wrote local categories.csv with {len(combined)} unique rows.")

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_KEY,
            Body=combined.to_csv(index=False),
        )
        print(f"✅ Upserted categories.csv to s3://{S3_BUCKET_NAME}/{S3_KEY}")
    except Exception as e:
        print(f"❌ Error uploading categories.csv to S3: {e}")

    return local_path

def join_categories_into_inspections(local_inspections_file: str) -> bool:
    try:
        df = pd.read_excel(local_inspections_file, dtype=str)
    except Exception as e:
        print(f"❌ Could not read {local_inspections_file}: {e}")
        return False

    needed = ["facility", "address", "city"]
    for col in needed:
        if col not in df.columns:
            print(f"❌ Missing column '{col}' in {local_inspections_file}")
            return False
        df[col] = df[col].fillna("").astype(str).str.strip()

    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
    AWS_REGION     = os.getenv("AWS_REGION")

    categories = None
    if AWS_ACCESS_KEY and AWS_SECRET_KEY and S3_BUCKET_NAME and AWS_REGION:
        try:
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=AWS_ACCESS_KEY,
                aws_secret_access_key=AWS_SECRET_KEY,
                region_name=AWS_REGION,
            )
            s3_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_KEY)
            categories = pd.read_csv(io.BytesIO(s3_obj["Body"].read()), dtype=str)
            print("✅ Loaded categories.csv from S3 for exact-match join.")
        except s3_client.exceptions.NoSuchKey:
            print("ℹ️ categories.csv not found in S3; will look for local fallback.")
        except Exception as e:
            print(f"❌ Error reading categories.csv from S3: {e}")

    if categories is None and os.path.exists("categories.csv"):
        try:
            categories = pd.read_csv("categories.csv", dtype=str)
            print("ℹ️ Loaded local categories.csv for exact-match join.")
        except Exception as e:
            print(f"❌ Error reading local categories.csv: {e}")
            categories = None

    if categories is None:
        if "ai_category" not in df.columns:
            insert_at = df.columns.get_loc("city") + 1
            df.insert(insert_at, "ai_category", "")
        try:
            for legacy in ["category","cuisine","ai_confidence","ai_rationale"]:
                if legacy in df.columns:
                    df.drop(columns=[legacy], inplace=True)
            df.to_excel(local_inspections_file, index=False)
            print("📝 Wrote inspections with empty 'ai_category' column (no categories.csv available).")
            return True
        except Exception as e:
            print(f"❌ Error saving inspections with empty ai_category: {e}")
            return False

    for col in ["facility","address","city","ai_category"]:
        if col not in categories.columns:
            categories[col] = ""
        categories[col] = categories[col].fillna("").astype(str).str.strip()

    categories = categories[["facility","address","city","ai_category"]].drop_duplicates()

    keys = list(zip(df["facility"], df["address"], df["city"]))
    m = dict(zip(zip(categories["facility"], categories["address"], categories["city"]), categories["ai_category"]))
    ai_vals = [m.get(k, "") for k in keys]

    if "ai_category" in df.columns:
        df["ai_category"] = ai_vals
    else:
        insert_at = df.columns.get_loc("city") + 1
        df.insert(insert_at, "ai_category", ai_vals)

    for legacy in ["category","cuisine","ai_confidence","ai_rationale"]:
        if legacy in df.columns:
            df.drop(columns=[legacy], inplace=True)

    try:
        df.to_excel(local_inspections_file, index=False)
        print("✅ Wrote inspections.xlsx with only 'ai_category' injected.")
        return True
    except Exception as e:
        print(f"❌ Error saving inspections with ai_category: {e}")
        return False
