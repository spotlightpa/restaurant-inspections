import os
import hashlib
import io
import boto3
import pandas as pd
from anthropic import Anthropic

BATCH_SIZE = 100

AP_MAP = {
    r"Jan\.": "January", r"Feb\.": "February", r"Aug\.": "August",
    r"Sept\.": "September", r"Oct\.": "October", r"Nov\.": "November",
    r"Dec\.": "December"
}

SYSTEM_PROMPT = """
Context: You are a food safety expert reviewing restaurant inspection reports from Pennsylvania.

Goal: Translate each inspector comment into a plain-language summary that is clear, accurate, and easy for the general public to understand.

Instructions:
1. Understand Inspector Comments - Carefully read each comment and ensure you fully understand the inspector's observation or directive.

2. Translate Each Comment into Plain-Language:
- Translate technical language into plain English
- Use simple sentence structure and everyday vocabulary
- Retain the original meaning as precisely as possible
- If already simple and understandable, you may copy it as-is

3. Rules to Follow:
- Always refer to the facility as a "facility" not a restaurant
- Always mention if the violation was corrected on site (look for "COS")
- Always mention if it notes a "repeat violation"
- Always mention if there's a deadline to fix or respond
- Do not add adjectives or modifiers that didn't exist in the original

4. Output Format:
Return ONLY a JSON object with one field:
{
  "summary": "Your 1-2 sentence plain language summary here"
}

Do not include any other text, explanations, or markdown formatting."""

def hash_comment(comment: str) -> str:
    if pd.isna(comment) or not str(comment).strip():
        return ""
    return hashlib.md5(str(comment).strip().encode()).hexdigest()

def load_summaries_from_s3() -> pd.DataFrame:
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
    AWS_REGION = os.getenv("AWS_REGION")
    if not all([AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET_NAME, AWS_REGION]):
        print("Missing AWS credentials for summaries")
        return pd.DataFrame(columns=["comment_hash", "comment_text", "ai_summary", "created_at"])
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    summaries_key = "2025/restaurant-inspections/comment_summaries.csv"
    try:
        s3_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=summaries_key)
        summaries = pd.read_csv(io.BytesIO(s3_obj["Body"].read()), dtype=str)
        print(f"Loaded {len(summaries)} existing summaries from S3")
        return summaries
    except s3_client.exceptions.NoSuchKey:
        print("No existing summaries found in S3, starting fresh")
        return pd.DataFrame(columns=["comment_hash", "comment_text", "ai_summary", "created_at"])
    except Exception as e:
        print(f"Error loading summaries from S3: {e}")
        return pd.DataFrame(columns=["comment_hash", "comment_text", "ai_summary", "created_at"])

def save_summaries_to_s3(summaries_df: pd.DataFrame) -> bool:
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
    AWS_REGION = os.getenv("AWS_REGION")
    if not all([AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET_NAME, AWS_REGION]):
        print("Missing AWS credentials, cannot save summaries")
        return False
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    summaries_key = "2025/restaurant-inspections/comment_summaries.csv"
    try:
        csv_buffer = io.StringIO()
        summaries_df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=summaries_key,
            Body=csv_buffer.getvalue()
        )
        print(f"Saved {len(summaries_df)} summaries to S3")
        return True
    except Exception as e:
        print(f"Error saving summaries to S3: {e}")
        return False

def summarize_comment(comment: str, api_key: str) -> dict:
    client = Anthropic(api_key=api_key)
    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            system=SYSTEM_PROMPT,
            messages=[
                {"role": "user", "content": f"Comment: {comment}"}
            ]
        )
        import json
        response_text = message.content[0].text.strip()
        result = json.loads(response_text)
        return {
            "summary": result.get("summary", ""),
            "input_tokens": message.usage.input_tokens,
            "output_tokens": message.usage.output_tokens
        }
    except Exception as e:
        print(f"Error summarizing comment: {e}")
        return {
            "summary": "",
            "input_tokens": 0,
            "output_tokens": 0
        }

def add_ai_summaries(local_inspections_file: str) -> bool:
    try:
        df = pd.read_excel(local_inspections_file, dtype=str)
        print(f"Loaded {len(df)} inspection rows")
        if "comment" not in df.columns:
            print("No comment column found")
            return True
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            print("Missing ANTHROPIC_API_KEY, skipping AI summaries")
            return False
        existing_summaries = load_summaries_from_s3()
        summaries_dict = {}
        if not existing_summaries.empty:
            summaries_dict = dict(zip(
                existing_summaries["comment_hash"],
                existing_summaries["ai_summary"]
            ))

        # Expand pipe-delimited comments into individual rows for per-violation summarization
        df["_orig_index"] = df.index
        df["_comment_parts"] = df["comment"].fillna("").apply(
            lambda c: [p.strip() for p in c.split(" | ")] if c.strip() else [""]
        )
        df_exploded = df.explode("_comment_parts").copy()
        df_exploded["_single_comment"] = df_exploded["_comment_parts"]
        df_exploded["_comment_hash"] = df_exploded["_single_comment"].apply(hash_comment)

        # Find which individual comments still need summarizing
        needs_summary = df_exploded[
            (df_exploded["_single_comment"].notna()) &
            (df_exploded["_single_comment"].str.strip() != "") &
            (~df_exploded["_comment_hash"].isin(summaries_dict.keys()))
        ].copy()

        # Sort by date descending so newest get summarized first
        date_col = next((c for c in df.columns if c in ("date", "inspection_date", "insp_date")), None)
        if date_col and date_col in needs_summary.columns:
            normalized_dates = needs_summary[date_col].astype(str)
            for pattern, full in AP_MAP.items():
                normalized_dates = normalized_dates.str.replace(pattern, full, regex=True)
            needs_summary["_sort_date"] = pd.to_datetime(normalized_dates, errors="coerce")
            needs_summary = needs_summary.sort_values("_sort_date", ascending=False).drop(columns=["_sort_date"])

        # Group by facility, take first BATCH_SIZE facilities
        facility_col = "id" if "id" in needs_summary.columns else None
        if facility_col:
            facility_order = list(dict.fromkeys(needs_summary[facility_col].tolist()))
            batch_facilities = facility_order[:BATCH_SIZE]
            batch_rows = needs_summary[needs_summary[facility_col].isin(batch_facilities)]
            remaining = len(facility_order) - len(batch_facilities)
            print(f"Found {len(facility_order)} facilities with new comments — processing {len(batch_facilities)} this run, {remaining} remaining for future runs")
        else:
            batch_rows = needs_summary.head(BATCH_SIZE)
            print(f"No facility id column found, processing first {BATCH_SIZE} comments")

        unique_comments = batch_rows[["_comment_hash", "_single_comment"]].drop_duplicates()
        print(f"  ({len(unique_comments)} unique comments across those facilities)")
        total_input_tokens = 0
        total_output_tokens = 0
        new_summaries = []
        for idx, (_, row) in enumerate(unique_comments.iterrows(), start=1):
            comment = row["_single_comment"]
            comment_hash = row["_comment_hash"]
            if not comment.strip():
                continue

            print(f"Summarizing {idx}/{len(unique_comments)}: {comment[:50]}...")
            result = summarize_comment(comment, api_key)
            new_summaries.append({
                "comment_hash": comment_hash,
                "comment_text": comment,
                "ai_summary": result["summary"],
                "created_at": pd.Timestamp.now().isoformat()
            })
            summaries_dict[comment_hash] = result["summary"]
            total_input_tokens += result["input_tokens"]
            total_output_tokens += result["output_tokens"]
        if new_summaries:
            new_summaries_df = pd.DataFrame(new_summaries)
            combined_summaries = pd.concat([existing_summaries, new_summaries_df], ignore_index=True)
            save_summaries_to_s3(combined_summaries)
        # Map summaries back to exploded rows and rejoin with pipe delimiter
        df_exploded["_ai_summary"] = df_exploded["_comment_hash"].map(
            lambda h: summaries_dict.get(h, "") if h else ""
        )

        rejoined = df_exploded.groupby("_orig_index")["_ai_summary"].apply(
            lambda parts: " | ".join(parts.fillna(""))
        )
        df["ai_summary"] = df.index.map(rejoined)
        df = df.drop(columns=["_orig_index", "_comment_parts"], errors="ignore")

        df.to_excel(local_inspections_file, index=False)
        if new_summaries:
            print(f"\nToken Usage:")
            print(f"  Input tokens: {total_input_tokens:,}")
            print(f"  Output tokens: {total_output_tokens:,}")
            print(f"  Total tokens: {total_input_tokens + total_output_tokens:,}")
            print(f"  Estimated cost: ${(total_input_tokens * 0.003 / 1000) + (total_output_tokens * 0.015 / 1000):.4f}")
        else:
            print("No new comments to summarize")

        return True
    except Exception as e:
        print(f"Error adding AI summaries: {e}")
        import traceback
        traceback.print_exc()
        return False