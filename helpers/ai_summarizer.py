import os
import hashlib
import io
import boto3
import pandas as pd
from anthropic import Anthropic

BATCH_SIZE = 25

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
Return ONLY a JSON object with two fields:
{
  "summary": "Your 1-2 sentence plain language summary here",
  "confidence": "High" or "Medium" or "Low"
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
        return pd.DataFrame(columns=["comment_hash", "comment_text", "ai_summary", "confidence_level", "created_at"])
    
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
        return pd.DataFrame(columns=["comment_hash", "comment_text", "ai_summary", "confidence_level", "created_at"])
    except Exception as e:
        print(f"Error loading summaries from S3: {e}")
        return pd.DataFrame(columns=["comment_hash", "comment_text", "ai_summary", "confidence_level", "created_at"])

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
            "confidence": result.get("confidence", "Low"),
            "input_tokens": message.usage.input_tokens,
            "output_tokens": message.usage.output_tokens
        }
    except Exception as e:
        print(f"Error summarizing comment: {e}")
        return {
            "summary": "",
            "confidence": "Low",
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
                zip(existing_summaries["ai_summary"], existing_summaries["confidence_level"])
            ))

        df["comment_hash"] = df["comment"].apply(hash_comment)
        needs_summary = df[
            (df["comment"].notna()) & 
            (df["comment"].str.strip() != "") &
            (~df["comment_hash"].isin(summaries_dict.keys()))
        ]
        
        date_col = next((c for c in df.columns if c in ("date", "inspection_date", "insp_date")), None)
        if date_col:
            needs_summary = needs_summary.copy()
            needs_summary["_sort_date"] = pd.to_datetime(needs_summary[date_col], errors="coerce")
            needs_summary = needs_summary.sort_values("_sort_date", ascending=False).drop(columns=["_sort_date"])
            needs_summary = needs_summary.sort_values(date_col, ascending=False)

        unique_comments = needs_summary[["comment_hash", "comment"]].drop_duplicates()

        total_new = len(unique_comments)
        if total_new > BATCH_SIZE:
            print(f"Found {total_new} new comments — processing {BATCH_SIZE} this run, {total_new - BATCH_SIZE} remaining for future runs")
            unique_comments = unique_comments.head(BATCH_SIZE)
        else:
            print(f"Found {total_new} new comments to summarize")
        
        if unique_comments.empty:
            print("No new comments to summarize")
            df["ai_summary"] = df["comment_hash"].map(lambda h: summaries_dict.get(h, ("", ""))[0])
            df["confidence_level"] = df["comment_hash"].map(lambda h: summaries_dict.get(h, ("", ""))[1])
            df.drop(columns=["comment_hash"], inplace=True)
            df.to_excel(local_inspections_file, index=False)
            return True
        
        total_input_tokens = 0
        total_output_tokens = 0
        new_summaries = []
        
        for idx, (_, row) in enumerate(unique_comments.iterrows(), start=1):
            comment = row["comment"]
            comment_hash = row["comment_hash"]

            print(f"Summarizing {idx}/{len(unique_comments)}: {comment[:50]}...")
            
            result = summarize_comment(comment, api_key)
            
            new_summaries.append({
                "comment_hash": comment_hash,
                "comment_text": comment,
                "ai_summary": result["summary"],
                "confidence_level": result["confidence"],
                "created_at": pd.Timestamp.now().isoformat()
            })
            
            summaries_dict[comment_hash] = (result["summary"], result["confidence"])
            
            total_input_tokens += result["input_tokens"]
            total_output_tokens += result["output_tokens"]
        
        if new_summaries:
            new_summaries_df = pd.DataFrame(new_summaries)
            combined_summaries = pd.concat([existing_summaries, new_summaries_df], ignore_index=True)
            save_summaries_to_s3(combined_summaries)
        
        df["ai_summary"] = df["comment_hash"].map(lambda h: summaries_dict.get(h, ("", ""))[0])
        df["confidence_level"] = df["comment_hash"].map(lambda h: summaries_dict.get(h, ("", ""))[1])
        
        df.drop(columns=["comment_hash"], inplace=True)
        
        df.to_excel(local_inspections_file, index=False)
        
        print(f"\nToken Usage:")
        print(f"  Input tokens: {total_input_tokens:,}")
        print(f"  Output tokens: {total_output_tokens:,}")
        print(f"  Total tokens: {total_input_tokens + total_output_tokens:,}")
        print(f"  Estimated cost: ${(total_input_tokens * 0.003 / 1000) + (total_output_tokens * 0.015 / 1000):.4f}")
        
        return True
        
    except Exception as e:
        print(f"Error adding AI summaries: {e}")
        import traceback
        traceback.print_exc()
        return False