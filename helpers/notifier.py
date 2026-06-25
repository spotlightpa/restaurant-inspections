import json
import os
import boto3
import requests
import pandas as pd
import re


def load_last_index(s3_client, bucket, prefix):
    """Download the last known inspection index from S3."""
    key = f"{prefix}last_inspections_index.json"
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        print("No previous index found, first run, skipping notifications.")
        return {}
    except Exception as e:
        print(f"⚠️ Could not load last index: {e}")
        return {}


def save_new_index(s3_client, bucket, prefix, index):
    """Upload the new inspection index to S3."""
    key = f"{prefix}last_inspections_index.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(index).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"✅ Saved new index with {len(index)} facilities.")


NOTIFICATION_DELAY_RUNS = 1  # Number of scraper runs to wait before sending. Set to 0 to send immediately.

def detect_and_notify(df, s3_client, bucket, prefix):
    """
    Compare current inspections against last known index.
    Call the Netlify notify function for any new inspections found.
    """
    # Build current index: facilityId -> latest inspection date
    current_index = {}
    new_inspections = []

    # Load previous index
    last_index = load_last_index(s3_client, bucket, prefix)

    # Group by facility ID, find latest inspection per facility
    for facility_id, group in df.groupby("id"):
        group_sorted = group.sort_values("inspection_date", ascending=False)
        latest_row = group_sorted.iloc[0]
        latest_date = str(latest_row.get("inspection_date", ""))
        current_index[facility_id] = latest_date

        # Check if this is newer than what we last saw
        prev_date = last_index.get(facility_id)
        if prev_date is None:
            # Brand new facility — skip notification, just index it
            continue
        if latest_date != prev_date:
            from datetime import datetime, timedelta
            try:
                parsed = datetime.strptime(latest_date, "%b. %d, %Y")
            except ValueError:
                try:
                    parsed = datetime.strptime(latest_date, "%B %d, %Y")
                except ValueError:
                    parsed = None
            if not parsed or parsed < datetime.now() - timedelta(days=8):
                current_index[facility_id] = latest_date
                continue
            print(f"🆕 New inspection: {facility_id} ({prev_date} → {latest_date})")

            # Build violations list from the latest row
            violations = []
            descs = str(latest_row.get("violation_description") or "").split(" | ")
            spotlights = str(latest_row.get("spotlight_pa") or "").split(" | ")
            comments = str(latest_row.get("comment") or "").split(" | ")
            ai_summaries = str(latest_row.get("ai_summary") or "").split(" | ")
            risk_levels = str(latest_row.get("risk_level") or "").split(" | ")

            for i, desc in enumerate(descs):
                if not desc.strip():
                    continue
                title = spotlights[i].strip() if i < len(spotlights) and spotlights[i].strip() not in ("", "NA") else desc.strip()
                comment = ai_summaries[i].strip() if i < len(ai_summaries) and ai_summaries[i].strip() else (comments[i].strip() if i < len(comments) else "")
                risk = risk_levels[i].strip() if i < len(risk_levels) else ""
                violations.append({
                    "title": title,
                    "comment": comment,
                    "risk": risk,
                })

            slug = re.sub(r'[^a-z0-9]+', '-', facility_id.lower()).strip('-')
            new_inspections.append({
                "facilityId": slug,
                "facilityName": str(latest_row.get("facility", "")),
                "inspectionDate": latest_date,
                "violations": violations,
            })

    # Save updated index
    save_new_index(s3_client, bucket, prefix, current_index)

    # Load pending notifications from previous run
    pending_key = f"{prefix}pending_notifications.json"
    pending = []
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=pending_key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        pending = data.get("inspections", [])
        runs_waited = data.get("runs_waited", 0) + 1
        print(f"📋 Found {len(pending)} pending notifications ({runs_waited}/{NOTIFICATION_DELAY_RUNS} runs waited)")
    except Exception:
        runs_waited = 0
        print("📋 No pending notifications found.")

    # Save current detections as pending for next run
    if new_inspections:
        print(f"💾 Saving {len(new_inspections)} new detections as pending (will send after {NOTIFICATION_DELAY_RUNS} run(s))")
        s3_client.put_object(
            Bucket=bucket,
            Key=pending_key,
            Body=json.dumps({"inspections": new_inspections, "runs_waited": 0}).encode("utf-8"),
            ContentType="application/json",
        )
    else:
        print("No new inspections detected this run.")

    # Send pending notifications if they've waited long enough
    if not pending or runs_waited < NOTIFICATION_DELAY_RUNS:
        if pending:
            print(f"⏳ Pending notifications not ready yet ({runs_waited}/{NOTIFICATION_DELAY_RUNS} runs waited). Holding.")
            s3_client.put_object(
                Bucket=bucket,
                Key=pending_key,
                Body=json.dumps({"inspections": pending, "runs_waited": runs_waited}).encode("utf-8"),
                ContentType="application/json",
            )
        return

    print(f"📬 Sending notifications for {len(pending)} pending inspection(s)...")

    # Clear pending after sending
    s3_client.delete_object(Bucket=bucket, Key=pending_key)

    notify_url = os.getenv("NOTIFY_FUNCTION_URL", "https://www.spotlightpa.org/.netlify/functions/notify")
    notify_secret = os.getenv("NOTIFY_SECRET")

    if not notify_url:
        print("⚠️ NOTIFY_FUNCTION_URL not set — skipping notification call.")
        return

    try:
        response = requests.post(
            notify_url,
            json={"inspections": pending},
            headers={
                "Content-Type": "application/json",
                "x-notify-secret": notify_secret or "",
            },
            timeout=60,
        )
        print(f"Notify response: {response.status_code} {response.text}")
    except Exception as e:
        print(f"⚠️ Notify call failed: {e}")