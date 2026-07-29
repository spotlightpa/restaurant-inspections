import os
import sys
import time
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from helpers.ai_summarizer import hash_comment, summarize_comment

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "philadelphia")
FULL_REPORTS_CLEANED_PATH = os.path.join(OUTPUT_DIR, "full_reports_cleaned.xlsx")
SUMMARIES_CACHE_PATH = os.path.join(OUTPUT_DIR, "comment_summaries.csv")

BATCH_SIZE = 2000
PROGRESS_EVERY = 20
CHECKPOINT_EVERY = 20


def load_summaries_cache():
    if os.path.exists(SUMMARIES_CACHE_PATH):
        df = pd.read_csv(SUMMARIES_CACHE_PATH, dtype=str)
        print(f"Loaded {len(df)} existing summaries from local cache")
        return df
    return pd.DataFrame(columns=["comment_hash", "comment_text", "ai_summary", "created_at"])


def save_summaries_cache(df):
    df.to_csv(SUMMARIES_CACHE_PATH, index=False)


def add_ai_summaries(limit_facilities=None):
    if not os.path.exists(FULL_REPORTS_CLEANED_PATH):
        raise FileNotFoundError(f"{FULL_REPORTS_CLEANED_PATH} not found — run field_cleaner.py first.")

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("Missing ANTHROPIC_API_KEY, stopping.")
        return

    df = pd.read_excel(FULL_REPORTS_CLEANED_PATH, dtype=str)
    print(f"Loaded {len(df)} inspection rows")

    if "violation_description" not in df.columns:
        print("No violation_description column found.")
        return

    existing_summaries = load_summaries_cache()
    summaries_dict = {}
    for _, row in existing_summaries.iterrows():
        if pd.notna(row["ai_summary"]) and str(row["ai_summary"]).strip():
            summaries_dict[row["comment_hash"]] = row["ai_summary"]

    df["_orig_index"] = df.index
    df["_comment_parts"] = df["violation_description"].fillna("").apply(
        lambda c: [p.strip() for p in c.split(" | ")] if c.strip() else [""]
    )
    df_exploded = df.explode("_comment_parts").copy()
    df_exploded["_single_comment"] = df_exploded["_comment_parts"]
    df_exploded["_comment_hash"] = df_exploded["_single_comment"].apply(hash_comment)

    needs_summary = df_exploded[
        (df_exploded["_single_comment"].notna()) &
        (df_exploded["_single_comment"].str.strip() != "") &
        (~df_exploded["_comment_hash"].isin(summaries_dict.keys()))
    ].copy()

    needs_summary["_sort_date"] = pd.to_datetime(needs_summary["inspection_date"], errors="coerce")
    needs_summary = needs_summary.sort_values("_sort_date", ascending=False).drop(columns=["_sort_date"])

    facility_order = list(dict.fromkeys(needs_summary["detail_url"].tolist()))
    batch_limit = limit_facilities or BATCH_SIZE
    batch_facilities = facility_order[:batch_limit]
    batch_rows = needs_summary[needs_summary["detail_url"].isin(batch_facilities)]
    remaining_facilities = len(facility_order) - len(batch_facilities)
    print(f"Found {len(facility_order)} facilities with new comments — "
          f"processing {len(batch_facilities)} this run, {remaining_facilities} remaining for future runs")

    total_needing_summary = needs_summary[["_comment_hash", "_single_comment"]].drop_duplicates()
    total_needing_summary = total_needing_summary[total_needing_summary["_single_comment"].str.strip() != ""]

    unique_comments = batch_rows[["_comment_hash", "_single_comment"]].drop_duplicates()
    print(f"Summarizing {len(unique_comments)} unique comments this run "
          f"({len(total_needing_summary) - len(unique_comments)} queued for future runs)")

    total_input_tokens = 0
    total_output_tokens = 0
    new_summaries = []
    start_time = time.time()
    stopped_early = False
    idx = 0

    for idx, (_, row) in enumerate(unique_comments.iterrows(), start=1):
        comment = row["_single_comment"]
        comment_hash = row["_comment_hash"]
        if not comment.strip():
            continue

        result = summarize_comment(comment, api_key)

        if result.get("fatal"):
            print(f"\n⛔ Stopping: billing/auth error. {idx - 1} comments summarized before failure.")
            stopped_early = True
            break

        new_summaries.append({
            "comment_hash": comment_hash,
            "comment_text": comment,
            "ai_summary": result["summary"],
            "created_at": pd.Timestamp.now().isoformat(),
        })
        summaries_dict[comment_hash] = result["summary"]
        total_input_tokens += result["input_tokens"]
        total_output_tokens += result["output_tokens"]

        if idx % PROGRESS_EVERY == 0 or idx == len(unique_comments):
            elapsed = time.time() - start_time
            rate = idx / elapsed if elapsed > 0 else 0
            remaining_time = (len(unique_comments) - idx) / rate if rate > 0 else 0
            cost_so_far = (total_input_tokens * 0.003 / 1000) + (total_output_tokens * 0.015 / 1000)
            print(f"  [{idx}/{len(unique_comments)}] elapsed {elapsed/60:.1f}m | ETA {remaining_time/60:.0f}m | "
                  f"cost so far ${cost_so_far:.4f}")

        if idx % CHECKPOINT_EVERY == 0 and new_summaries:
            combined = pd.concat([existing_summaries, pd.DataFrame(new_summaries)], ignore_index=True)
            save_summaries_cache(combined)

    if new_summaries:
        combined_summaries = pd.concat([existing_summaries, pd.DataFrame(new_summaries)], ignore_index=True)
        save_summaries_cache(combined_summaries)

    df_exploded["_ai_summary"] = df_exploded["_comment_hash"].map(
        lambda h: summaries_dict.get(h, "") if h else ""
    )
    rejoined = df_exploded.groupby("_orig_index")["_ai_summary"].apply(
        lambda parts: " | ".join(parts.fillna(""))
    )
    df["ai_summary"] = df.index.map(rejoined)
    df = df.drop(columns=["_orig_index", "_comment_parts"], errors="ignore")

    df.to_excel(FULL_REPORTS_CLEANED_PATH, index=False)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Comments attempted this run: {idx if len(unique_comments) else 0}")
    print(f"Stopped early (billing/auth): {stopped_early}")
    if new_summaries:
        cost = (total_input_tokens * 0.003 / 1000) + (total_output_tokens * 0.015 / 1000)
        print(f"Input tokens:  {total_input_tokens:,}")
        print(f"Output tokens: {total_output_tokens:,}")
        print(f"Estimated cost: ${cost:.4f}")
    else:
        print("No new comments summarized.")
    print(f"\n💾 Saved → {FULL_REPORTS_CLEANED_PATH}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit-facilities", type=int, default=None,
                         help="Only process the first N facilities with new comments (for testing)")
    args = parser.parse_args()
    add_ai_summaries(limit_facilities=args.limit_facilities)
