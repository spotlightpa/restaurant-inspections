import os
import io
import json
import string
from typing import List, Dict, Tuple

import boto3
import pandas as pd

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

from helpers.categories_helper import S3_KEY as CATS_S3_KEY

CATEGORIES = ["Pizza","Cafe","Bakery","Dessert","Pub","Deli","Fast Food","Restaurant","Mobile","Venue Dining","Other"]
CUISINE_CATEGORIES = ["Mexican","Chinese","Japanese","Thai","Indian","Mediterranean","Greek","Middle Eastern","Korean","Vietnamese","Italian","BBQ","Seafood","American","Caribbean","Latin American","Other"]

def normalize_strict(cat: str) -> str:
    return cat if cat in CATEGORIES else "Other"

def normalize_cuisine(cat: str) -> str:
    return cat if cat in CUISINE_CATEGORIES else "Other"

AI_PROMPT_HEADER = """
You classify Pennsylvania restaurant/food-establishment inspections.

Return EXACTLY:
- strict_category: ONE value from this list:
{allowed_categories}
- cuisine: ONE value from this list:
{allowed_cuisines}
- ai_category: a short free-text label (1–5 words) to help search (e.g., "neapolitan pizza", "boba tea cafe").
- confidence: 0–1
- rationale: one brief sentence citing evidence used.

Rules:
1) Use ONLY the establishment fields provided. Do not invent details.
2) Prefer specific strict_category over general; if unclear, use "Other".
3) If cuisine is unclear, use "Other".
4) Output JSON ONLY, one object per line (JSONL) with keys: "id","strict_category","cuisine","ai_category","confidence","rationale".
""".strip()

EVIDENCE_ORDER = [
    "program","facility_type","facility kind","license_type",
    "inspection_type","inspection_purpose","purpose",
    "owner","dba","chain",
    "violations","violation","violation_description","notes","remarks","comments"
]

_PUNCT_TABLE = str.maketrans({c: " " for c in string.punctuation})
def _simplify_name(s: str) -> str:
    if not isinstance(s, str): return "unknown"
    t = " ".join([w for w in s.strip().lower().translate(_PUNCT_TABLE).split() if w])
    return t or "unknown"

def _allowed_lists_text() -> Tuple[str, str]:
    allowed_cats = "\n".join(f"- {c}" for c in CATEGORIES)
    allowed_cuis = "\n".join(f"- {c}" for c in CUISINE_CATEGORIES)
    return allowed_cats, allowed_cuis

def _excerpt(text: str, max_words: int = 50) -> str:
    if not isinstance(text, str): return ""
    parts = text.strip().split()
    return text.strip() if len(parts) <= max_words else " ".join(parts[:max_words]) + " …"

def _gather_evidence(ins: pd.DataFrame, fac: str, addr: str, city: str) -> str:
    m = (ins["facility"].fillna("")==fac) & (ins["address"].fillna("")==addr) & (ins["city"].fillna("")==city)
    row = ins.loc[m].head(1)
    if row.empty: return ""
    r = row.iloc[0].to_dict()
    out = []
    for col in EVIDENCE_ORDER:
        if col in ins.columns:
            val = str(r.get(col, "") or "").strip()
            if not val: continue
            if col in {"violations","violation","violation_description","notes","remarks","comments"}:
                val = _excerpt(val)
            out.append(f"{col}: {val}")
    return "\n".join(out)

def _build_batch_prompt(items: List[Dict], evidence_map: Dict[int, str]) -> str:
    allowed_cats, allowed_cuis = _allowed_lists_text()
    lines = [
        AI_PROMPT_HEADER.format(allowed_categories=allowed_cats, allowed_cuisines=allowed_cuis),
        "Classify each establishment below. Output JSONL (one JSON object per line) with keys: id, strict_category, cuisine, ai_category, confidence, rationale.",
        "",
    ]
    for it in items:
        lines.append(
            f"id: {it['id']}\n"
            f"Facility: {it['facility']}\n"
            f"Address: {it['address']}\n"
            f"City: {it['city']}\n"
        )
        ev = evidence_map.get(it["id"], "")
        if ev:
            lines.append(f"EVIDENCE:\n{ev}\n")
    lines.append("\nReturn ONLY JSON lines, no markdown.")
    return "\n".join(lines)

def _parse_jsonl(s: str) -> List[Dict]:
    s = s.replace("```json", "```").replace("```", "")
    out = []
    for line in s.splitlines():
        line = line.strip()
        if not line or "{" not in line or "}" not in line: continue
        try:
            start = line.find("{"); end = line.rfind("}") + 1
            out.append(json.loads(line[start:end]))
        except Exception:
            continue
    return out

def _openai_client():
    if OpenAI is None:
        raise RuntimeError("OpenAI SDK not installed. Run: pip install 'openai>=1.0.0'")
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OPENAI_API_KEY not set in environment.")
    return OpenAI(api_key=key)

def _load_categories_df_from_s3_or_local() -> pd.DataFrame:
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
    AWS_REGION     = os.getenv("AWS_REGION")
    if AWS_ACCESS_KEY and AWS_SECRET_KEY and S3_BUCKET_NAME and AWS_REGION:
        try:
            s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=AWS_REGION)
            obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=CATS_S3_KEY)
            print("✅ Loaded categories.csv from S3 for labeling.")
            return pd.read_csv(io.BytesIO(obj["Body"].read()), dtype=str)
        except Exception as e:
            print(f"ℹ️ Could not load S3 categories.csv: {e}")
    if os.path.exists("categories.csv"):
        try:
            print("ℹ️ Loaded local categories.csv for labeling.")
            return pd.read_csv("categories.csv", dtype=str)
        except Exception as e:
            print(f"❌ Error reading local categories.csv: {e}")
    return pd.DataFrame(columns=["facility","address","city","ai_category"])

def _save_categories_df(df: pd.DataFrame):
    df.to_csv("categories.csv", index=False)
    print(f"📝 Updated local categories.csv ({len(df)} rows).")
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
    AWS_REGION     = os.getenv("AWS_REGION")
    if AWS_ACCESS_KEY and AWS_SECRET_KEY and S3_BUCKET_NAME and AWS_REGION:
        try:
            s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=AWS_REGION)
            s3.put_object(Bucket=S3_BUCKET_NAME, Key=CATS_S3_KEY, Body=df.to_csv(index=False))
            print(f"✅ Wrote labeled categories to s3://{S3_BUCKET_NAME}/{CATS_S3_KEY}")
        except Exception as e:
            print(f"❌ Error uploading labeled categories.csv to S3: {e}")

def label_categories_via_ai(local_inspections_file: str, limit: int | None = None, model: str = "gpt-4o-mini") -> int:
    try:
        ins = pd.read_excel(local_inspections_file, dtype=str)
        for col in ["facility","address","city"]:
            if col not in ins.columns:
                raise RuntimeError(f"'{col}' missing from {local_inspections_file}")
            ins[col] = ins[col].fillna("").astype(str).str.strip()
    except Exception as e:
        print(f"❌ Could not read {local_inspections_file}: {e}")
        return 0

    cats = _load_categories_df_from_s3_or_local()
    for c in ["facility","address","city","ai_category"]:
        if c not in cats.columns:
            cats[c] = ""
        cats[c] = cats[c].fillna("").astype(str).str.strip()

    mask = (cats["ai_category"].fillna("").astype(str).str.strip() == "")
    pending = cats.loc[mask, ["facility","address","city"]].drop_duplicates()
    if isinstance(limit, int):
        pending = pending.head(limit)
    if pending.empty:
        print("✅ No unlabeled rows found in categories.csv.")
        return 0

    try:
        client = _openai_client()
    except Exception as e:
        print(f"❌ OpenAI client error: {e}")
        return 0

    batch_size = int(os.getenv("AI_BATCH_SIZE", "50"))
    rows_list = pending.reset_index(drop=True).to_dict(orient="records")

    applied_total = 0
    for start in range(0, len(rows_list), batch_size):
        chunk = rows_list[start:start+batch_size]
        items: List[Dict] = []
        evidence_map: Dict[int, str] = {}
        for j, r in enumerate(chunk):
            rid = start + j
            fac, addr, city = r["facility"], r["address"], r["city"]
            items.append({"id": rid, "facility": fac, "address": addr, "city": city})
            ev = _gather_evidence(ins, fac, addr, city)
            if ev:
                evidence_map[rid] = ev

        prompt = _build_batch_prompt(items, evidence_map)
        try:
            resp = client.chat.completions.create(
                model=model,
                temperature=0,
                messages=[
                    {"role":"system","content":"You are a careful, terse classifier that replies in strict JSON lines."},
                    {"role":"user","content":prompt},
                ],
            )
            text = resp.choices[0].message.content or ""
        except Exception as e:
            print(f"❌ OpenAI API error on batch starting {start}: {e}")
            continue

        rows = _parse_jsonl(text)
        pred_map: Dict[int, str] = {}
        for r in rows:
            try:
                rid = int(r.get("id"))
            except Exception:
                continue
            ai_cat = (r.get("ai_category") or "").strip()
            if not ai_cat or ai_cat.lower() in {"unknown","other"}:
                it = next((i for i in items if i["id"] == rid), None)
                ai_cat = _simplify_name(it["facility"]) if it else "unknown"
            pred_map[rid] = ai_cat

        for it in items:
            rid = it["id"]
            predicted = pred_map.get(rid, _simplify_name(it["facility"]))
            m = (
                (cats["facility"] == it["facility"]) &
                (cats["address"] == it["address"]) &
                (cats["city"]    == it["city"]) &
                (cats["ai_category"].fillna("") == "")
            )
            if m.any():
                cats.loc[m, "ai_category"] = predicted
                applied = int(m.sum())
                applied_total += applied
                print(f"🤖 AI labeled: {it['facility']} | {it['address']} | {it['city']} → ai_category='{predicted}'")

    if applied_total:
        _save_categories_df(cats)
        print(f"🤖 Labeled {applied_total} rows via {model}.")
    else:
        print("⚠️ No labels applied.")

    return applied_total
