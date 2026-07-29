import os
import re
import sys
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from helpers.cleaner import fix_ordinal_suffixes
from helpers.dictionaries.ap_months import AP_MONTHS
from helpers.dictionaries.ap_addresses import AP_STREET_ABBREVIATIONS
from helpers.dictionaries.title_case import TITLE_CASE

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "philadelphia")
FACILITIES_PATH = os.path.join(OUTPUT_DIR, "facilities.xlsx")
FACILITIES_CLEANED_PATH = os.path.join(OUTPUT_DIR, "facilities_cleaned.xlsx")
FULL_REPORTS_PATH = os.path.join(OUTPUT_DIR, "full_reports.xlsx")
FULL_REPORTS_CLEANED_PATH = os.path.join(OUTPUT_DIR, "full_reports_cleaned.xlsx")


def clean_facility_name(name):
    if not isinstance(name, str):
        return name

    x = name.title()
    x = fix_ordinal_suffixes(x)
    x = " ".join(
        word if i == 0 or word.lower() not in TITLE_CASE else word.lower()
        for i, word in enumerate(x.split())
    )
    x = re.sub(r"[`´'']", "'", x)
    x = re.sub(r"(\b\w+)'S\b", lambda m: f"{m.group(1)}'s", x)
    x = re.sub(r"\bLlc\b", "LLC", x)
    x = re.sub(r"\bDba\b", "DBA", x)
    return x


def clean_address(address_raw):

    if not isinstance(address_raw, str) or not address_raw.strip():
        return address_raw, "Philadelphia"

    x = address_raw
    x = re.sub(r"\s+Philadelphia\s*,", ", Philadelphia,", x, flags=re.IGNORECASE)

    x = x.title()
    x = re.sub(r"\b(N|S|E|W|NE|NW|SE|SW)\b", r"\1.", x)
    x = re.sub(r"\bPa\b", "PA", x)

    for full, abbr in AP_STREET_ABBREVIATIONS.items():
        x = re.sub(rf"\b{full}\b", abbr, x)

    x = re.sub(r"(?<=\d)(ST|ND|RD|TH)\b", lambda m: m.group(0).lower(), x, flags=re.IGNORECASE)
    x = re.sub(r"\.{2,}", ".", x)
    x = re.sub(r",\s*,+", ", ", x)
    x = re.sub(r"\s+", " ", x).strip()

    return x, "Philadelphia"


def clean_inspection_date(date_str):
    if not isinstance(date_str, str) or not date_str.strip():
        return date_str

    dt = pd.to_datetime(date_str, format="%m/%d/%Y", errors="coerce")
    if pd.isna(dt):
        return date_str

    formatted = dt.strftime("%B %-d, %Y")
    formatted = re.sub(
        r"(" + "|".join(AP_MONTHS.keys()) + r")",
        lambda m: AP_MONTHS[m.group()],
        formatted,
    )
    return formatted


def clean_facilities_file():
    if not os.path.exists(FACILITIES_PATH):
        print(f"⚠️  {FACILITIES_PATH} not found — skipping.")
        return

    df = pd.read_excel(FACILITIES_PATH, dtype=str)
    df["facility"] = df["facility"].apply(clean_facility_name)

    addr_city = df["address_raw"].apply(clean_address)
    df["address"] = addr_city.apply(lambda t: t[0])
    df.insert(df.columns.get_loc("address") + 1, "city", addr_city.apply(lambda t: t[1]))

    df.to_excel(FACILITIES_CLEANED_PATH, index=False)
    print(f"✅ Cleaned facilities saved → {FACILITIES_CLEANED_PATH} ({len(df)} rows)")


def clean_full_reports_file():
    if not os.path.exists(FULL_REPORTS_PATH):
        print(f"⚠️  {FULL_REPORTS_PATH} not found — skipping.")
        return

    df = pd.read_excel(FULL_REPORTS_PATH, dtype=str)
    df["facility"] = df["facility"].apply(clean_facility_name)
    df["inspection_date"] = df["inspection_date"].apply(clean_inspection_date)

    df.to_excel(FULL_REPORTS_CLEANED_PATH, index=False)
    print(f"✅ Cleaned full reports saved → {FULL_REPORTS_CLEANED_PATH} ({len(df)} rows)")


if __name__ == "__main__":
    clean_facilities_file()
    clean_full_reports_file()
