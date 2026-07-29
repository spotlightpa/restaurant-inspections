from counties.philadelphia.scraper import scrape_to_dataframe
from counties.philadelphia.full_reports import scrape_all_full_reports
from counties.philadelphia.field_cleaner import clean_facilities_file, clean_full_reports_file

import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "philadelphia")
FACILITIES_PATH = os.path.join(OUTPUT_DIR, "facilities.xlsx")


def run(skip_scrape=False, skip_reports=False):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not skip_scrape:
        df = scrape_to_dataframe()
        if df.empty:
            print("⚠️ Philadelphia facility scrape returned no rows — stopping.")
            return
        df.to_excel(FACILITIES_PATH, index=False)
        print(f"✅ Saved {len(df)} facilities → {FACILITIES_PATH}")

    if not skip_reports:
        scrape_all_full_reports()

    clean_facilities_file()
    clean_full_reports_file()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-scrape", action="store_true")
    parser.add_argument("--skip-reports", action="store_true")
    args = parser.parse_args()
    run(skip_scrape=args.skip_scrape, skip_reports=args.skip_reports)
