import re
import shutil
import os
import boto3
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError

from helpers.cleaner import clean_data
from helpers.uploader import upload_to_s3
from helpers.geocoder_helper import geocode
from helpers.categories_helper import upsert_categories, join_categories_into_inspections
from helpers.ai_labeler import label_categories_via_ai


def main():
    """Runs the Playwright scraper and processes the downloaded file."""
    headless = os.getenv("CI", "false").lower() == "true"  # Runs headless in GitHub Actions
    print(f"🔍 Running in {'headless' if headless else 'headed'} mode.")

    start_url = "http://cedatareporting.pa.gov/reports/powerbi/Public/AG/FS/PBI/Food_Safety_Inspections"

        # Sync data folder from S3 before anything else
        print("Syncing data folder from S3...")
        try:
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_REGION")
            )
            bucket = os.getenv("S3_BUCKET_NAME")
            prefix = "2025/restaurant-inspections/"
            paginator = s3_client.get_paginator("list_objects_v2")
            os.makedirs("data", exist_ok=True)
            for s3_page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in s3_page.get("Contents", []):
                    key = obj["Key"]
                    filename = key.replace(prefix, "")
                    if not filename or filename.endswith("/"):
                        continue
                    local_path = os.path.join("data", filename)
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    s3_client.download_file(bucket, key, local_path)
                    print(f"  Downloaded: {local_path}")
            print("S3 sync complete.")
        except Exception as e:
            print(f"⚠️ S3 sync failed, continuing with local data: {e}")

    # Download all 66 counties and merge
    print("Starting county-by-county download...")
    destination_path = "data/inspections.xlsx"

    COUNTIES = [
        "Adams", "Allegheny", "Armstrong", "Beaver", "Bedford", "Berks", "Blair",
        "Bradford", "Bucks", "Butler", "Cambria", "Cameron", "Carbon", "Centre",
        "Chester", "Clarion", "Clearfield", "Clinton", "Columbia", "Crawford",
        "Cumberland", "Dauphin", "Delaware", "Elk", "Erie", "Fayette", "Forest",
        "Franklin", "Fulton", "Greene", "Huntingdon", "Indiana", "Jefferson",
        "Juniata", "Lackawanna", "Lancaster", "Lawrence", "Lebanon", "Lehigh",
        "Luzerne", "Lycoming", "McKean", "Mercer", "Mifflin", "Monroe",
        "Montgomery", "Montour", "Northampton", "Northumberland", "Perry",
        "Philadelphia", "Pike", "Potter", "Schuylkill", "Snyder", "Somerset",
        "Sullivan", "Susquehanna", "Tioga", "Union", "Venango", "Warren",
        "Washington", "Wayne", "Westmoreland", "Wyoming", "York"
    ]

    import time
    run_start = time.time()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=500)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        failed_counties = []
        succeeded_counties = []

        for i, county in enumerate(COUNTIES, start=1):
            slug = county.lower()
            county_path = f"data/counties/{slug}.xlsx"
            tmp_path = f"data/counties/{slug}_tmp.xlsx"
            os.makedirs("data/counties", exist_ok=True)

            if os.path.exists(tmp_path):
                os.remove(tmp_path)

            county_start = time.time()
            print(f"[{i}/{len(COUNTIES)}] Downloading {county}...")

            try:
                page.goto(start_url)
                page.wait_for_timeout(20000)

                report_frame = page.frame(url=re.compile(r"cedatareporting\.pa\.gov/powerbi/\?id="))
                if not report_frame:
                    raise Exception("Could not find Power BI iframe")

                tab_locator = report_frame.locator("text=Violation Details")
                tab_locator.wait_for(state="visible", timeout=30000)
                tab_locator.click()
                report_frame.wait_for_timeout(10000)

                focus_div = report_frame.locator(".imageBackground").first
                focus_div.click(timeout=15000, force=True)
                page.wait_for_timeout(500)

                for _ in range(7):
                    page.keyboard.press("Tab")
                    page.wait_for_timeout(150)

                page.keyboard.press("Enter")
                page.wait_for_timeout(300)
                page.keyboard.press("Enter")
                page.wait_for_timeout(500)

                page.keyboard.type(slug, delay=100)
                page.wait_for_timeout(1000)
                page.keyboard.press("ArrowDown")
                page.wait_for_timeout(300)
                page.keyboard.press("Enter")
                report_frame.wait_for_timeout(3000)

                page.keyboard.press("Escape")
                report_frame.wait_for_timeout(2000)

                hover_xpath = (
                    "xpath=//*[@id='pvExplorationHost']/div/div/exploration/div/explore-canvas/"
                    "div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container[19]/"
                    "transform/div/div[2]/div/div"
                )
                button_xpath = (
                    "xpath=//*[@id='pvExplorationHost']/div/div/exploration/div/explore-canvas/"
                    "div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container[19]/"
                    "transform/div/visual-container-header/div/div/div/visual-container-options-menu/"
                    "visual-header-item-container/div"
                )
                hover_element = report_frame.locator(hover_xpath)
                button_locator = report_frame.locator(button_xpath)

                hover_element.wait_for(state="visible", timeout=30000)
                hover_element.hover()
                button_locator.click()
                page.wait_for_timeout(2000)

                page.keyboard.press("Enter")
                page.wait_for_timeout(500)

                for _ in range(4):
                    page.keyboard.press("Tab")
                    page.wait_for_timeout(200)

                with page.expect_download(timeout=120000) as dl_info:
                    page.keyboard.press("Enter")

                dl = dl_info.value
                shutil.copy(dl.path(), tmp_path)

                clean_data(tmp_path)

                df = pd.read_excel(tmp_path)
                row_count = len(df)

                if row_count == 0:
                    raise Exception("Downloaded file has 0 rows after cleaning")

                if row_count >= 149000:
                    print(f"⚠️ WARNING: {county} has {row_count} rows — may be hitting export cap")

                df["county"] = slug
                df.to_excel(tmp_path, index=False)
                shutil.move(tmp_path, county_path)
                print(f"✅ [{i}/{len(COUNTIES)}] {county} — {row_count} rows")
                succeeded_counties.append(county)

            except Exception as e:
                print(f"❌ [{i}/{len(COUNTIES)}] {county} FAILED: {e}")
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                failed_counties.append(county)
                continue

        browser.close()

    print(f"\nCounty download complete — {len(succeeded_counties)} succeeded, {len(failed_counties)} failed")
    if failed_counties:
        print(f"Failed counties: {', '.join(failed_counties)}")

    # Merge all county files
    print("Merging county files...")
    county_dfs = []
    for county in COUNTIES:
        slug = county.lower()
        county_path = f"data/counties/{slug}.xlsx"
        if os.path.exists(county_path):
            county_dfs.append(pd.read_excel(county_path, dtype=str))
        else:
            print(f"⚠️ Missing county file: {county_path}")

    if not county_dfs:
        print("❌ No county files to merge, aborting.")
        return

    fresh = pd.concat(county_dfs, ignore_index=True)
    fresh = fresh.drop_duplicates(subset=["facility", "address", "inspection_date"])
    print(f"Merged {len(fresh)} rows from {len(county_dfs)} counties")

    enrich_cols = ["Latitude", "Longitude", "spotlight_pa", "priority_level", "risk_level", "requirement_description", "ai_summary"]
    if os.path.exists(destination_path):
        existing = pd.read_excel(destination_path, dtype=str)
        existing_enrich = existing[["facility", "address", "inspection_date"] + [c for c in enrich_cols if c in existing.columns]].drop_duplicates(subset=["facility", "address", "inspection_date"])
        fresh = fresh.merge(existing_enrich, on=["facility", "address", "inspection_date"], how="left")
        print(f"Preserved enriched data for matching rows")

    # Drop isp, add id, sort
    if "isp" in fresh.columns:
        fresh.drop(columns=["isp"], inplace=True)

    fresh["id"] = fresh["facility"].fillna("") + " — " + fresh["address"].fillna("")

    ap_map = {
        r"Jan\.": "January", r"Feb\.": "February", r"Aug\.": "August",
        r"Sept\.": "September", r"Oct\.": "October", r"Nov\.": "November",
        r"Dec\.": "December"
    }
    normalized = fresh["inspection_date"].astype(str)
    for pattern, full in ap_map.items():
        normalized = normalized.str.replace(pattern, full, regex=True)
    fresh["_sort_date"] = pd.to_datetime(normalized, errors="coerce")
    fresh = fresh.sort_values("_sort_date", ascending=False).drop(columns=["_sort_date"])
    fresh = fresh.reset_index(drop=True)

    fresh.to_excel(destination_path, index=False)
    print(f"Saved merged inspections to {destination_path}")

    # Join violation details for rows missing risk_level
    from helpers.violations_helper import join_violation_details
    join_violation_details(destination_path)

    # Drop Latitude/Longitude so geocoder can merge them fresh from addresses.csv
    _df_pre_geo = pd.read_excel(destination_path, dtype=str)
    if "Latitude" in _df_pre_geo.columns:
        _df_pre_geo.drop(columns=["Latitude", "Longitude"], inplace=True)
        _df_pre_geo.to_excel(destination_path, index=False)
    geocode(destination_path)

    # Add AI summaries
    from helpers.ai_summarizer import add_ai_summaries
    add_ai_summaries(destination_path)

    # Re-read final file for notify and upload
    df_final = pd.read_excel(destination_path)

    # Detect new inspections and trigger notifications
    # from helpers.notifier import detect_and_notify
    # detect_and_notify(df_final, s3_client, bucket, prefix)

    # Upload to S3
    upload_to_s3(destination_path)

    # Run roundup violations scraper
    from helpers.roundup_violations import main as run_roundup_violations
    run_roundup_violations()


if __name__ == "__main__":
    main()
