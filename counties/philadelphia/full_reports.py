import os
import re
import time
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

try:
    from counties.philadelphia.scraper import http_get, init_session, OUTPUT_DIR, BASE_URL
except ImportError:
    from scraper import http_get, init_session, OUTPUT_DIR, BASE_URL

FACILITIES_PATH = os.path.join(OUTPUT_DIR, "facilities.xlsx")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "full_reports.xlsx")
FAILED_PATH = os.path.join(OUTPUT_DIR, "full_reports_failed.xlsx")
CHECKPOINT_EVERY = 100
PROGRESS_EVERY = 20

DATE_MARKER_RE = re.compile(r"Inspection Date:\s*(\d{2}/\d{2}/\d{4})?")

PURPOSE_RE = re.compile(r"Purpose of Inspection\s+([A-Za-z][A-Za-z /]*?)\s+Inspection Type")

VIOLATION_CITATION_RE = re.compile(
    r"(\d{1,3})\s+Violation of Code:\s*\[([^\]]+)\]\s*(.*?)"
    r"(?=\d{1,3}\s+Violation of Code:|Remarks|Summary|Person in Charge|$)",
    re.S,
)

EE_VIOLATION_RE = re.compile(
    r"(\d{3,4}\.\d{1,4}(?:\([a-z]\))?)\s+(.*?)"
    r"(?=\d{3,4}\.\d{1,4}(?:\([a-z]\))?\s|$)",
    re.S,
)


def get_report_links_for_facility(html):

    soup = BeautifulSoup(html, "html.parser")
    text = re.sub(r"\s+", " ", soup.get_text(separator=" ", strip=True)).strip()

    dates = [m.group(1) for m in DATE_MARKER_RE.finditer(text)]
    links = soup.find_all("a", string=re.compile("View Full Inspection Report", re.I))
    hrefs = [urljoin(BASE_URL + "/", a["href"].strip()) for a in links if a.get("href")]

    if len(dates) != len(hrefs):

        n = min(len(dates), len(hrefs))
        dates, hrefs = dates[:n], hrefs[:n]

    return [(d, h) for d, h in zip(dates, hrefs) if d]


def parse_full_report(html, report_url=""):

    soup = BeautifulSoup(html, "html.parser")
    text = re.sub(r"\s+", " ", soup.get_text(separator=" ", strip=True)).strip()

    purpose_match = PURPOSE_RE.search(text)
    inspection_reason = purpose_match.group(1).strip() if purpose_match else ""

    is_ee = "/EE/" in report_url
    report_type = "EE" if is_ee else "RetailFood"

    codes, citations, descs = [], [], []
    if is_ee:
        for code, desc in EE_VIOLATION_RE.findall(text):
            desc = desc.strip().replace("|", "/")
            if desc:
                codes.append(code)
                citations.append(code)
                descs.append(desc)
    else:
        for code, citation, desc in VIOLATION_CITATION_RE.findall(text):
            desc = desc.strip().replace("|", "/")
            codes.append(code)
            citations.append(citation.strip())
            descs.append(desc)

    return {
        "inspection_reason":      inspection_reason,
        "report_type":            report_type,
        "violation_code":         " | ".join(codes),
        "violation_citation":     " | ".join(citations),
        "violation_description":  " | ".join(descs),
    }


def scrape_all_full_reports(limit=None):
    if not os.path.exists(FACILITIES_PATH):
        raise FileNotFoundError(
            f"{FACILITIES_PATH} not found — run philadelphia_scraper.py first."
        )

    facilities = pd.read_excel(FACILITIES_PATH, dtype=str)
    if limit:
        facilities = facilities.head(limit)

    done_urls = set()
    all_records = []
    if os.path.exists(OUTPUT_PATH):
        existing = pd.read_excel(OUTPUT_PATH, dtype=str)
        done_urls = set(existing["detail_url"].unique())
        all_records = existing.to_dict("records")
        print(f"Resuming — {len(done_urls)} facilities already scraped in {OUTPUT_PATH}")

    init_session()

    todo = facilities[~facilities["detail_url"].isin(done_urls)]
    total_todo = len(todo)
    print(f"\n🚀 Scraping full inspection reports for {total_todo} facilities "
          f"({len(facilities) - total_todo} already done)")

    retry_counter = [0]
    failed = []
    n_reports = 0
    start_time = time.time()

    def on_retry(attempt, retries, error):
        retry_counter[0] += 1

    for i, row in enumerate(todo.itertuples(), 1):
        try:
            facility_r = http_get(row.detail_url, on_retry=on_retry)
            report_links = get_report_links_for_facility(facility_r.text)
        except Exception as e:
            failed.append({"facility": row.facility, "detail_url": row.detail_url,
                            "stage": "facility_page", "error": str(e)})
            continue

        for inspection_date, report_url in report_links:
            try:
                report_r = http_get(report_url, on_retry=on_retry)
                parsed = parse_full_report(report_r.text, report_url=report_url)
            except Exception as e:
                failed.append({"facility": row.facility, "detail_url": row.detail_url,
                                "stage": f"report ({inspection_date})", "error": str(e)})
                continue

            n_reports += 1
            all_records.append({
                "facility":              row.facility,
                "detail_url":            row.detail_url,
                "inspection_date":       inspection_date,
                "inspection_reason":     parsed["inspection_reason"],
                "report_type":           parsed["report_type"],
                "violation_code":        parsed["violation_code"],
                "violation_citation":    parsed["violation_citation"],
                "violation_description": parsed["violation_description"],
            })

        if i % PROGRESS_EVERY == 0 or i == total_todo:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (total_todo - i) / rate if rate > 0 else 0
            print(f"  [{i}/{total_todo}] elapsed {elapsed/60:.1f}m | "
                  f"~{rate*60:.1f} facilities/min | ETA {remaining/60:.0f}m | "
                  f"reports={n_reports} failed={len(failed)} retries={retry_counter[0]}")

        if i % CHECKPOINT_EVERY == 0:
            pd.DataFrame(all_records).to_excel(OUTPUT_PATH, index=False)
            if failed:
                pd.DataFrame(failed).to_excel(FAILED_PATH, index=False)
            print(f"  💾 Checkpoint saved ({len(all_records)} rows so far)")

    df = pd.DataFrame(all_records)
    df.to_excel(OUTPUT_PATH, index=False)
    if failed:
        pd.DataFrame(failed).to_excel(FAILED_PATH, index=False)

    elapsed_total = time.time() - start_time
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Facilities attempted this run:   {total_todo}")
    print(f"Individual reports scraped:      {n_reports}")
    print(f"Failed steps (facility or report): {len(failed)}")
    print(f"Total retry attempts:            {retry_counter[0]}")
    print(f"Elapsed time:                    {elapsed_total/60:.1f} minutes")
    print(f"Total rows in output (all runs): {len(df)}")
    if failed:
        print(f"\n⚠️  {len(failed)} steps failed — saved to {FAILED_PATH}")
        print("   Re-running this same command will retry failed facilities")
        print("   automatically (a facility only counts as done once all its")
        print("   reports succeed, since it isn't added to the done list otherwise).")
        for f in failed[:10]:
            print(f"     - {f['facility']} [{f['stage']}]: {f['error']}")
        if len(failed) > 10:
            print(f"     ... and {len(failed) - 10} more (see {FAILED_PATH})")
    print(f"\n💾 Saved → {OUTPUT_PATH}")
    return df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                         help="Only scrape the first N facilities (for testing)")
    args = parser.parse_args()
    scrape_all_full_reports(limit=args.limit)
