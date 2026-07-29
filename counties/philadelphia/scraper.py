import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL   = "https://philadelphia-pa.healthinspections.us/philadelphia"
INDEX_URL  = f"{BASE_URL}/index.cfm"
SEARCH_URL = f"{BASE_URL}/search.cfm"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "philadelphia")
REQUEST_DELAY = 1.5
RESULTS_PER_PAGE = 20

PHILLY_ZIPS = [
    "19101", "19102", "19103", "19104", "19105", "19106", "19107", "19108",
    "19109", "19110", "19111", "19112", "19113", "19114", "19115", "19116",
    "19118", "19119", "19120", "19121", "19122", "19123", "19124", "19125",
    "19126", "19127", "19128", "19129", "19130", "19131", "19132", "19133",
    "19134", "19135", "19136", "19137", "19138", "19139", "19140", "19141",
    "19142", "19143", "19144", "19145", "19146", "19147", "19148", "19149",
    "19150", "19151", "19152", "19153", "19154",
]

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": INDEX_URL,
})

def http_get(url, params=None, retries=3, on_retry=None):
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=30, allow_redirects=True)
            r.raise_for_status()
            time.sleep(REQUEST_DELAY)
            return r
        except requests.RequestException as e:
            if on_retry:
                on_retry(attempt + 1, retries, e)
            else:
                print(f"  ⚠️  Attempt {attempt+1}/{retries} failed: {e}")
            time.sleep(REQUEST_DELAY * 3)
    raise RuntimeError(f"All {retries} attempts failed for {url}")

def init_session():
    print("🔄 Initializing session...")
    http_get(INDEX_URL)
    print(f"   Cookies: {dict(SESSION.cookies)}")

def build_params(zip_code="", start=1):
    return {
        "1":       "1",
        "kw1":     "",
        "kw2":     "",
        "rel1":    "F.organization_facility",
        "rel2":    "F.organization_facility",
        "zc":      zip_code,
        "facType": "Any",
        "subType": "Any",
        "dtRng":   "NO",
        "pre":     "Contains",
        "start":   start,
    }


def start_for_page(page_num):
    return (page_num - 1) * RESULTS_PER_PAGE + 1

def parse_results(html, zip_code, debug=False):
    soup = BeautifulSoup(html, "html.parser")

    if "btnSearch" in html and "Facilities matched" not in html:
        print(f"   ⚠️  Got index/form page instead of results")
        print(f"   [debug snippet]\n{html[800:2000]}")
        return [], 0, 1

    total = 0
    m = re.search(r"(\d+)\s+Facilit(?:y|ies)\s+matched", html, re.I)
    if m:
        total = int(m.group(1))

    max_page = 1
    page_links = soup.find_all("a", href=re.compile(r"[?&]start=(\d+)", re.I))
    if page_links:
        offsets = [int(re.search(r"[?&]start=(\d+)", a["href"]).group(1)) for a in page_links]
        max_page = max((o - 1) // RESULTS_PER_PAGE + 1 for o in offsets)
    elif total > RESULTS_PER_PAGE:
        max_page = (total + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE

    records = []
    facility_links = [
        a for a in soup.find_all("a", href=True)
        if "facilityID=" in a["href"] and "inspectionID=" not in a["href"]
    ]

    for link in facility_links:
        name = link.get_text(strip=True)
        if not name or len(name) < 2:
            continue

        detail_url = urljoin(BASE_URL + "/", link["href"].strip())

        anchor = link.parent if (link.parent and link.parent.name in ("b", "strong")) else link
        text_nodes = []
        for sib in anchor.next_siblings:
            if hasattr(sib, "name") and sib.name in ("b", "strong"):
                break
            t = sib.get_text(separator=" ", strip=True) if hasattr(sib, "get_text") else str(sib).strip()
            if t:
                text_nodes.append(t)

        combined = re.sub(r"\s+", " ", " ".join(text_nodes)).strip()
        dm = re.search(r"Last Inspection Date:\s*(\d{2}/\d{2}/\d{4})", combined)
        if dm:
            date_str = dm.group(1)
            address_raw = combined[:dm.start()].strip()
        else:
            date_str = ""
            address_raw = re.sub(r"Last Inspection Date:.*", "", combined).strip()

        records.append({
            "facility":             name,
            "address_raw":          address_raw,
            "last_inspection_date": date_str,
            "zip_searched":         zip_code,
            "detail_url":           detail_url,
        })

    print(f"   Parsed {len(records)} facilities (reported total: {total}, pages: {max_page})")

    if debug or (total > 0 and len(records) == 0):
        print(f"   [debug: total={total} but 0 parsed — dumping 2000 chars of body]")
        body = soup.find("body")
        print((body.get_text()[:2000] if body else html[:2000]))

    return records, total, max_page

def scrape_to_dataframe(zips=None, debug_zip=None) -> pd.DataFrame:
    zips = [debug_zip] if debug_zip else (zips or PHILLY_ZIPS)
    print(f"\n🚀 Scraping Philadelphia — {len(zips)} ZIP code(s)")

    init_session()

    all_records = []
    seen_urls = set()

    for i, zip_code in enumerate(zips, 1):
        print(f"\n📮 ZIP {zip_code} ({i}/{len(zips)})")

        params = build_params(zip_code, start=1)
        r = http_get(SEARCH_URL, params=params)
        print(f"   Final URL: {r.url}")

        if debug_zip:
            print(f"\n--- RAW HTML (first 3000 chars) ---\n{r.text[:3000]}\n---\n")

        records, total, max_page = parse_results(r.text, zip_code, debug=bool(debug_zip))
        for rec in records:
            if rec["detail_url"] not in seen_urls:
                seen_urls.add(rec["detail_url"])
                all_records.append(rec)

        for page_num in range(2, max_page + 1):
            print(f"   📄 Page {page_num}/{max_page} (start={start_for_page(page_num)})...")
            params = build_params(zip_code, start=start_for_page(page_num))
            r = http_get(SEARCH_URL, params=params)
            records, _, _ = parse_results(r.text, zip_code)
            for rec in records:
                if rec["detail_url"] not in seen_urls:
                    seen_urls.add(rec["detail_url"])
                    all_records.append(rec)

    print(f"\n✅ Total unique facilities: {len(all_records)}")
    return pd.DataFrame(all_records)

def scrape(debug_zip=None):

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = scrape_to_dataframe(debug_zip=debug_zip)

    if df.empty:
        print("⚠️  No data — check [debug] output above.")
        return

    print(f"\n📐 Columns: {list(df.columns)}")
    print(df.head(5).to_string())

    out_path = os.path.join(OUTPUT_DIR, "facilities.xlsx")
    df.to_excel(out_path, index=False)
    print(f"\n💾 Saved → {out_path}")


if __name__ == "__main__":
    import sys
    debug = sys.argv[1] if len(sys.argv) > 1 else None
    scrape(debug_zip=debug)
