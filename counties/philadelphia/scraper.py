import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import date, datetime, timedelta

BASE_URL   = "https://philadelphia-pa.healthinspections.us/philadelphia"
INDEX_URL  = f"{BASE_URL}/index.cfm"
SEARCH_URL = f"{BASE_URL}/search.cfm"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "philadelphia")
REQUEST_DELAY = 1.5
RESULTS_PER_PAGE = 20
RESULTS_CAP = 100


PHILLY_ZIPS = """
00000 01042 07083 07101 07109 07946
08003 08009 08012 08016 08021 08028
08031 08037 08046 08052 08053 08054
08060 08070 08081 08084 08085 08086
08091 08093 08094 08103 08105 08107
08252 08302 08520 08525 08610 08619
08628 08817 10984 11730 17502 17516
17543 17579 17602 18015 18017 18049
18103 18235 18704 18901 18938 18966
18969 19001 19002 19007 19013 19018
19020 19022 19023 19027 19031 19034
19038 19040 19046 19050 19053 19054
19060 19063 19072 19079 19081 19082
19083 19086 19092 19094 19096 19100
19101 19102 19103 19104 19105 19106
19107 19108 19109 19110 19111 19112
19113 19114 19115 19116 19118 19119
19120 19121 19122 19123 19124 19125
19126 19127 19128 19129 19130 19131
19132 19133 19134 19135 19136 19137
19138 19139 19140 19141 19142 19143
19144 19145 19146 19147 19148 19149
19150 19151 19152 19153 19154 19176
19317 19320 19331 19335 19342 19343
19348 19365 19401 19406 19422 19428
19438 19444 19446 19462 19464 19468
19707 19709 19711 19720 19803 19808
19809 19810 19902 21078 21224 22079
23230 60546 80305 90245 98011 99999
""".split()

HISTORY_YEARS = 3

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": INDEX_URL,
})


MAX_BACKOFF = 300


def http_get(url, params=None, on_retry=None, max_client_error_retries=3):
    attempt = 0
    client_error_attempts = 0
    while True:
        attempt += 1
        try:
            r = SESSION.get(url, params=params, timeout=30, allow_redirects=True)
            r.raise_for_status()
            time.sleep(REQUEST_DELAY)
            return r
        except requests.RequestException as e:
            is_client_error = (
                isinstance(e, requests.HTTPError)
                and e.response is not None
                and 400 <= e.response.status_code < 500
            )
            if is_client_error:
                client_error_attempts += 1
                if client_error_attempts >= max_client_error_retries:
                    raise RuntimeError(f"Giving up after {client_error_attempts} client errors for {url}: {e}")

            if on_retry:
                on_retry(attempt, None, e)
            else:
                print(f"  ⚠️  Attempt {attempt} failed: {e}")

            backoff = min(REQUEST_DELAY * 3 * (2 ** min(attempt - 1, 8)), MAX_BACKOFF)
            time.sleep(backoff)


def init_session():
    print("🔄 Initializing session...")
    http_get(INDEX_URL)
    print(f"   Cookies: {dict(SESSION.cookies)}")


def build_params(zip_code="", start=1, date_range=None):
    params = {
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
    if date_range:
        sd, ed = date_range
        params["dtRng"] = "YES"
        params["sd"] = sd
        params["ed"] = ed
    return params


def start_for_page(page_num):
    return (page_num - 1) * RESULTS_PER_PAGE + 1


def get_total(html):
    m = re.search(r"(\d+)\s+Facilit(?:y|ies)\s+matched", html, re.I)
    return int(m.group(1)) if m else 0


def weekly_windows(months_back=None):
    today = date.today()
    start = today - timedelta(days=30 * months_back) if months_back else today - timedelta(days=365 * HISTORY_YEARS)

    windows = []
    cur = start
    while cur <= today:
        end = min(cur + timedelta(days=6), today)
        windows.append((cur.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y")))
        cur = end + timedelta(days=1)
    return windows


def daily_windows(sd_str, ed_str):
    sd = datetime.strptime(sd_str, "%m/%d/%Y").date()
    ed = datetime.strptime(ed_str, "%m/%d/%Y").date()
    days = []
    cur = sd
    while cur <= ed:
        days.append((cur.strftime("%m/%d/%Y"), cur.strftime("%m/%d/%Y")))
        cur += timedelta(days=1)
    return days


def parse_results(html, zip_code, debug=False):
    soup = BeautifulSoup(html, "html.parser")

    if "btnSearch" in html and "Facilities matched" not in html:
        print(f"   ⚠️  Got index/form page instead of results")
        print(f"   [debug snippet]\n{html[800:2000]}")
        return [], 0, 1

    total = get_total(html)

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


def scrape_query(zip_code, all_records, seen_urls, date_range=None):
    params = build_params(zip_code, start=1, date_range=date_range)
    r = http_get(SEARCH_URL, params=params)

    records, total, max_page = parse_results(r.text, zip_code)
    for rec in records:
        if rec["detail_url"] not in seen_urls:
            seen_urls.add(rec["detail_url"])
            all_records.append(rec)

    for page_num in range(2, max_page + 1):
        print(f"   📄 Page {page_num}/{max_page} (start={start_for_page(page_num)})...")
        params = build_params(zip_code, start=start_for_page(page_num), date_range=date_range)
        r = http_get(SEARCH_URL, params=params)
        records, _, _ = parse_results(r.text, zip_code)
        for rec in records:
            if rec["detail_url"] not in seen_urls:
                seen_urls.add(rec["detail_url"])
                all_records.append(rec)

    return total


def scrape_zip(zip_code, all_records, seen_urls, months_back=None):
    print(f"   Catch-all (undated facilities)...")
    scrape_query(zip_code, all_records, seen_urls, date_range=None)

    windows = weekly_windows(months_back)
    for sd, ed in windows:
        total = scrape_query(zip_code, all_records, seen_urls, date_range=(sd, ed))
        if total >= RESULTS_CAP:
            print(f"   ⚠️  zip={zip_code} week {sd}-{ed} hit the cap ({total}) — splitting into days.")
            for dsd, ded in daily_windows(sd, ed):
                day_total = scrape_query(zip_code, all_records, seen_urls, date_range=(dsd, ded))
                if day_total >= RESULTS_CAP:
                    print(f"   🛑 zip={zip_code} day={dsd} STILL at the cap ({day_total}) — "
                          f"this is beyond what date-splitting alone can resolve; flag for manual review.")


def scrape_to_dataframe(zips=None, debug_zip=None, months_back=None) -> pd.DataFrame:
    zips = [debug_zip] if debug_zip else (zips or PHILLY_ZIPS)
    print(f"\n🚀 Scraping Philadelphia — {len(zips)} ZIP code(s)"
          + (f", last {months_back} month(s)" if months_back else f", last {HISTORY_YEARS} years"))

    init_session()

    all_records = []
    seen_urls = set()

    for i, zip_code in enumerate(zips, 1):
        print(f"\n📮 ZIP {zip_code} ({i}/{len(zips)})")
        scrape_zip(zip_code, all_records, seen_urls, months_back=months_back)

    print(f"\n✅ Total unique facilities: {len(all_records)}")
    return pd.DataFrame(all_records)


def scrape(debug_zip=None, months_back=None):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = scrape_to_dataframe(debug_zip=debug_zip, months_back=months_back)

    if df.empty:
        print("⚠️  No data — check [debug] output above.")
        return

    print(f"\n📐 Columns: {list(df.columns)}")
    print(df.head(5).to_string())

    out_path = os.path.join(OUTPUT_DIR, "facilities.xlsx")
    df.to_excel(out_path, index=False)
    print(f"\n💾 Saved → {out_path}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("debug_zip", nargs="?", default=None)
    parser.add_argument("--months-back", type=int, default=None,
                         help="Only scan the last N months (for incremental runs). Default: full 3-year history.")
    args = parser.parse_args()
    scrape(debug_zip=args.debug_zip, months_back=args.months_back)
