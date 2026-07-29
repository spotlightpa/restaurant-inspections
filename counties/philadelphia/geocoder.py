import os
import time
import pandas as pd
from dotenv import load_dotenv
from geocodio import GeocodioClient
from geocodio.exceptions import GeocodioAuthError, GeocodioDataError, GeocodioServerError

load_dotenv()

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "philadelphia")
FACILITIES_CLEANED_PATH = os.path.join(OUTPUT_DIR, "facilities_cleaned.xlsx")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "geocoded_addresses.csv")
FAILED_PATH = os.path.join(OUTPUT_DIR, "geocoded_addresses_failed.csv")

CHECKPOINT_EVERY = 25
PROGRESS_EVERY = 20
CALL_DELAY = 0.1

client = GeocodioClient(os.getenv("GEOCODIO_API_KEY"))


def geocode_address(address):
    try:
        response = client.geocode(address)
        time.sleep(CALL_DELAY)

        if response.get("results"):
            loc = response["results"][0]["location"]
            return loc.get("lat"), loc.get("lng"), None
        return None, None, "no results"

    except GeocodioAuthError as e:
        return None, None, f"QUOTA_EXCEEDED: {e}"
    except GeocodioDataError as e:
        return None, None, f"data error: {e}"
    except GeocodioServerError as e:
        return None, None, f"server error: {e}"
    except Exception as e:
        return None, None, str(e)


def geocode_all(limit=None):
    if not os.path.exists(FACILITIES_CLEANED_PATH):
        raise FileNotFoundError(
            f"{FACILITIES_CLEANED_PATH} not found — run field_cleaner.py first."
        )

    facilities = pd.read_excel(FACILITIES_CLEANED_PATH, dtype=str)
    addresses = facilities["address"].dropna().drop_duplicates().tolist()
    if limit:
        addresses = addresses[:limit]

    done_addresses = set()
    results = []
    if os.path.exists(OUTPUT_PATH):
        existing = pd.read_csv(OUTPUT_PATH, dtype=str)
        done_addresses = set(existing["Address"].unique())
        results = existing.to_dict("records")
        print(f"Resuming — {len(done_addresses)} addresses already geocoded in {OUTPUT_PATH}")

    todo = [a for a in addresses if a not in done_addresses]
    total_todo = len(todo)
    print(f"\n🚀 Geocoding {total_todo} addresses ({len(addresses) - total_todo} already done)")

    failed = []
    start_time = time.time()
    stopped_early = False
    i = 0

    for i, address in enumerate(todo, 1):
        lat, lng, error = geocode_address(address)

        if error and error.startswith("QUOTA_EXCEEDED"):
            print(f"\n⛔ API key hit its limit (or is invalid) after {i - 1}/{total_todo} addresses this run.")
            print("   Stopping cleanly — re-run this same command once quota resets")
            print("   (already-geocoded addresses will be skipped automatically).")
            stopped_early = True
            break

        if error:
            failed.append({"Address": address, "error": error})
        else:
            results.append({"Address": address, "Latitude": lat, "Longitude": lng})

        if i % PROGRESS_EVERY == 0 or i == total_todo:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (total_todo - i) / rate if rate > 0 else 0
            print(f"  [{i}/{total_todo}] elapsed {elapsed/60:.1f}m | ~{rate*60:.0f}/min | "
                  f"ETA {remaining/60:.0f}m | geocoded={len(results)} failed={len(failed)}")

        if i % CHECKPOINT_EVERY == 0:
            pd.DataFrame(results).to_csv(OUTPUT_PATH, index=False)
            if failed:
                pd.DataFrame(failed).to_csv(FAILED_PATH, index=False)

    pd.DataFrame(results).to_csv(OUTPUT_PATH, index=False)
    if failed:
        pd.DataFrame(failed).to_csv(FAILED_PATH, index=False)

    elapsed_total = time.time() - start_time
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Addresses attempted this run: {i if todo else 0}")
    print(f"Failed (no results/errors):   {len(failed)}")
    print(f"Stopped early (quota):        {stopped_early}")
    print(f"Elapsed time: {elapsed_total/60:.1f} minutes")
    print(f"Total addresses geocoded (all runs): {len(results)}")
    print(f"\n💾 Saved → {OUTPUT_PATH}")
    if failed:
        print(f"💾 Failures saved → {FAILED_PATH}")
    return pd.DataFrame(results)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                         help="Only geocode the first N addresses (for testing)")
    args = parser.parse_args()
    geocode_all(limit=args.limit)