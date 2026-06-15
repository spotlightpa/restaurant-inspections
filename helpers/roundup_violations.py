import re
import shutil
import os
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError
from helpers.cleaner import clean_data


def main():
    headless = os.getenv("CI", "false").lower() == "true"
    print(f"🔍 Running in {'headless' if headless else 'headed'} mode.")

    start_url = "http://cedatareporting.pa.gov/reports/powerbi/Public/AG/FS/PBI/Food_Safety_Inspections"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=500)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.goto(start_url)

        page.wait_for_timeout(20000)

        # Identify the Power BI iframe
        report_frame = page.frame(url=re.compile(r"cedatareporting\.pa\.gov/powerbi/\?id="))
        if not report_frame:
            print("Could not find the main Power BI report frame.")
            browser.close()
            return

        # We can add or remove counties here as needed
        counties = ["berks", "centre"]

        for county_search in counties:
                file_slug = county_search
                print(f"\nProcessing: {file_slug}")

                page.goto(start_url)
                page.wait_for_timeout(20000)

                report_frame = page.frame(url=re.compile(r"cedatareporting\.pa\.gov/powerbi/\?id="))
                if not report_frame:
                    print("Could not find report frame after reload.")
                    continue

                tab_locator = report_frame.locator("text=Violation Details")
                try:
                    tab_locator.wait_for(state="visible", timeout=30000)
                    tab_locator.click()
                    print("Clicked 'Violation Details' tab.")
                except TimeoutError:
                    print("Violation Details tab not found.")
                    continue

                report_frame.wait_for_timeout(10000)

                focus_div = report_frame.locator(".imageBackground").first
                try:
                    focus_div.click(timeout=15000, force=True)
                    print("Clicked into report area.")
                except Exception as e:
                    print(f"Could not click report area: {e}")
                    continue

                page.wait_for_timeout(500)

                for _ in range(7):
                    page.keyboard.press("Tab")
                    page.wait_for_timeout(150)

                page.keyboard.press("Enter")
                page.wait_for_timeout(300)
                page.keyboard.press("Enter")
                page.wait_for_timeout(500)

                page.keyboard.type(county_search, delay=100)
                page.wait_for_timeout(1000)
                page.keyboard.press("ArrowDown")
                page.wait_for_timeout(300)
                page.keyboard.press("Enter")
                report_frame.wait_for_timeout(3000)
                print(f"Selected county: {county_search}")

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

                try:
                    hover_element.wait_for(state="visible", timeout=30000)
                    hover_element.hover()
                    button_locator.click()
                    page.wait_for_timeout(2000)
                    page.keyboard.press("Enter")
                    page.wait_for_timeout(500)

                    for _ in range(4):
                        page.keyboard.press("Tab")
                        page.wait_for_timeout(200)

                    county_path = f"data/{file_slug}.xlsx"
                    os.makedirs("data", exist_ok=True)
                    with page.expect_download(timeout=60000) as dl_info:
                        page.keyboard.press("Enter")
                        print(f"Downloading {file_slug}...")

                    dl = dl_info.value
                    shutil.copy(dl.path(), county_path)
                    print(f"Saved: {county_path}")

                    clean_data(county_path)
                    print(f"Cleaned: {county_path}")

                    df = pd.read_excel(county_path)
                    df["county"] = county_search
                    df.to_excel(county_path, index=False)
                    print(f"Stamped county={county_search}")

                except Exception as e:
                    print(f"Download failed for {county_search}: {e}")

        all_files = [
            f"data/{county}.xlsx"
            for county in counties
        ]
        dfs = []
        for f in all_files:
            if os.path.exists(f):
                dfs.append(pd.read_excel(f))
            else:
                print(f"⚠️ Missing file, skipping: {f}")

        if dfs:
            roundup = pd.concat(dfs, ignore_index=True)
            roundup_path = "data/roundup.xlsx"
            roundup.to_excel(roundup_path, index=False)
            print(f"\n✅ Merged roundup saved: {roundup_path} ({len(roundup)} rows)")

            from helpers.roundup_violations_generator import generate_roundup_from_violations
            for county in counties:
                generate_roundup_from_violations(roundup_path, county)
        else:
            print("⚠️ No files to merge.")

        page.wait_for_timeout(5000)
        browser.close()


if __name__ == "__main__":
    main()