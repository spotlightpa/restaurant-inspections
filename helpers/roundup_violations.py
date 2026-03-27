import re
import shutil
import os
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError


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
            for compliance in ["out", "in"]:
                file_slug = f"{county_search}_{compliance}"
                print(f"\nProcessing: {file_slug}")

                # Reload the page fresh for each iteration to avoid stale state
                page.goto(start_url)
                page.wait_for_timeout(20000)

                report_frame = page.frame(url=re.compile(r"cedatareporting\.pa\.gov/powerbi/\?id="))
                if not report_frame:
                    print("Could not find report frame after reload.")
                    continue

                # Click Violation Details tab
                tab_locator = report_frame.locator("text=Violation Details")
                try:
                    tab_locator.wait_for(state="visible", timeout=30000)
                    tab_locator.click()
                    print("Clicked 'Violation Details' tab.")
                except TimeoutError:
                    print("Violation Details tab not found.")
                    continue

                report_frame.wait_for_timeout(10000)

                # Click the imageBackground div to get focus into the report area
                focus_div = report_frame.locator(".imageBackground").first
                try:
                    focus_div.click(timeout=15000, force=True)
                    print("Clicked into report area.")
                except Exception as e:
                    print(f"Could not click report area: {e}")
                    continue

                page.wait_for_timeout(500)

                # Tab to reach the Compliance slicer
                for _ in range(13):
                    page.keyboard.press("Tab")
                    page.wait_for_timeout(150)

                # Enter to focus slicer, select compliance filter
                page.keyboard.press("Enter")
                page.wait_for_timeout(300)
                page.keyboard.press("ArrowRight")
                page.wait_for_timeout(300)
                if compliance == "out":
                    page.keyboard.press("ArrowRight")
                    page.wait_for_timeout(300)
                page.keyboard.press("Enter")
                report_frame.wait_for_timeout(3000)
                print(f"Selected '{compliance}' via keyboard.")

                # Tab to State-County
                tab_count = 14 if compliance == "in" else 13
                for _ in range(tab_count):
                    page.keyboard.press("Tab")
                    page.wait_for_timeout(150)

                # Open dropdown
                page.keyboard.press("Enter")
                page.wait_for_timeout(500)

                # Arrow down into list, then Enter to open search
                page.keyboard.press("ArrowDown")
                page.wait_for_timeout(300)
                page.keyboard.press("Enter")
                page.wait_for_timeout(500)

                # Type county name and select
                page.keyboard.type(county_search, delay=100)
                page.wait_for_timeout(1000)
                page.keyboard.press("ArrowDown")
                page.wait_for_timeout(300)
                page.keyboard.press("Enter")
                report_frame.wait_for_timeout(3000)
                print(f"Selected county: {county_search}")

                # Close dropdown
                page.keyboard.press("Escape")
                report_frame.wait_for_timeout(2000)

                # Re-locate hover/button elements after reload
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

                # Hover and export
                try:
                    hover_element.wait_for(state="visible", timeout=30000)
                    hover_element.hover()
                    button_locator.click()
                    page.wait_for_timeout(2000)
                    page.keyboard.press("Enter")

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

                except Exception as e:
                    print(f"Download failed for {county_search}: {e}")

        page.wait_for_timeout(5000)
        browser.close()


if __name__ == "__main__":
    main()