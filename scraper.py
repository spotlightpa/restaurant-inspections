import re
import shutil
import os
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

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=500)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.goto(start_url)

        # Wait (ms) for the page to load completely
        page.wait_for_timeout(20000)

        # Identify the Power BI iframe
        report_frame = page.frame(url=re.compile(r"cedatareporting\.pa\.gov/powerbi/\?id="))
        if not report_frame:
            print("Could not find the main Power BI report frame.")
            browser.close()
            return

        # Click "Violation Details"
        tab_locator = report_frame.locator("text=Violation Details")
        try:
            tab_locator.wait_for(state="visible", timeout=30000)
            tab_locator.click()
            print("Clicked 'Violation Details' tab.")
        except TimeoutError:
            print("Violation Details tab not found or not visible.")
            browser.close()
            return

        report_frame.wait_for_timeout(5000)

        # Hover over the area
        hover_xpath = (
            "xpath=//*[@id='pvExplorationHost']/div/div/exploration/div/explore-canvas/"
            "div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container[19]/"
            "transform/div/div[2]/div/div"
        )
        hover_element = report_frame.locator(hover_xpath)
        try:
            hover_element.wait_for(state="visible", timeout=30000)
            hover_element.hover()
            print("Hovered over the visual area.")
        except TimeoutError:
            print("Hover element not found or not visible.")
            browser.close()
            return

        # Click the "..." menu icon
        button_xpath = (
            "xpath=//*[@id='pvExplorationHost']/div/div/exploration/div/explore-canvas/"
            "div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container[19]/"
            "transform/div/visual-container-header/div/div/div/visual-container-options-menu/"
            "visual-header-item-container/div"
        )
        button_locator = report_frame.locator(button_xpath)
        try:
            button_locator.wait_for(state="visible", timeout=30000)
            button_locator.click()
            print("Clicked the '...' menu button.")
        except TimeoutError:
            print("'...' menu button not found or not visible.")
            browser.close()
            return

        # Wait briefly (ms) to ensure the menu is rendered
        page.wait_for_timeout(2000)

        # Keyboard Navigation to Click "Export data"
        try:
            # Press the down arrow key to navigate to "Export data"
            page.keyboard.press("Enter")
            print("Navigated to 'Export data' using keyboard.")
        except Exception as e:
            print(f"Keyboard navigation failed: {e}")

        # Keyboard Navigation to Click "Export"
        try:
            # Press the Tab key
            for _ in range(4):
                page.keyboard.press("Tab")
                page.wait_for_timeout(200)  # Small (ms) delay to ensure stable navigation

            # Press Enter to activate the focused button (ms)
            with page.expect_download(timeout=60000) as download_info:
                page.keyboard.press("Enter")
                print("Activated 'Export data' button via keyboard navigation.")

            # Get the download object
            download = download_info.value

            # Save the downloaded file
            downloaded_file_path = download.path()
            destination_path = "inspections.xlsx"
            shutil.copy(downloaded_file_path, destination_path)

            print(f"File downloaded and saved as: {destination_path}")

            # Clean the data file
            clean_data(destination_path)

            # Process addresses and store them
            geocode(destination_path)

            # Build/merge unique facility categories store
            # upsert_categories(destination_path)

            # Label unlabeled rows with AI (exactly on facility/address/city)
            # label_categories_via_ai(
            #     destination_path,
            #     model=os.getenv("OPENAI_MODEL", "gpt-4o-mini")
            # )

            # Join categories back into export (exact match on facility/address/city)
            # join_categories_into_inspections(destination_path)

            # Drop the 'isp' column before uploading to S3 to reduce file size
            df_final = pd.read_excel(destination_path)
            if "isp" in df_final.columns:
                df_final.drop(columns=["isp"], inplace=True)
                df_final.to_excel(destination_path, index=False)

            # Upload to S3
            upload_to_s3(destination_path)

        except Exception as e:
            print(f"Download handling failed: {e}")

        # Wait to observe the result (ms)
        page.wait_for_timeout(5000)
        browser.close()


if __name__ == "__main__":
    main()
