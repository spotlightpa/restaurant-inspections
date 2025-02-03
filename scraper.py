import pandas as pd
import re
import shutil
from playwright.sync_api import sync_playwright, TimeoutError

def clean_data(file_path):
    try:
        # Read the Excel file
        df = pd.read_excel(file_path)

        # Remove the first two rows
        df = df.iloc[2:].reset_index(drop=True)

        # Define new column names
        new_column_names = [
            "isp", 
            "inspection_date", 
            "inspection_reason", 
            "facility", 
            "address", 
            "violation_code", 
            "violation_description", 
            "comment"
        ]

        # Ensure the number of columns matches before renaming
        if len(df.columns) == len(new_column_names):
            df.columns = new_column_names
        else:
            print(f"Warning: Column count mismatch. Expected {len(new_column_names)}, but got {len(df.columns)}.")

        # Trim whitespace in every string cell
        df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))

        # Convert to datetime
        df['inspection_date'] = pd.to_datetime(df['inspection_date'], errors='coerce')

        # Sort by descending
        df = df.sort_values(by='inspection_date', ascending=False)

        # Format the date
        df['inspection_date'] = df['inspection_date'].dt.strftime('%B %d, %Y')

        # Save the cleaned data
        df.to_excel(file_path, index=False)

        print(f"Cleaned data saved as: {file_path}")
    except Exception as e:
        print(f"Data cleaning failed: {e}")


def main():
    start_url = "http://cedatareporting.pa.gov/reports/powerbi/Public/AG/FS/PBI/Food_Safety_Inspections"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=500)
        # Set up a browser context with a designated download path
        context = browser.new_context(accept_downloads=True)

        # Enable console logging
        def on_console_message(msg):
            print(f"Console {msg.type}: {msg.text}")
        context.on("page", lambda page: page.on("console", on_console_message))

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
            "xpath=/html/body/div[1]/ui-view/div/div/div/div/div/div/exploration-container/"
            "div/docking-container/div/div/div/div/exploration-host/div/div/exploration/"
            "div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/"
            "visual-container[19]/transform/div/div[3]/div/visual-modern/div/div/div[2]/"
            "div[1]/div[1]/div[8]/div"
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
            "visual-header-item-container/div/button"
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
            # Press the Tab key 5 times
            for _ in range(5):
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

        except Exception as e:
            print(f"Download handling failed: {e}")

        # Wait to observe the result (ms)
        page.wait_for_timeout(5000)
        browser.close()

if __name__ == "__main__":
    main()
