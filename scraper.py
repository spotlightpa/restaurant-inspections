import pandas as pd
import re
import shutil
from playwright.sync_api import sync_playwright, TimeoutError

# AP style dictionary map
AP_MONTHS = {
    "January": "Jan.",
    "February": "Feb.",
    "March": "March",
    "April": "April",
    "May": "May",
    "June": "June",
    "July": "July",
    "August": "Aug.",
    "September": "Sept.",
    "October": "Oct.",
    "November": "Nov.",
    "December": "Dec."
}

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

        # Define words that should not be capitalized unless at the start
        small_words = {"a", "an", "and", "as", "at", "but", "by", "for", "if", "in", "nor", 
                       "of", "on", "or", "so", "the", "to", "up", "yet"}

        # Convert to title case
        df["facility"] = df["facility"].apply(lambda x: x.title() if isinstance(x, str) else x)

        # Correct small words
        df["facility"] = df["facility"].apply(lambda x: ' '.join(
            [word if i == 0 or word.lower() not in small_words else word.lower() 
             for i, word in enumerate(x.split())]) if isinstance(x, str) else x
        )

        # Normalize apostrophes (replace backticks and other variations with a standard apostrophe)
        df["facility"] = df["facility"].apply(
            lambda x: re.sub(r"[`´‘’]", "'", x) if isinstance(x, str) else x
        )

        # Fix possessive capitalization (e.g., "Joe'S" to "Joe's")
        df["facility"] = df["facility"].apply(
            lambda x: re.sub(r"(\b\w+)'S\b", lambda m: f"{m.group(1)}'s", x) if isinstance(x, str) else x
        )

        # Replace "Llc" with "LLC"
        df["facility"] = df["facility"].apply(
            lambda x: re.sub(r'\bLlc\b', 'LLC', x) if isinstance(x, str) else x
        )

        # Replace "Dba" with "DBA"
        df["facility"] = df["facility"].apply(
            lambda x: re.sub(r'\bDba\b', 'DBA', x) if isinstance(x, str) else x
        )

        # Convert address to title case
        df["address"] = df["address"].apply(lambda x: x.title() if isinstance(x, str) else x)

        # Replace " Pa " with ", PA " in address
        df["address"] = df["address"].apply(
            lambda x: re.sub(r'(\s)Pa(\s)', r', PA\2', x) if isinstance(x, str) else x
        )

        # Replace hidden line breaks with commas in address
        df["address"] = df["address"].astype(str).str.replace(r'\s*\n\s*', ', ', regex=True)

        # Convert inspection_date to datetime
        df['inspection_date'] = pd.to_datetime(df['inspection_date'], errors='coerce')

        # Sort by descending
        df = df.sort_values(by='inspection_date', ascending=False)

        # Format the date
        df['inspection_date'] = df['inspection_date'].dt.strftime('%B %-d, %Y')

        # Replace month names with AP style abbreviations
        for full_month, ap_month in AP_MONTHS.items():
            df["inspection_date"] = df["inspection_date"].str.replace(full_month, ap_month, regex=False)

        # Extract city using ", PA " as the right boundary and the last comma before it as the left boundary — this works better with ", PA " than simply splitting by commas
        def extract_city(address):
            if isinstance(address, str):
                match = re.search(r",\s*([^,]+)\s*,\s*PA\s", address)
                if match:
                    return match.group(1).strip()
            return ""

        # Insert 'city' column right after 'address'
        df.insert(df.columns.get_loc("address") + 1, "city", df["address"].apply(extract_city))

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

        except Exception as e:
            print(f"Download handling failed: {e}")

        # Wait to observe the result (ms)
        page.wait_for_timeout(5000)
        browser.close()

if __name__ == "__main__":
    main()
