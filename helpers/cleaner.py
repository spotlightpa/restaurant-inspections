import pandas as pd
import re
from helpers.dictionaries.ap_months import AP_MONTHS
from helpers.dictionaries.ap_addresses import AP_STREET_ABBREVIATIONS
from helpers.dictionaries.title_case import TITLE_CASE

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

        # Convert to title case
        df["facility"] = df["facility"].apply(lambda x: x.title() if isinstance(x, str) else x)

        # Correct small words in facility names
        df["facility"] = df["facility"].apply(lambda x: ' '.join(
            [word if i == 0 or word.lower() not in TITLE_CASE else word.lower() 
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

        # Replace compass directions with AP style
        df["address"] = df["address"].apply(
            lambda x: re.sub(r"\b(N|S|E|W|NE|NW|SE|SW)\b", r"\1.", x) if isinstance(x, str) else x
        )
        
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

         # Replace months with AP Style abbreviations
        df["inspection_date"] = df["inspection_date"].apply(
            lambda x: re.sub(r"(" + "|".join(AP_MONTHS.keys()) + r")", lambda m: AP_MONTHS[m.group()], x) if pd.notna(x) else x
        )

        # Replace streets with AP Style abbreviations
        def replace_street_type(address):
            if isinstance(address, str):
                for full, abbr in AP_STREET_ABBREVIATIONS.items():
                    address = re.sub(rf"\b{full}\b", abbr, address)
            return address

        df["address"] = df["address"].apply(replace_street_type)

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