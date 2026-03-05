import pandas as pd
import re
from helpers.dictionaries.ap_addresses import AP_STREET_ABBREVIATIONS


def clean_facilities(file_path):
    try:
        df = pd.read_excel(file_path, header=2)

        # Rename columns to match expected names
        new_column_names = [
            "facility",
            "address",
            "phone",
            "active_status",
            "last_inspection_date",
            "inspection_reason",
            "compliance",
            "organization"
        ]

        if len(df.columns) == len(new_column_names):
            df.columns = new_column_names
        else:
            print(f"Warning: Column count mismatch. Expected {len(new_column_names)}, got {len(df.columns)}: {list(df.columns)}")

        # Trim whitespace
        df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))

        # Replace hidden line breaks in address
        df["address"] = df["address"].astype(str).str.replace(r'\s*\n\s*', ', ', regex=True)

        # Replace " Pa " with ", PA "
        df["address"] = df["address"].apply(
            lambda x: re.sub(r'(\s)Pa(\s)', r', PA\2', x) if isinstance(x, str) else x
        )

        # Extract city
        def extract_city(address):
            if isinstance(address, str):
                match = re.search(r",\s*([^,]+)\s*,\s*PA\s", address)
                if match:
                    return match.group(1).strip()
            return ""

        df.insert(df.columns.get_loc("address") + 1, "city", df["address"].apply(extract_city))

        # Replace streets with AP Style abbreviations
        def replace_street_type(address):
            if isinstance(address, str):
                for full, abbr in AP_STREET_ABBREVIATIONS.items():
                    address = re.sub(rf"\b{full}\b", abbr, address)
            return address

        df["address"] = df["address"].apply(replace_street_type)

        # Convert last_inspection_date to datetime and format
        df["last_inspection_date"] = pd.to_datetime(df["last_inspection_date"], errors="coerce")
        df = df.sort_values(by="last_inspection_date", ascending=False)
        df["last_inspection_date"] = df["last_inspection_date"].dt.strftime("%B %-d, %Y")

        df.to_excel(file_path, index=False)
        print(f"Cleaned facilities data saved as: {file_path}")

    except Exception as e:
        print(f"Facilities cleaning failed: {e}")