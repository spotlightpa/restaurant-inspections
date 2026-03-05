import pandas as pd
import re
from helpers.dictionaries.ap_months import AP_MONTHS
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

        # Facility cleaning
        df["facility"] = df["facility"].apply(lambda x: x.title() if isinstance(x, str) else x)

        df["facility"] = df["facility"].apply(
            lambda x: re.sub(r"[`´'']", "'", x) if isinstance(x, str) else x
        )
        df["facility"] = df["facility"].apply(
            lambda x: re.sub(r"(\b\w+)'S\b", lambda m: f"{m.group(1)}'s", x) if isinstance(x, str) else x
        )
        df["facility"] = df["facility"].apply(
            lambda x: re.sub(r'\bLlc\b', 'LLC', x) if isinstance(x, str) else x
        )
        df["facility"] = df["facility"].apply(
            lambda x: re.sub(r'\bDba\b', 'DBA', x) if isinstance(x, str) else x
        )

        # Address cleaning
        df["address"] = df["address"].astype(str).str.replace(r'\s*\n\s*', ', ', regex=True)
        df["address"] = df["address"].apply(lambda x: x.title() if isinstance(x, str) else x)
        df["address"] = df["address"].apply(
            lambda x: re.sub(r"\b(N|S|E|W|NE|NW|SE|SW)\b", r"\1.", x) if isinstance(x, str) else x
        )
        df["address"] = df["address"].apply(
            lambda x: re.sub(r'(\s)Pa(\s)', r', PA\2', x) if isinstance(x, str) else x
        )

        def replace_street_type(address):
            if isinstance(address, str):
                for full, abbr in AP_STREET_ABBREVIATIONS.items():
                    address = re.sub(rf"\b{full}\b", abbr, address)
            return address

        df["address"] = df["address"].apply(replace_street_type)
        df["address"] = df["address"].apply(
            lambda x: re.sub(r"(?<=\d)(ST|ND|RD|TH)\b", lambda m: m.group(0).lower(), x, flags=re.IGNORECASE) if isinstance(x, str) else x
        )
        df["address"] = df["address"].apply(lambda x: re.sub(r"\.{2,}", ".", x) if isinstance(x, str) else x)
        df["address"] = df["address"].apply(lambda x: re.sub(r",\s*,+", ", ", x) if isinstance(x, str) else x)

        # --- City extraction ---
        def extract_city(address):
            if isinstance(address, str):
                match = re.search(r",\s*([^,]+)\s*,\s*PA\s", address)
                if match:
                    return match.group(1).strip()
            return ""

        df.insert(df.columns.get_loc("address") + 1, "city", df["address"].apply(extract_city))

        # Date cleaning
        df["last_inspection_date"] = pd.to_datetime(df["last_inspection_date"], errors="coerce")
        df = df.sort_values(by="last_inspection_date", ascending=False)
        df["last_inspection_date"] = df["last_inspection_date"].dt.strftime("%B %-d, %Y")
        df["last_inspection_date"] = df["last_inspection_date"].apply(
            lambda x: re.sub(r"(" + "|".join(AP_MONTHS.keys()) + r")", lambda m: AP_MONTHS[m.group()], x) if pd.notna(x) else x
        )

        df.to_excel(file_path, index=False)
        print(f"Cleaned facilities data saved as: {file_path}")

    except Exception as e:
        print(f"Facilities cleaning failed: {e}")