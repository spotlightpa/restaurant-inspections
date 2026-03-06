import os
import pandas as pd
from datetime import datetime, timedelta
from docx import Document
from docx.shared import Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from helpers.uploader import upload_to_s3


AP_MONTHS = {
    "January": "Jan.", "February": "Feb.", "March": "March",
    "April": "April", "May": "May", "June": "June",
    "July": "July", "August": "Aug.", "September": "Sept.",
    "October": "Oct.", "November": "Nov.", "December": "Dec."
}

def ap_date(dt, include_month=True):
    month = AP_MONTHS[dt.strftime("%B")]
    day = str(dt.day)
    return f"{month} {day}" if include_month else day

def get_week_range():
    today = datetime.today()
    # Go back to the most recently completed week (last Monday–Sunday)
    start = today - timedelta(days=today.weekday() + 7)  # Monday of last week
    end = start + timedelta(days=6)  # Sunday of last week
    # If same month: "Sept. 3-9", if different months: "Sept. 29-Oct. 5"
    if start.month == end.month:
        date_range = f"{ap_date(start)}-{end.day}"
    else:
        date_range = f"{ap_date(start)}-{ap_date(end)}"
    return date_range, start.strftime("%Y-%m-%d")


def generate_roundup(file_path, county_slug):
    try:
        df = pd.read_excel(file_path)

        # Load inspections to get violation risk levels
        inspections_path = "data/inspections.xlsx"
        try:
            insp = pd.read_excel(inspections_path, dtype=str)
            insp["facility"] = insp["facility"].fillna("").str.strip()
            insp["city"] = insp["city"].fillna("").str.strip()
        except Exception as e:
            print(f"⚠️ Could not load inspections for violation counts: {e}")
            insp = pd.DataFrame()
        county_name = county_slug.title()
        date_range, date_slug = get_week_range()

        doc = Document()

        # Title
        title = doc.add_paragraph()
        title.alignment = WD_ALIGN_PARAGRAPH.LEFT
        run = title.add_run(f"{county_name} County Restaurant Inspections, {date_range}")
        run.bold = True
        run.font.size = Pt(14)

        # Date
        doc.add_paragraph(datetime.today().strftime("%B %-d, %Y"))

        # Intro
        doc.add_paragraph(
            f"The Pennsylvania Department of Agriculture inspected the following food establishments "
            f"in {county_name} County this week. Find the full database at Spotlight PA's Restaurant Safety Tracker."
        )

        # Out of compliance
        out = df[df["compliance"].str.strip().str.lower() == "out"] if "compliance" in df.columns else pd.DataFrame()
        heading_out = doc.add_paragraph()
        run_out = heading_out.add_run("Out-of-compliance inspections this week:")
        run_out.bold = True
        run_out.font.size = Pt(12)

        if out.empty:
            doc.add_paragraph("No out-of-compliance inspections this week.")
        else:
            for _, row in out.iterrows():
                facility = str(row.get("facility", ""))
                city = str(row.get("city", ""))
                address = str(row.get("address", ""))
                date = str(row.get("last_inspection_date", ""))

                p = doc.add_paragraph()
                p.add_run(facility).bold = True

                addr_run = p.add_run(f"\n{address}")
                addr_run.italic = True

                date_run = p.add_run(f"\n{date}")
                date_run.italic = True

                # Count violations by risk level from inspections
                if not insp.empty:
                    match = insp[
                        (insp["facility"] == facility) &
                        (insp["city"] == city)
                    ]
                    if not match.empty and "risk_level" in match.columns:
                        all_levels = (
                            match["risk_level"]
                            .dropna()
                            .str.split(r"\s*\|\s*")
                            .explode()
                            .str.strip()
                            .str.title()
                        )
                        all_levels = all_levels[all_levels != "Na"]
                        counts = all_levels.value_counts()
                        if not counts.empty:
                            ap_nums = {
                                1: "One", 2: "Two", 3: "Three", 4: "Four",
                                5: "Five", 6: "Six", 7: "Seven", 8: "Eight", 9: "Nine"
                            }
                            for level, count in counts.items():
                                num = ap_nums.get(count, str(count))
                                label = f"{level.lower()} risk violation" + ("s" if count != 1 else "")
                                doc.add_paragraph(f"{num} {label}", style="List Bullet")

        # In compliance - filter to last completed week only
        passed = df[df["compliance"].str.strip().str.lower() == "in"] if "compliance" in df.columns else df
        if "last_inspection_date" in passed.columns:
            passed = passed.copy()
            AP_MONTHS_REVERSE = {
                "Jan.": "January", "Feb.": "February", "March": "March",
                "April": "April", "May": "May", "June": "June",
                "July": "July", "Aug.": "August", "Sept.": "September",
                "Oct.": "October", "Nov.": "November", "Dec.": "December"
            }
            def reverse_ap_date(s):
                if not isinstance(s, str):
                    return pd.NaT
                for abbr, full in AP_MONTHS_REVERSE.items():
                    s = s.replace(abbr, full)
                return pd.to_datetime(s, errors="coerce")

            passed["_parsed_date"] = passed["last_inspection_date"].apply(reverse_ap_date)
            week_start = datetime.today() - timedelta(days=datetime.today().weekday() + 7)
            week_end = week_start + timedelta(days=6)
            passed = passed[
                (passed["_parsed_date"] >= pd.Timestamp(week_start.date())) &
                (passed["_parsed_date"] <= pd.Timestamp(week_end.date()))
            ]
            passed = passed.drop(columns=["_parsed_date"])
        heading_in = doc.add_paragraph()
        run_in = heading_in.add_run("These establishments passed inspections this week:")
        run_in.bold = True
        run_in.font.size = Pt(12)

        for _, row in passed.iterrows():
            facility = str(row.get("facility", ""))
            address = str(row.get("address", ""))
            date = str(row.get("last_inspection_date", ""))
            doc.add_paragraph(f"{facility}, {address}, {date}", style="List Bullet")

        # Save
        folder = os.path.join("data", "roundup", county_slug)
        os.makedirs(folder, exist_ok=True)
        output_path = os.path.join(folder, f"{date_slug}.docx")
        doc.save(output_path)
        print(f"Roundup saved: {output_path}")

        # Upload to S3
        s3_key_override = f"2025/restaurant-inspections/roundup/{county_slug}/{date_slug}.docx"
        upload_to_s3(output_path, s3_key_override=s3_key_override)

    except Exception as e:
        print(f"Roundup generation failed for {county_slug}: {e}")