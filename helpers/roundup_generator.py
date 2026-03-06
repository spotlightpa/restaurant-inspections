import os
import pandas as pd
from datetime import datetime, timedelta
from docx import Document
from docx.shared import Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from helpers.uploader import upload_to_s3


def get_week_range():
    today = datetime.today()
    start = today - timedelta(days=today.weekday())  # Monday
    end = start + timedelta(days=6)  # Sunday
    fmt = "%B %-d, %Y"
    return start.strftime(fmt), end.strftime(fmt), start.strftime("%Y-%m-%d")


def generate_roundup(file_path, county_slug):
    try:
        df = pd.read_excel(file_path)
        county_name = county_slug.title()
        start_of_week, end_of_week, date_slug = get_week_range()

        doc = Document()

        # Title
        title = doc.add_paragraph()
        title.alignment = WD_ALIGN_PARAGRAPH.LEFT
        run = title.add_run(f"{county_name} County Restaurant Inspections for {start_of_week} - {end_of_week}")
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
                p = doc.add_paragraph()
                p.add_run(str(row.get("facility", ""))).bold = True
                p.add_run(f"\n{row.get('address', '')}")
                p.add_run(f"\n{row.get('last_inspection_date', '')}")
                violations = row.get("violation_description", "")
                if pd.notna(violations) and str(violations).strip():
                    p.add_run(f"\n{violations}")

        # In compliance
        passed = df[df["compliance"].str.strip().str.lower() == "in"] if "compliance" in df.columns else df
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