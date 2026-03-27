import os
import re
import pandas as pd
from datetime import datetime, timedelta
from docx import Document
from docx.shared import Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement


def add_hyperlink(paragraph, text, url):
    part = paragraph.part
    r_id = part.relate_to(url, "http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink", is_external=True)
    hyperlink = OxmlElement("w:hyperlink")
    hyperlink.set(qn("r:id"), r_id)
    run = OxmlElement("w:r")
    rPr = OxmlElement("w:rPr")
    rStyle = OxmlElement("w:rStyle")
    rStyle.set(qn("w:val"), "Hyperlink")
    rPr.append(rStyle)
    run.append(rPr)
    t = OxmlElement("w:t")
    t.text = text
    run.append(t)
    hyperlink.append(run)
    paragraph._p.append(hyperlink)
    return hyperlink


AP_MONTHS = {
    "January": "Jan.", "February": "Feb.", "March": "March",
    "April": "April", "May": "May", "June": "June",
    "July": "July", "August": "Aug.", "September": "Sept.",
    "October": "Oct.", "November": "Nov.", "December": "Dec."
}

AP_MONTHS_REVERSE = {v: k for k, v in AP_MONTHS.items()}

AP_NUMS = {
    1: "One", 2: "Two", 3: "Three", 4: "Four", 5: "Five",
    6: "Six", 7: "Seven", 8: "Eight", 9: "Nine"
}


def reverse_ap_date(s):
    if not isinstance(s, str):
        return pd.NaT
    for abbr, full in AP_MONTHS_REVERSE.items():
        s = s.replace(abbr, full)
    return pd.to_datetime(s, errors="coerce")


def ap_date(dt, include_month=True):
    month = AP_MONTHS[dt.strftime("%B")]
    day = str(dt.day)
    return f"{month} {day}" if include_month else day


def get_week_range():
    today = datetime.today()
    start = today - timedelta(days=today.weekday() + 7)  # Monday of last week
    end = start + timedelta(days=6)  # Sunday of last week
    if start.month == end.month:
        date_range = f"{ap_date(start)}-{end.day}"
    else:
        date_range = f"{ap_date(start)}-{ap_date(end)}"
    return date_range, start.strftime("%Y-%m-%d"), start, end


def generate_roundup_from_violations(roundup_path, county_slug):
    try:
        df = pd.read_excel(roundup_path)

        # Join ai_summary + risk_level from main inspections file
        inspections_path = "data/inspections.xlsx"
        try:
            insp = pd.read_excel(inspections_path, dtype=str)
            join_cols = [c for c in ["facility", "inspection_date", "ai_summary", "risk_level"] if c in insp.columns]
            if "facility" in join_cols and "inspection_date" in join_cols:
                insp = insp[join_cols].drop_duplicates(subset=["facility", "inspection_date"])
                df = df.merge(insp, on=["facility", "inspection_date"], how="left")
                print("Joined ai_summary and risk_level from inspections.xlsx")
                matched = df["ai_summary"].notna().sum()
                print(f"  ai_summary matched on {matched} of {len(df)} rows")
                if matched == 0:
                    print(f"  Sample roundup dates: {df['inspection_date'].head(3).tolist()}")
                    print(f"  Sample inspections dates: {insp['inspection_date'].head(3).tolist()}")
        except Exception as e:
            print(f"⚠️ Could not join from inspections.xlsx: {e}")

        # Filter to this county
        df = df[df["county"].str.strip().str.lower() == county_slug.lower()].copy()
        if df.empty:
            print(f"⚠️ No data for county: {county_slug}")
            return

        county_name = county_slug.title()
        date_range, date_slug, week_start, week_end = get_week_range()

        # Parse inspection dates
        df["_parsed_date"] = df["inspection_date"].apply(reverse_ap_date)

        # Filter to last completed week
        df = df[
            (df["_parsed_date"] >= pd.Timestamp(week_start.date())) &
            (df["_parsed_date"] <= pd.Timestamp(week_end.date()))
        ]

        if df.empty:
            print(f"⚠️ No inspections in date range for {county_slug}")
            return

        # Split out/in
        out = df[df["compliance"].str.strip().str.lower() == "out"].copy()
        passed = df[df["compliance"].str.strip().str.lower() == "in"].copy()

        # Deduplicate passed — one row per facility+date for the list
        passed_deduped = passed.drop_duplicates(subset=["facility", "inspection_date"])

        # Build document
        doc = Document()

        # Title
        title = doc.add_paragraph()
        title.alignment = WD_ALIGN_PARAGRAPH.LEFT
        run = title.add_run(f"{county_name} County Restaurant Inspections, {date_range}")
        run.bold = True
        run.font.size = Pt(20)

        # Date
        doc.add_paragraph(datetime.today().strftime("%B %-d, %Y"))

        # Intro
        intro = doc.add_paragraph(
            f"The Pennsylvania Department of Agriculture inspected the following food establishments "
            f"in {county_name} County this week. Find the full database at Spotlight PA's "
        )
        add_hyperlink(intro, "Restaurant Safety Tracker", "https://www.spotlightpa.org/restaurant-inspections")
        intro.add_run(".")

        # Out-of-compliance section
        heading_out = doc.add_paragraph()
        run_out = heading_out.add_run("Out-of-compliance inspections this week:")
        run_out.bold = True
        run_out.font.size = Pt(16)

        if out.empty:
            doc.add_paragraph("No out-of-compliance inspections this week.")
        else:
            # Group by facility+date so violations are aggregated per inspection
            for (facility, inspection_date), group in out.groupby(["facility", "inspection_date"], sort=False):
                address = str(group.iloc[0].get("address", ""))
                date = str(inspection_date)

                facility_slug = re.sub(r'[^a-z0-9]+', '-', facility.lower()).strip('-')
                tracker_url = f"https://www.spotlightpa.org/restaurant-inspections/#{facility_slug}"

                p = doc.add_paragraph()
                add_hyperlink(p, facility, tracker_url)
                p.add_run(f"\n{address}").italic = True
                p.add_run(f"\n{date}").italic = True

                # Count violations by risk level and show summary line
                if "risk_level" in group.columns:
                    all_levels = (
                        group["risk_level"]
                        .dropna()
                        .astype(str)
                        .str.split(r"\s*\|\s*")
                        .explode()
                        .str.strip()
                        .str.title()
                    )
                    all_levels = all_levels[~all_levels.isin(["Na", "Nan", ""])]
                    counts = all_levels.value_counts()
                    parts = []
                    for level in ["High Risk", "Moderate Risk", "Low Risk"]:
                        count = counts.get(level, 0)
                        if count:
                            clean_level = re.sub(r'\s*risk\s*', '', level, flags=re.IGNORECASE).strip().lower()
                            parts.append(f"{count} {clean_level} risk")
                    if parts:
                        total = sum(counts.values)
                        summary_line = ", ".join(parts) + " violation" + ("s" if total != 1 else "")
                        doc.add_paragraph(summary_line)

                # Add AI summaries as bullets, ordered high to low risk, with bold risk label
                if "ai_summary" in group.columns:
                    risk_order = {"High Risk": 0, "Moderate Risk": 1, "Low Risk": 2}

                    # Explode pipe-delimited comments and risk levels into individual rows
                    pairs = []
                    for _, vrow in group.iterrows():
                        raw_summary = str(vrow.get("comment", ""))
                        raw_risk = str(vrow.get("risk_level", ""))
                        if not raw_summary or raw_summary.lower() in ("nan", ""):
                            continue
                        summaries = [s.strip() for s in raw_summary.split(" | ") if s.strip()]
                        risks = [r.strip() for r in raw_risk.split(" | ") if r.strip()] if raw_risk and raw_risk.lower() not in ("nan", "") else []
                        for i, summary in enumerate(summaries):
                            risk = risks[i] if i < len(risks) else ""
                            sort_key = risk_order.get(risk.title(), 99)
                            pairs.append((sort_key, risk, summary))

                    # Sort all individual violations high to low
                    pairs.sort(key=lambda x: x[0])

                    for _, risk_label, summary in pairs:
                        p = doc.add_paragraph(style="List Bullet")
                        if risk_label:
                            clean_label = re.sub(r'\s*risk\s*', '', risk_label, flags=re.IGNORECASE).strip().title()
                            p.add_run(f"{clean_label}: ").bold = True
                        p.add_run(summary)

        # In-compliance section
        heading_in = doc.add_paragraph()
        run_in = heading_in.add_run("These establishments passed inspections this week:")
        run_in.bold = True
        run_in.font.size = Pt(16)

        if passed_deduped.empty:
            doc.add_paragraph("No passing inspections recorded this week.")
        else:
            for _, row in passed_deduped.iterrows():
                facility = str(row.get("facility", ""))
                address = str(row.get("address", ""))
                date = str(row.get("inspection_date", ""))
                p = doc.add_paragraph(style="List Bullet")
                p.add_run(facility).bold = True
                p.add_run(f", {address}, {date}")

        # Save docx
        folder = os.path.join("data", "roundup", county_slug)
        os.makedirs(folder, exist_ok=True)
        output_path = os.path.join(folder, f"{date_slug}.docx")
        doc.save(output_path)
        print(f"Roundup saved: {output_path}")

        # Save plain text preview
        txt_path = output_path.replace(".docx", ".txt")
        with open(txt_path, "w", encoding="utf-8") as f:
            for para in doc.paragraphs:
                f.write(para.text + "\n")
        print(f"Text preview saved: {txt_path}")

        # Upload to S3
        from helpers.uploader import upload_to_s3
        s3_key = f"2025/restaurant-inspections/roundup/{county_slug}/{date_slug}.docx"
        upload_to_s3(output_path, s3_key_override=s3_key)

    except Exception as e:
        print(f"Roundup generation failed for {county_slug}: {e}")
        import traceback
        traceback.print_exc()