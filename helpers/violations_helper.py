import re
import io
import os
import boto3
import pandas as pd

def clean_violation_code(code: str) -> str:

    # Remove letters and parentheses from violation code
    if pd.isna(code):
        return ""
    
    code = str(code).strip()
    
    # Remove everything in parentheses and the parentheses
    cleaned = re.sub(r'\([^)]*\)', '', code)
    
    # Normalize spaces around hyphens
    cleaned = re.sub(r'\s*-\s*', ' - ', cleaned)
    
    # Remove any remaining letters
    cleaned = re.sub(r'[a-zA-Z]', '', cleaned)
    
    # Clean up any multiple spaces
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Remove trailing hyphens, periods, and spaces
    cleaned = cleaned.rstrip('- .')
    
    # Remove leading hyphens, periods, and spaces
    cleaned = cleaned.lstrip('- .')
    
    # Final trim
    cleaned = cleaned.strip()
    
    return cleaned

def translate_priority_to_risk(priority_level: str) -> str:
    if not priority_level or priority_level == "NA":
        return "NA"
    
    # Mapping dictionary
    risk_map = {
        "P": "high risk",
        "Pf": "moderate risk",
        "C": "low risk"
    }
    
    # Split by comma in case there are multiple
    parts = [p.strip() for p in priority_level.split(",")]
    risk_parts = []
    
    for part in parts:
        if part in risk_map:
            risk_parts.append(risk_map[part])
        else:
            risk_parts.append("NA")
    
    return ", ".join(risk_parts)

def join_violation_details(local_inspections_file: str) -> bool:
    try:
        # Load inspections
        df = pd.read_excel(local_inspections_file, dtype=str)
        print(f"Loaded inspections file with {len(df)} rows")
        
        if "violation_code" not in df.columns:
            print("No violation_code column found, skipping violation details join")
            return True
            
        # Download from S3
        AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
        AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
        AWS_REGION = os.getenv("AWS_REGION")
        
        if not all([AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET_NAME, AWS_REGION]):
            print("Missing AWS credentials for food-codes download")
            return False
            
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )
        
        food_codes_key = "2025/restaurant-inspections/food-codes.csv"
        
        try:
            s3_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=food_codes_key)
            food_codes = pd.read_csv(io.BytesIO(s3_obj["Body"].read()), dtype=str)
            print(f"Loaded food-codes.csv from S3 with {len(food_codes)} codes")
        except s3_client.exceptions.NoSuchKey:
            print(f"food-codes.csv not found at s3://{S3_BUCKET_NAME}/{food_codes_key}")
            return False
        except Exception as e:
            print(f"Error downloading food-codes.csv: {e}")
            return False
            
        # Clean and prepare food codes lookup
        required_cols = ["Requirement", "Spotlight PA Category", "Priority Level", "Requirement Description"]
        if not all(col in food_codes.columns for col in required_cols):
            print(f"food-codes.csv missing required columns. Has: {food_codes.columns.tolist()}")
            return False
            
        # Strip whitespace from Requirement column and create lookup dict
        food_codes["Requirement"] = food_codes["Requirement"].fillna("").astype(str).str.strip()
        
        # Create lookup dictionaries
        lookup = {}
        for _, row in food_codes.iterrows():
            req = row["Requirement"]
            if req:
                lookup[req] = {
                    "spotlight_pa": row.get("Spotlight PA Category", ""),
                    "priority_level": row.get("Priority Level", ""),
                    "requirement_description": row.get("Requirement Description", "")
                }
        
        print(f"Created lookup dictionary with {len(lookup)} violation codes")
        
        # Track unique missing codes
        missing_codes = set()
        
        # Process each row
        spotlight_pa_list = []
        priority_level_list = []
        risk_level_list = []
        requirement_description_list = []
        
        for idx, row in df.iterrows():
            violation_codes = str(row.get("violation_code", ""))
            original_descriptions = str(row.get("violation_description", ""))
            
            if pd.isna(violation_codes) or violation_codes.strip() == "":
                spotlight_pa_list.append("")
                priority_level_list.append("")
                risk_level_list.append("")
                requirement_description_list.append("")
                continue
                
            # Split by pipe
            codes = [c.strip() for c in violation_codes.split("|")]
            descriptions = [d.strip() for d in original_descriptions.split("|")]
            
            # Pad descriptions list if it's shorter than codes list
            while len(descriptions) < len(codes):
                descriptions.append("")
            
            # Clean each code and look up details
            spotlight_parts = []
            priority_parts = []
            risk_parts = []
            description_parts = []
            
            for i, code in enumerate(codes):
                cleaned_code = clean_violation_code(code)
                original_desc = descriptions[i] if i < len(descriptions) else ""
                
                if cleaned_code in lookup:
                    details = lookup[cleaned_code]
                    spotlight_parts.append(details["spotlight_pa"])
                    priority_parts.append(details["priority_level"])
                    # Translate priority to risk
                    risk_parts.append(translate_priority_to_risk(details["priority_level"]))
                    description_parts.append(details["requirement_description"])
                else:
                    # Track unique missing codes
                    if cleaned_code and cleaned_code not in missing_codes:
                        missing_codes.add(cleaned_code)
                    
                    # Use "NA" for missing values
                    spotlight_parts.append("NA")
                    priority_parts.append("NA")
                    risk_parts.append("NA")
                    description_parts.append(original_desc)
            
            # Join with pipes
            spotlight_pa_list.append(" | ".join(spotlight_parts))
            priority_level_list.append(" | ".join(priority_parts))
            risk_level_list.append(" | ".join(risk_parts))
            requirement_description_list.append(" | ".join(description_parts))
        
        # Report unique missing codes
        if missing_codes:
            print(f"\n{len(missing_codes)} unique violation codes not found in food-codes.csv:")
            print(f"   (Using 'NA' for spotlight_pa/priority_level/risk_level, original description for requirement_description)")
            for code in sorted(missing_codes):
                print(f"   - {code}")
        
        # Add new columns to dataframe
        df["spotlight_pa"] = spotlight_pa_list
        df["priority_level"] = priority_level_list
        df["risk_level"] = risk_level_list
        df["requirement_description"] = requirement_description_list
        
        # Save back to Excel
        df.to_excel(local_inspections_file, index=False)
        print(f"\nAdded violation details columns to {local_inspections_file}")
        
        return True
        
    except Exception as e:
        print(f"Error joining violation details: {e}")
        import traceback
        traceback.print_exc()
        return False