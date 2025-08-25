CATEGORIES = [
    "Pizza",
    "Cafe",
    "Bakery",
    "Dessert",
    "Pub",
    "Deli",
    "Fast Food",
    "Restaurant",
    "Mobile",
    "Venue Dining",
    "Other",
]
def normalize_strict(cat: str) -> str:
    return cat if cat in CATEGORIES else "Other"

CUISINE_CATEGORIES = [
    "Mexican",
    "Chinese",
    "Japanese",
    "Thai",
    "Indian",
    "Mediterranean",
    "Greek",
    "Middle Eastern",
    "Korean",
    "Vietnamese",
    "Italian",
    "BBQ",
    "Seafood",
    "American",
    "Caribbean",
    "Latin American",
    "Other",
]
def normalize_cuisine(cat: str) -> str:
    return cat if cat in CUISINE_CATEGORIES else "Other"

AI_PROMPT_HEADER = """
You classify Pennsylvania restaurant/food-establishment inspections.

Return EXACTLY:
- strict_category: ONE value from this list:
{allowed_categories}
- cuisine: ONE value from this list:
{allowed_cuisines}
- ai_category: a short free-text label (1–5 words) to help search (e.g., "neapolitan pizza", "boba tea cafe").
- confidence: 0–1
- rationale: one brief sentence citing evidence used.

Rules:
1) Use ONLY the establishment fields and evidence provided. Do not invent details.
2) Prefer specific strict_category over general; if unclear, use "Other".
3) If cuisine is unclear, use "Other".
4) Output JSON ONLY with keys: "strict_category", "cuisine", "ai_category", "confidence", "rationale".
"""

def render_ai_prompt(facility: str, address: str, city: str, evidence_text: str | None = None) -> str:
    allowed_cats = "\n".join(f"- {c}" for c in CATEGORIES)
    allowed_cuis = "\n".join(f"- {c}" for c in CUISINE_CATEGORIES)
    ev = f"\nEVIDENCE:\n{evidence_text.strip()}\n" if evidence_text else ""
    return (
        AI_PROMPT_HEADER.format(
            allowed_categories=allowed_cats,
            allowed_cuisines=allowed_cuis,
        )
        + "\nEstablishment:\n"
        + f"- Facility: {facility}\n- Address: {address}\n- City: {city}\n"
        + ev
        + "\nReturn ONLY JSON, no markdown."
    )
