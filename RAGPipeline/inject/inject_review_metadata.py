import pandas as pd
import os
import re

# --- CONFIGURATION ---
FACILITIES_PATH = "../../seoul-medical-facilities/seoul_medical_facilities_grouped.parquet"
REVIEWS_INPUT_PATH = "../../seoul-medical-facilities/seoul_medical_reviews_merged.parquet"
REVIEWS_OUTPUT_PATH = "../../seoul-medical-facilities/seoul_medical_reviews_enriched.parquet"

def classify_script(text):
    """
    Classifies text into:
    - 'Hangul': Contains Korean, no English letters.
    - 'Roman': Contains English letters, no Korean.
    - 'Mixed': Contains both Korean and English letters.
    - 'Other': Numbers, emojis, or punctuation only.
    """
    # Handle non-string or empty inputs safely
    if not isinstance(text, str) or not text.strip():
        return 'Other/Empty'

    # Regex for Hangul (Syllables and Jamo)
    has_hangul = bool(re.search(r'[가-힣ㄱ-ㅎㅏ-ㅣ]', text))
    # Regex for Roman alphabets
    has_roman = bool(re.search(r'[a-zA-Z]', text))

    if has_hangul and has_roman:
        return 'Mixed'
    elif has_hangul:
        return 'Hangul'
    elif has_roman:
        return 'Roman'
    else:
        return 'Other'

def main():
    # 1. Load Facilities Data
    if not os.path.exists(FACILITIES_PATH):
        print(f"Error: Facilities file not found at {FACILITIES_PATH}")
        return
    
    print("Loading facilities data...")
    df_facilities = pd.read_parquet(FACILITIES_PATH)
    
    # Select only the columns we want to inject
    # We rename them slightly to avoid confusion if needed, or keep as is
    cols_to_merge = df_facilities[['place_id', 'group_name', 'group_code', 'is_healthcare']]

    # 2. Load Reviews Data
    if not os.path.exists(REVIEWS_INPUT_PATH):
        print(f"Error: Reviews file not found at {REVIEWS_INPUT_PATH}")
        return

    print(f"Loading reviews data from {REVIEWS_INPUT_PATH}...")
    df_reviews = pd.read_parquet(REVIEWS_INPUT_PATH)
    original_count = len(df_reviews)

    # 3. Inject Script Type (Language Analysis)
    print("Injecting 'script_type' label (Hangul/Roman/Mixed)...")
    df_reviews['script_type'] = df_reviews['review_text'].apply(classify_script)

    # 4. Inject Group Info (Join with Facilities)
    print("Injecting facility metadata (Groups A-E)...")
    # Left join ensures we keep all reviews even if facility info is missing
    df_enriched = df_reviews.merge(cols_to_merge, on='place_id', how='left')

    # Fill NaNs for reviews where place_id didn't match a facility
    df_enriched['group_name'] = df_enriched['group_name'].fillna('Unknown')
    df_enriched['group_code'] = df_enriched['group_code'].fillna('U') # U for Unknown
    df_enriched['is_healthcare'] = df_enriched['is_healthcare'].fillna(False)

    # 5. Validation
    print("-" * 40)
    print("Validation Check:")
    print(f"Original Rows: {original_count}")
    print(f"Enriched Rows: {len(df_enriched)}")
    if original_count != len(df_enriched):
        print("WARNING: Row count mismatch! Check for duplicates in place_id.")
    else:
        print("Row count matches. Merge successful.")
    
    print("\nSample of injected columns:")
    print(df_enriched[['review_text', 'script_type', 'group_name']].head(3))
    print("-" * 40)

    # 6. Save to new file
    print(f"Saving enriched dataset to {REVIEWS_OUTPUT_PATH}...")
    df_enriched.to_parquet(REVIEWS_OUTPUT_PATH)
    print("Done. Success!")

if __name__ == "__main__":
    main()
