import pandas as pd
import os
import re

# Define File Paths
FACILITIES_PATH = "../../seoul-medical-facilities/seoul_medical_facilities_grouped.parquet"
REVIEWS_PATH = "../../seoul-medical-facilities/seoul_medical_reviews_merged.parquet"

def classify_script(text):
    """
    Classifies text into:
    - 'Hangul': Contains Korean, no English letters.
    - 'Roman': Contains English letters, no Korean.
    - 'Mixed': Contains both Korean and English letters.
    - 'Other': Numbers, emojis, or punctuation only.
    """
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
    # Keep only relevant columns
    df_facilities = df_facilities[['place_id', 'group_name', 'group_code']]

    # 2. Load Reviews Data
    if not os.path.exists(REVIEWS_PATH):
        print(f"Error: Reviews file not found at {REVIEWS_PATH}")
        return

    print("Loading reviews data...")
    df_reviews = pd.read_parquet(REVIEWS_PATH)
    
    # 3. Detect Script Type
    print("Analyzing script types (Hangul/Roman/Mixed)...")
    # Using a simple lambda for speed, though detailed function above is cleaner
    df_reviews['script_type'] = df_reviews['review_text'].apply(classify_script)

    # 4. Join Data
    print("Joining datasets...")
    merged_df = df_reviews.merge(df_facilities, on='place_id', how='left')

    # Fill NaN group names for unmatched IDs
    merged_df['group_name'] = merged_df['group_name'].fillna('Unknown/Unmatched')

    # 5. Generate Breakdown
    print("\n" + "="*60)
    print("      REVIEW SCRIPT BREAKDOWN BY CATEGORY GROUP      ")
    print("="*60)
    
    # Create a Pivot Table
    breakdown = pd.crosstab(
        index=merged_df['group_name'], 
        columns=merged_df['script_type'],
        margins=True,
        margins_name="Total"
    )

    # Reorder columns for readability if they exist
    desired_order = ['Hangul', 'Mixed', 'Roman', 'Other', 'Other/Empty']
    existing_cols = [c for c in desired_order if c in breakdown.columns]
    # Add 'Total' to the end
    if 'Total' in breakdown.columns:
        existing_cols.append('Total')
    
    breakdown = breakdown[existing_cols]

    # Print the table
    print(breakdown)
    print("="*60)
    
    # 6. Specific Focus on Healthcare (A, B, C)
    healthcare_groups = ['Medical Specialty', 'Medical Facility', 'Therapy & Support']
    healthcare_df = merged_df[merged_df['group_name'].isin(healthcare_groups)]
    
    print("\n--- Summary for Healthcare Only (Groups A, B, C) ---")
    hc_counts = healthcare_df['script_type'].value_counts()
    print(hc_counts)
    
    # Calculate percentages for Healthcare
    total_hc = len(healthcare_df)
    if total_hc > 0:
        print(f"\nHealthcare Reviews %:")
        print(f"Hangul Only : {hc_counts.get('Hangul', 0) / total_hc * 100:.1f}%")
        print(f"Mixed       : {hc_counts.get('Mixed', 0) / total_hc * 100:.1f}%")
        print(f"Roman Only  : {hc_counts.get('Roman', 0) / total_hc * 100:.1f}%")

if __name__ == "__main__":
    main()
