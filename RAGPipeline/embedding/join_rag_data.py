import pandas as pd
import os

# --- CONFIGURATION ---
RAG_KNOWLEDGE_PATH = "../../../seoul-medical-facilities/seoul_medical_rag_knowledge.parquet"
FACILITIES_ENRICHED_PATH = "../../../seoul-medical-facilities/seoul_medical_facilities_enriched_multigpu.parquet"
OUTPUT_PATH = "../../../seoul-medical-facilities/facilities_metareviews_rag_ready.parquet"

# Columns to explicitly remove
DROP_COLS = [
    'medical_info_raw', 
    'parsing_success', 
    'enrichment_error', 
    'enriched_at', 
    'verified_place_id', 
    'error', 
    'scraped_at_dt','page_number'
]

def main():
    print(f"Loading datasets...")
    
    # 1. Load Data
    if not os.path.exists(RAG_KNOWLEDGE_PATH) or not os.path.exists(FACILITIES_ENRICHED_PATH):
        print("❌ Error: One or more input files not found.")
        return

    df_rag = pd.read_parquet(RAG_KNOWLEDGE_PATH)
    df_fac = pd.read_parquet(FACILITIES_ENRICHED_PATH)
    
    print(f"   RAG Data: {len(df_rag):,} rows")
    print(f"   Facilities Data: {len(df_fac):,} rows")

    # 2. Drop unwanted columns from Facilities
    print(f"Cleaning columns...")
    df_fac = df_fac.drop(columns=DROP_COLS, errors='ignore')

    # 3. Perform Join
    # We use df_fac as the LEFT base to keep all facilities (equivalent to right join on rag)
    # RAG file uses 'Facility' as the key, Facilities file uses 'place_id'
    print(f"Merging data (Left Join on Facilities)...")
    
    df_merged = df_fac.merge(
        df_rag, 
        left_on='place_id', 
        right_on='Facility', 
        how='left'
    )

    # Optional: Drop the duplicate key column 'Facility' from RAG if desired, 
    # but keeping it is fine for verification.
    
    # 4. Save
    print(f"Saving to {OUTPUT_PATH}...")
    df_merged.to_parquet(OUTPUT_PATH)
    
    print(f"\n✅ Success!")
    print(f"   Final Shape: {df_merged.shape}")
    print(f"   Facilities with RAG Summaries: {df_merged['Summaries'].notna().sum():,}")
    print(f"   Facilities with English Scores: {df_merged['english_confidence_score'].notna().sum():,}")

if __name__ == "__main__":
    main()
