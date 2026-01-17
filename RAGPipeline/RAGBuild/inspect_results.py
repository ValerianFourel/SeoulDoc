import pandas as pd
import os

# --- Configuration ---
# Paths based on your previous messages
FILE_1 = "../../../seoul-medical-facilities/seoul_medical_facilities_enriched_multigpu.parquet"
FILE_2 = "../../../seoul-medical-facilities/seoul_medical_rag_knowledge.parquet"

def inspect_parquet(file_path):
    print(f"\n{'='*80}")
    print(f"📂 FILE: {file_path}")
    print(f"{'='*80}")

    if not os.path.exists(file_path):
        print(f"❌ Error: File not found at {file_path}")
        return

    try:
        # Load the dataframe
        df = pd.read_parquet(file_path)
        
        # Print basic info
        print(f"✅ Loaded successfully!")
        print(f"   Rows: {len(df):,}")
        print(f"   Columns: {len(df.columns)}")
        print(f"   Column Names: {list(df.columns)}")
        
        # Show first 5 rows
        print(f"\n--- HEAD (First 5 Rows) ---")
        # We use to_string() to ensure all columns are visible if possible, 
        # or just standard print which pandas formats nicely
        print(df.head(5).to_string())
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")

def main():
    # Inspect File 1 (Enriched Facilities with English Scores)
    inspect_parquet(FILE_1)

    # Inspect File 2 (RAG Knowledge / Summaries)
    inspect_parquet(FILE_2)

if __name__ == "__main__":
    main()
