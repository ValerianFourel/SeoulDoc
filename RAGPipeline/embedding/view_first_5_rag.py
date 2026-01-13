"""
Seoul Medical Facilities - View First 5 RAG Entries
===================================================
Display the first 5 entries from RAG knowledge file
"""

import pandas as pd
import json

RAG_FILE = "../../../seoul-medical-facilities/seoul_medical_rag_knowledge.parquet"

# Load data
print("Loading RAG knowledge...")
df = pd.read_parquet(RAG_FILE)
print(f"Total records: {len(df):,}\n")

# Display first 5
for i in range(min(5, len(df))):
    print("="*80)
    print(f"ENTRY #{i+1}")
    print("="*80)
    
    row = df.iloc[i]
    
    for col in df.columns:
        value = row[col]
        
        # Parse JSON strings
        if isinstance(value, str) and (value.startswith('[') or value.startswith('{')):
            try:
                value = json.loads(value)
            except:
                pass
        
        # Pretty print
        if isinstance(value, list):
            print(f"\n{col}:")
            for item in value:
                print(f"  • {item}")
        elif isinstance(value, dict):
            print(f"\n{col}:")
            for k, v in value.items():
                print(f"  {k}: {v}")
        else:
            print(f"\n{col}: {value}")
    
    print("\n")

