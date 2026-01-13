"""
Seoul Medical Facilities - RAG Annotation Checker
=================================================
Check facilities with RAG knowledge by index number
"""

import pandas as pd
import json
from datetime import datetime
import os
import sys

# ==========================================
# CONFIGURATION
# ==========================================
ENRICHED_FILE = "../../../seoul-medical-facilities/seoul_medical_facilities_enriched.parquet"
RAG_KNOWLEDGE_FILE = "../../../seoul-medical-facilities/seoul_medical_rag_knowledge.parquet"

# ==========================================
# MAIN FUNCTION
# ==========================================
def main():
    print("="*80)
    print("  SEOUL MEDICAL FACILITIES - RAG CHECKER")
    print("="*80)
    
    # ---------------------------------------------------------
    # LOAD DATA
    # ---------------------------------------------------------
    print("\n📂 Loading data...")
    
    if not os.path.exists(RAG_KNOWLEDGE_FILE):
        print(f"❌ Error: RAG knowledge file not found: {RAG_KNOWLEDGE_FILE}")
        return
    
    if not os.path.exists(ENRICHED_FILE):
        print(f"❌ Error: Enriched file not found: {ENRICHED_FILE}")
        return
    
    rag_df = pd.read_parquet(RAG_KNOWLEDGE_FILE)
    enriched_df = pd.read_parquet(ENRICHED_FILE)
    
    # Filter to only facilities with RAG
    rag_place_ids = set(rag_df['Facility'].values)
    filtered_df = enriched_df[enriched_df['place_id'].isin(rag_place_ids)].reset_index(drop=True)
    
    print(f"   ✓ Loaded {len(rag_df):,} RAG records")
    print(f"   ✓ Filtered to {len(filtered_df):,} facilities with RAG\n")
    
    # ---------------------------------------------------------
    # INTERACTIVE LOOP
    # ---------------------------------------------------------
    print("="*80)
    print("  Enter index number (0-{}) or 'q' to quit".format(len(filtered_df)-1))
    print("="*80)
    
    while True:
        user_input = input("\nIndex> ").strip().lower()
        
        if user_input == 'q' or user_input == 'quit':
            print("\n👋 Goodbye!")
            break
        
        if not user_input.isdigit():
            continue
            
        idx = int(user_input)
        if idx < 0 or idx >= len(filtered_df):
            print(f"⚠️  Index out of range (0-{len(filtered_df)-1})")
            continue
        
        # Get facility
        row = filtered_df.iloc[idx]
        place_id = row['place_id']
        
        print(f"\n{'='*80}")
        print(f"  FACILITY #{idx}")
        print(f"{'='*80}")
        
        # Basic info
        print(f"\nName: {row['name']}")
        print(f"Category: {row['category']}")
        print(f"Address: {row['address']}")
        print(f"Phone: {row.get('phone', 'N/A')}")
        print(f"Reviews: {row.get('reviews', 'N/A')}")
        print(f"District: {row.get('file_district', 'N/A')}")
        
        # Medical info
        print(f"\n{'='*80}")
        print("  ENRICHED MEDICAL INFO")
        print(f"{'='*80}")
        
        if not row.get('has_medical_info') or not row.get('parsing_success'):
            print("\n❌ No enriched medical information available\n")
        else:
            medical_info = row.get('medical_info_parsed')
            
            # Check if medical_info is None or scalar NaN
            if medical_info is None or (isinstance(medical_info, float) and pd.isna(medical_info)):
                print("\n❌ No enriched medical information available\n")
            else:
                # Parse JSON if string
                if isinstance(medical_info, str):
                    try:
                        medical_info = json.loads(medical_info)
                    except:
                        print("\n❌ No enriched medical information available\n")
                        medical_info = None
                
                if medical_info and isinstance(medical_info, dict):
                    for key, value in medical_info.items():
                        # Handle numpy arrays and other array-like objects
                        import numpy as np
                        
                        # Skip if None or scalar NaN
                        if value is None:
                            continue
                        if isinstance(value, float) and pd.isna(value):
                            continue
                        
                        # Handle arrays/lists
                        if isinstance(value, (list, tuple, np.ndarray)):
                            if len(value) == 0:
                                continue
                            print(f"\n{key}:")
                            for item in value:
                                print(f"  • {item}")
                        # Handle strings
                        elif isinstance(value, str):
                            if value:
                                print(f"\n{key}: {value}")
                        # Handle other values
                        else:
                            print(f"\n{key}: {value}")
                    print()
        
        # RAG knowledge
        print(f"{'='*80}")
        print("  RAG GENERATED KNOWLEDGE")
        print(f"{'='*80}")
        
        rag_record = rag_df[rag_df['Facility'] == place_id].iloc[0]
        
        # Total reviews
        total_reviews = rag_record.get('Total_Reviews')
        if total_reviews is not None and not (isinstance(total_reviews, float) and pd.isna(total_reviews)):
            print(f"\nTotal Reviews Analyzed: {total_reviews}")
        
        # Get highlights and summaries
        highlights = rag_record.get('Key_Highlights')
        summaries = rag_record.get('Summaries')
        
        # Parse if needed
        if highlights is not None and not (isinstance(highlights, float) and pd.isna(highlights)):
            if isinstance(highlights, str):
                try:
                    highlights = json.loads(highlights)
                except:
                    highlights = None
        
        if summaries is not None and not (isinstance(summaries, float) and pd.isna(summaries)):
            if isinstance(summaries, str):
                try:
                    summaries = json.loads(summaries)
                except:
                    summaries = None
        
        # Display highlights with their summaries
        if isinstance(highlights, list) and isinstance(summaries, list) and len(highlights) > 0:
            print(f"\n{'='*80}")
            print(f"  KEY INSIGHTS ({len(highlights)} topics)")
            print(f"{'='*80}\n")
            
            for i, (highlight, summary) in enumerate(zip(highlights, summaries), 1):
                if isinstance(highlight, dict):
                    topic_en = highlight.get('topic_en', 'N/A')
                    topic_ko = highlight.get('topic_ko', 'N/A')
                    relevance = highlight.get('relevance', 0)
                    
                    print(f"[{i}] {topic_en}")
                    print(f"    한국어: {topic_ko}")
                    print(f"    Relevance: {relevance:.3f}")
                    print(f"    Summary: {summary}")
                    print()
                else:
                    print(f"[{i}] {highlight}")
                    print(f"    Summary: {summary}")
                    print()
        elif isinstance(summaries, list) and len(summaries) > 0:
            # Fallback if no highlights but have summaries
            print(f"\nSummaries ({len(summaries)}):")
            for i, summary in enumerate(summaries, 1):
                print(f"  {i}. {summary}")
            print()
        
        # Display FULL RAG record
        print(f"{'='*80}")
        print("  FULL RAG RECORD (ALL FIELDS)")
        print(f"{'='*80}\n")
        
        for col in rag_df.columns:
            value = rag_record[col]
            
            # Skip if None or scalar NaN
            if value is None or (isinstance(value, float) and pd.isna(value)):
                continue
            
            # Parse JSON strings
            if isinstance(value, str) and (value.startswith('[') or value.startswith('{')):
                try:
                    parsed_value = json.loads(value)
                    print(f"{col}:")
                    print(json.dumps(parsed_value, indent=2, ensure_ascii=False))
                    print()
                    continue
                except:
                    pass
            
            # Display based on type
            if isinstance(value, (list, tuple)):
                print(f"{col} ({len(value)} items):")
                print(json.dumps(list(value), indent=2, ensure_ascii=False))
                print()
            elif isinstance(value, dict):
                print(f"{col}:")
                print(json.dumps(value, indent=2, ensure_ascii=False))
                print()
            else:
                print(f"{col}: {value}")
                print()
        
        print(f"{'='*80}")
        print(f"URL: {row['url']}")
        print(f"{'='*80}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Interrupted by user. Goodbye!")
        sys.exit(0)
