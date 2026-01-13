"""
Inspect Saved Prompts and Metadata
==================================
Quick inspection of pickle files
"""

import pickle
import json

# ==========================================
# FILE PATHS
# ==========================================
PROMPTS_FILE = "../../../seoul-medical-facilities/seoul_medical_facility_prompts.pkl"
METADATA_FILE = "../../../seoul-medical-facilities/seoul_medical_facility_metadata.pkl"

def main():
    print("="*70)
    print("Inspecting Saved Prompts and Metadata")
    print("="*70)
    
    # ---------------------------------------------------------
    # LOAD FILES
    # ---------------------------------------------------------
    print(f"\n📂 Loading files...")
    
    with open(PROMPTS_FILE, 'rb') as f:
        facility_prompts = pickle.load(f)
    
    with open(METADATA_FILE, 'rb') as f:
        facility_metadata = pickle.load(f)
    
    print(f"   ✓ Loaded {len(facility_prompts):,} prompts")
    print(f"   ✓ Loaded {len(facility_metadata):,} metadata entries")
    
    # ---------------------------------------------------------
    # PROMPTS INSPECTION
    # ---------------------------------------------------------
    print(f"\n" + "="*70)
    print("📄 PROMPTS STRUCTURE")
    print("="*70)
    
    print(f"\nTotal Prompts: {len(facility_prompts):,}")
    print(f"Type: {type(facility_prompts)}")
    
    # Show first prompt (truncated)
    print(f"\n--- EXAMPLE PROMPT #1 (First 1500 chars) ---")
    print(facility_prompts[0][:1500])
    print("...")
    print(f"[Full length: {len(facility_prompts[0])} characters]")
    
    # Show prompt lengths distribution
    prompt_lengths = [len(p) for p in facility_prompts]
    print(f"\n📊 Prompt Length Statistics:")
    print(f"   Min: {min(prompt_lengths):,} chars")
    print(f"   Max: {max(prompt_lengths):,} chars")
    print(f"   Mean: {sum(prompt_lengths)/len(prompt_lengths):,.0f} chars")
    print(f"   Median: {sorted(prompt_lengths)[len(prompt_lengths)//2]:,} chars")
    
    # ---------------------------------------------------------
    # METADATA INSPECTION
    # ---------------------------------------------------------
    print(f"\n" + "="*70)
    print("📋 METADATA STRUCTURE")
    print("="*70)
    
    print(f"\nTotal Metadata Entries: {len(facility_metadata):,}")
    print(f"Type: {type(facility_metadata)}")
    
    # Show first 5 metadata entries
    print(f"\n--- FIRST 5 METADATA ENTRIES ---")
    for i, meta in enumerate(facility_metadata[:5], 1):
        print(f"\n{i}. Facility: {meta['place_id']}")
        print(f"   Reviews: {meta['n_reviews']:,}")
        print(f"   Summaries: {meta['n_summaries']}")
        print(f"   Highlights: {meta['n_highlights']}")
        print(f"   Min Relevance: {meta['min_relevance']:.1%}")
    
    # Metadata statistics
    print(f"\n📊 Metadata Statistics:")
    
    n_reviews_list = [m['n_reviews'] for m in facility_metadata]
    n_summaries_list = [m['n_summaries'] for m in facility_metadata]
    n_highlights_list = [m['n_highlights'] for m in facility_metadata]
    
    print(f"\n   Reviews per Facility:")
    print(f"     Min: {min(n_reviews_list)}")
    print(f"     Max: {max(n_reviews_list)}")
    print(f"     Mean: {sum(n_reviews_list)/len(n_reviews_list):.1f}")
    
    print(f"\n   Summaries per Facility:")
    print(f"     Min: {min(n_summaries_list)}")
    print(f"     Max: {max(n_summaries_list)}")
    print(f"     Mean: {sum(n_summaries_list)/len(n_summaries_list):.1f}")
    
    print(f"\n   Highlights per Facility:")
    print(f"     Min: {min(n_highlights_list)}")
    print(f"     Max: {max(n_highlights_list)}")
    print(f"     Mean: {sum(n_highlights_list)/len(n_highlights_list):.1f}")
    
    # Relevance threshold distribution
    from collections import Counter
    relevance_dist = Counter(m['min_relevance'] for m in facility_metadata)
    
    print(f"\n   Relevance Threshold Distribution:")
    for threshold in sorted(relevance_dist.keys(), reverse=True):
        count = relevance_dist[threshold]
        print(f"     {threshold:.0%}: {count:,} facilities ({count/len(facility_metadata)*100:.1f}%)")
    
    # ---------------------------------------------------------
    # VALIDATION
    # ---------------------------------------------------------
    print(f"\n" + "="*70)
    print("✅ VALIDATION")
    print("="*70)
    
    assert len(facility_prompts) == len(facility_metadata), "Prompts and metadata count mismatch!"
    print(f"   ✓ Prompts and metadata counts match: {len(facility_prompts):,}")
    
    # Check if place_ids are extractable from prompts
    place_id_from_meta = facility_metadata[0]['place_id']
    place_id_in_prompt = place_id_from_meta in facility_prompts[0]
    print(f"   ✓ Place ID from metadata found in prompt: {place_id_in_prompt}")
    
    # Check prompt structure
    has_facility = "FACILITY:" in facility_prompts[0]
    has_topics = "ALL IDENTIFIED TOPICS" in facility_prompts[0]
    has_task = "TASK:" in facility_prompts[0]
    has_output_format = "OUTPUT FORMAT" in facility_prompts[0]
    
    print(f"\n   Prompt Structure Check:")
    print(f"     Has FACILITY section: {has_facility}")
    print(f"     Has TOPICS section: {has_topics}")
    print(f"     Has TASK section: {has_task}")
    print(f"     Has OUTPUT FORMAT: {has_output_format}")
    
    if all([has_facility, has_topics, has_task, has_output_format]):
        print(f"\n   ✅ All prompts appear to have correct structure!")
    else:
        print(f"\n   ⚠️ Warning: Some prompts may be missing sections!")
    
    print(f"\n" + "="*70)
    print("✅ Inspection Complete!")
    print("="*70)
    print(f"\nReady to run clustering_part5.py for summary generation")

if __name__ == "__main__":
    main()
