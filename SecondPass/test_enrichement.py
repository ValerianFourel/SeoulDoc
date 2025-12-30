#!/usr/bin/env python3
"""Test script for enrichment"""

import os
from enrich_medical_info import DatasetManager, EnrichmentOrchestrator, export_parsed_data_to_json

def test_enrichment(num_facilities=3):
    """Test enrichment on first N facilities"""
    
    groq_api_key = os.getenv("GROQ_API_KEY")
    
    if not groq_api_key:
        print("❌ Error: GROQ_API_KEY not set")
        return
    
    # Load dataset
    dataset_mgr = DatasetManager(cache_dir="./data")
    facilities_df = dataset_mgr.load_dataset()
    
    # Enrich first N facilities
    orchestrator = EnrichmentOrchestrator(
        output_dir="./data",
        groq_api_key=groq_api_key
    )
    
    enriched_df, failed_df = orchestrator.enrich_all_facilities(
        facilities_df,
        start_idx=0,
        end_idx=num_facilities,
        checkpoint_freq=2,
        headless=False  # Not headless for debugging
    )
    
    # Save test results
    enriched_df.to_parquet("./data/test_enriched.parquet", index=False)
    
    # Print summary
    orchestrator.print_summary(enriched_df)
    
    # Export parsed data
    export_parsed_data_to_json(enriched_df, "./data/test_llm_parsed")
    
    print(f"\n✓ Test complete! Check ./data/test_enriched.parquet")
    
    return enriched_df

if __name__ == "__main__":
    test_enrichment(num_facilities=3)
