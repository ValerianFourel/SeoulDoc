from review_scraper import DatasetManager, ReviewScrapingOrchestrator

def scrape_partial(start_idx=0, end_idx=10, checkpoint_freq=5):
    """Scrape a subset of facilities for testing"""
    
    # Load dataset
    dataset_mgr = DatasetManager(cache_dir="./data")
    facilities_df = dataset_mgr.load_dataset()
    
    # Scrape subset
    orchestrator = ReviewScrapingOrchestrator(output_dir="./data")
    reviews_df, failed_df = orchestrator.scrape_all_facilities(
        facilities_df,
        start_idx=start_idx,
        end_idx=end_idx,
        checkpoint_freq=checkpoint_freq,
        headless=True
    )
    
    # Save with different filename
    output_file = f"./data/seoul_medical_reviews_{start_idx}_{end_idx}.parquet"
    if not reviews_df.empty:
        reviews_df.to_parquet(output_file, index=False)
        print(f"\n✓ Saved to: {output_file}")
    
    return reviews_df, failed_df

if __name__ == "__main__":
    # Test with first 10 facilities
    reviews_df, failed_df = scrape_partial(start_idx=0, end_idx=10)