#!/usr/bin/env python3
"""
Test Naver Review Scraper with 5 Facilities
Quick test to validate scraper functionality
Searches by name and matches place_id from parquet
"""

import pandas as pd
from pathlib import Path
import json
from datetime import datetime
import sys
import os

# Import the main scraper
sys.path.insert(0, os.path.dirname(__file__))
from naver_review_scraper import NaverMapsReviewScraper, load_facilities_dataset


def get_test_facilities(num: int = 5) -> pd.DataFrame:
    """Get test facilities from dataset"""
    
    print("="*70)
    print(f"LOADING {num} TEST FACILITIES")
    print("="*70)
    
    # Load full dataset
    df = load_facilities_dataset(source="local")
    
    # Clean data
    df = df[df['place_id'].notna() & df['name'].notna()].copy()
    
    # Prefer facilities with some existing review data (just for info)
    # But we'll scrape fresh reviews regardless
    with_reviews = df[df['reviews'].notna() & (df['reviews'] != '')]
    without_reviews = df[df['reviews'].isna() | (df['reviews'] == '')]
    
    print(f"\nDataset stats:")
    print(f"  Total facilities: {len(df):,}")
    print(f"  With review data: {len(with_reviews):,}")
    print(f"  Without review data: {len(without_reviews):,}")
    
    # Sample a mix - prefer facilities with existing review data
    test_facilities = []
    
    # Try to get mix: 60% with reviews, 40% without
    with_review_count = min(int(num * 0.6), len(with_reviews))
    without_review_count = num - with_review_count
    
    if with_review_count > 0 and len(with_reviews) > 0:
        test_facilities.append(with_reviews.sample(n=with_review_count, random_state=42))
    
    if without_review_count > 0 and len(without_reviews) > 0:
        n_sample = min(without_review_count, len(without_reviews))
        test_facilities.append(without_reviews.sample(n=n_sample, random_state=42))
    
    if test_facilities:
        test_df = pd.concat(test_facilities, ignore_index=True).head(num)
    else:
        # Fallback: just sample randomly
        test_df = df.sample(n=min(num, len(df)), random_state=42)
    
    print(f"\n✓ Selected {len(test_df)} facilities for testing")
    print(f"  With existing review data: {sum(test_df['place_id'].isin(with_reviews['place_id']))}")
    print(f"  Without review data: {sum(test_df['place_id'].isin(without_reviews['place_id']))}")
    
    return test_df


def test_facility(scraper: NaverMapsReviewScraper, 
                  facility: pd.Series,
                  output_dir: Path) -> dict:
    """Test scraping for a single facility"""
    
    place_id = str(facility['place_id'])
    facility_name = facility['name']
    
    print(f"\n{'='*70}")
    print(f"FACILITY: {facility_name}")
    print(f"{'='*70}")
    print(f"Place ID: {place_id}")
    print(f"Category: {facility.get('category', 'N/A')}")
    print(f"Address: {facility.get('address', 'N/A')[:50]}...")
    
    try:
        # Navigate and scrape (search by name, match place_id)
        review_data = scraper.scrape_reviews_for_facility(facility_name, place_id)
        
        # Print results
        print(f"\n📊 RESULTS:")
        print(f"   Has reviews: {review_data['has_reviews']}")
        print(f"   Review count: {review_data['review_count']}")
        
        if review_data.get('scrape_error'):
            print(f"   ⚠ Error: {review_data['scrape_error']}")
        
        if review_data['has_reviews'] and review_data['reviews']:
            sample = review_data['reviews'][0]
            print(f"\n   📝 First review:")
            print(f"      Reviewer: {sample.get('reviewer_info', {}).get('reviewer_name', 'N/A')}")
            review_text = sample.get('review_text', '')[:100]
            print(f"      Text: {review_text}...")
            print(f"      Visit date: {sample.get('visit_info', {}).get('visit_date', 'N/A')}")
            print(f"      Images: {len(sample.get('images', []))}")
        
        # Save result
        output_file = output_dir / f"test_{place_id}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'place_id': place_id,
                'facility_name': facility_name,
                'review_data': review_data
            }, f, ensure_ascii=False, indent=2)
        
        print(f"\n   💾 Saved: {output_file.name}")
        
        return {
            'place_id': place_id,
            'facility_name': facility_name,
            'has_reviews': review_data['has_reviews'],
            'review_count': review_data['review_count'],
            'error': review_data.get('scrape_error')
        }
        
    except Exception as e:
        print(f"\n   ✗ ERROR: {e}")
        return {
            'place_id': place_id,
            'facility_name': facility_name,
            'has_reviews': False,
            'review_count': 0,
            'error': str(e)
        }


def run_test(num_facilities: int = 10, headless: bool = False, 
             partition_x: int = 1, partition_y: int = 1):
    """
    Run the test
    
    Args:
        num_facilities: Number of facilities to test
        headless: Run in headless mode
        partition_x: Which partition to test (1 to partition_y)
        partition_y: Total number of partitions
    """
    
    print("\n" + "="*70)
    print("NAVER MAPS REVIEW SCRAPER - TEST MODE")
    if partition_y > 1:
        print(f"PARTITION {partition_x}/{partition_y}")
    print("="*70)
    
    # Create output directory
    output_dir = Path("./data/test_results")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get test facilities
    test_df = get_test_facilities(num_facilities)
    
    # Apply partitioning if requested
    if partition_y > 1:
        test_df = test_df.reset_index(drop=True)
        partition_indices = list(range(partition_x - 1, len(test_df), partition_y))
        test_df = test_df.iloc[partition_indices].copy()
        
        print(f"\n{'='*70}")
        print(f"PARTITION FILTERING")
        print(f"{'='*70}")
        print(f"Partition {partition_x} of {partition_y}")
        print(f"Testing {len(test_df)} facilities from this partition")
        print(f"Pattern: Every {partition_y}th facility starting from position {partition_x}")
        print(f"{'='*70}\n")
    
    # Initialize scraper
    print("\n" + "="*70)
    print("INITIALIZING SCRAPER")
    print("="*70)
    print(f"Headless: {headless}")
    
    scraper = NaverMapsReviewScraper(headless=headless)
    scraper.setup_driver()
    print("✓ Scraper ready")
    
    # Test each facility
    print("\n" + "="*70)
    print(f"TESTING {len(test_df)} FACILITIES")
    if partition_y > 1:
        print(f"PARTITION {partition_x}/{partition_y}")
    print("="*70)
    
    results = []
    
    try:
        for idx, (_, facility) in enumerate(test_df.iterrows(), 1):
            print(f"\n{'#'*70}")
            print(f"TEST {idx}/{len(test_df)}")
            if partition_y > 1:
                print(f"PARTITION {partition_x}/{partition_y}")
            print(f"{'#'*70}")
            
            result = test_facility(scraper, facility, output_dir)
            results.append(result)
            
            # Wait between tests
            if idx < len(test_df):
                print("\n⏳ Waiting 3 seconds...")
                import time
                time.sleep(3)
    
    finally:
        print("\n" + "="*70)
        print("CLOSING BROWSER")
        print("="*70)
        scraper.close_driver()
        print("✓ Browser closed")
    
    # Print summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    if partition_y > 1:
        print(f"PARTITION {partition_x}/{partition_y}")
    print("="*70)
    
    summary_df = pd.DataFrame(results)
    
    total = len(summary_df)
    with_reviews = summary_df['has_reviews'].sum()
    total_reviews = summary_df['review_count'].sum()
    errors = summary_df['error'].notna().sum()
    
    print(f"\nTotal tested: {total}")
    print(f"With reviews: {with_reviews}")
    print(f"Total reviews: {total_reviews}")
    print(f"Errors: {errors}")
    
    print("\n" + summary_df.to_string(index=False))
    
    # Save summary with partition suffix
    partition_suffix = f"_p{partition_x}_of_{partition_y}" if partition_y > 1 else ""
    summary_file = output_dir / f"summary{partition_suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    summary_df.to_csv(summary_file, index=False, encoding='utf-8-sig')
    print(f"\n💾 Summary: {summary_file}")
    
    print("\n" + "="*70)
    print("✅ TEST COMPLETE")
    if partition_y > 1:
        print(f"   PARTITION {partition_x}/{partition_y}")
    print("="*70)
    print(f"Results in: {output_dir}")
    
    return summary_df


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Test scraper with facilities')
    parser.add_argument('--num', type=int, default=10, help='Number of facilities (default: 10)')
    parser.add_argument('--visible', action='store_true', help='Show browser')
    parser.add_argument(
        '--partition-x',
        type=int,
        default=1,
        help='Which partition to test (1 to partition-y)'
    )
    parser.add_argument(
        '--partition-y',
        type=int,
        default=1,
        help='Total number of partitions (default: 1 = test all)'
    )
    
    args = parser.parse_args()
    
    summary = run_test(
        num_facilities=args.num,
        headless=not args.visible,
        partition_x=args.partition_x,
        partition_y=args.partition_y
    )