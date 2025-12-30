#!/usr/bin/env python3
"""
Test Naver Review Scraper with 5 Facilities
Quick test to validate scraper functionality
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
    
    # Sample a mix
    test_facilities = []
    
    if len(with_reviews) >= 3:
        test_facilities.append(with_reviews.sample(n=min(3, num), random_state=42))
    else:
        test_facilities.append(with_reviews)
    
    remaining = num - len(test_facilities[0]) if test_facilities else num
    if remaining > 0:
        n_sample = min(remaining, len(without_reviews))
        if n_sample > 0:
            test_facilities.append(without_reviews.sample(n=n_sample, random_state=42))
    
    test_df = pd.concat(test_facilities, ignore_index=True).head(num)
    
    print(f"\n✓ Selected {len(test_df)} facilities for testing")
    
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
        # Navigate and scrape (search is now handled inside)
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


def run_test(num_facilities: int = 5, headless: bool = False):
    """Run the test"""
    
    print("\n" + "="*70)
    print("NAVER MAPS REVIEW SCRAPER - TEST MODE")
    print("="*70)
    
    # Create output directory
    output_dir = Path("./data/test_results")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get test facilities
    test_df = get_test_facilities(num_facilities)
    
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
    print("="*70)
    
    results = []
    
    try:
        for idx, (_, facility) in enumerate(test_df.iterrows(), 1):
            print(f"\n{'#'*70}")
            print(f"TEST {idx}/{len(test_df)}")
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
    
    # Save summary
    summary_file = output_dir / f"summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    summary_df.to_csv(summary_file, index=False, encoding='utf-8-sig')
    print(f"\n💾 Summary: {summary_file}")
    
    print("\n" + "="*70)
    print("✅ TEST COMPLETE")
    print("="*70)
    print(f"Results in: {output_dir}")
    
    return summary_df


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Test scraper with 5 facilities')
    parser.add_argument('--num', type=int, default=5, help='Number of facilities (default: 5)')
    parser.add_argument('--visible', action='store_true', help='Show browser')
    
    args = parser.parse_args()
    
    summary = run_test(
        num_facilities=args.num,
        headless=not args.visible
    )