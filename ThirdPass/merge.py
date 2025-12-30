#!/usr/bin/env python3
"""
Merge Worker JSON Files
Combines all worker JSON files into a single dataset
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime


def merge_worker_jsons(worker_dir: str = "./data/worker_results"):
    """Merge all worker JSON files"""
    
    worker_dir = Path(worker_dir)
    
    print("="*70)
    print("MERGING WORKER JSON FILES")
    print("="*70)
    
    if not worker_dir.exists():
        print(f"\n✗ Directory not found: {worker_dir}")
        print("  No worker results to merge")
        return
    
    # Find all JSON files
    worker_files = list(worker_dir.glob('*.json'))
    
    if not worker_files:
        print(f"\n✗ No JSON files found in: {worker_dir}")
        return
    
    print(f"\n✓ Found {len(worker_files)} worker files:")
    for f in worker_files:
        size_kb = f.stat().st_size / 1024
        print(f"  • {f.name} ({size_kb:.1f} KB)")
    
    # Merge all data
    print(f"\n{'='*70}")
    print("MERGING DATA")
    print(f"{'='*70}\n")
    
    all_results = {}
    total_facilities = 0
    total_reviews = 0
    
    for worker_file in worker_files:
        try:
            print(f"Reading: {worker_file.name}")
            with open(worker_file, 'r', encoding='utf-8') as f:
                worker_data = json.load(f)
            
            facilities = len(worker_data)
            reviews = sum(r.get('review_count', 0) for r in worker_data.values())
            
            print(f"  ✓ {facilities} facilities, {reviews} reviews")
            
            all_results.update(worker_data)
            total_facilities += facilities
            total_reviews += reviews
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
    
    print(f"\n{'─'*70}")
    print(f"Total unique facilities: {len(all_results)}")
    print(f"Total reviews: {total_reviews}")
    print(f"{'─'*70}\n")
    
    # Save merged JSON
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = worker_dir.parent
    
    merged_json = output_dir / f"reviews_merged_{timestamp}.json"
    print(f"Saving merged JSON...")
    with open(merged_json, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    print(f"✓ {merged_json}")
    
    # Convert to DataFrame
    print(f"\nConverting to DataFrame...")
    records = []
    
    for place_id, review_data in all_results.items():
        if review_data.get('has_reviews') and review_data.get('reviews'):
            # Facility with reviews
            for review in review_data['reviews']:
                record = {
                    'place_id': place_id,
                    'facility_name': review_data.get('facility_name', ''),
                    'review_index': review.get('review_index'),
                    'reviewer_name': review.get('reviewer_info', {}).get('reviewer_name'),
                    'review_text': review.get('review_text'),
                    'visit_date': review.get('visit_info', {}).get('visit_date'),
                    'visit_count': review.get('visit_info', {}).get('visit_count'),
                    'verification_method': review.get('visit_info', {}).get('verification_method'),
                    'visit_keywords': json.dumps(review.get('visit_keywords', []), ensure_ascii=False),
                    'image_urls': json.dumps(review.get('images', []), ensure_ascii=False),
                    'image_count': len(review.get('images', [])),
                    'has_owner_response': review.get('owner_response') is not None,
                    'owner_response_text': review.get('owner_response', {}).get('response_text') if review.get('owner_response') else None,
                    'reaction_count': review.get('reactions', {}).get('reaction_count'),
                    'scraped_at': review.get('scraped_at'),
                    'worker_id': review_data.get('worker_id')
                }
                records.append(record)
        else:
            # Facility with no reviews
            record = {
                'place_id': place_id,
                'facility_name': review_data.get('facility_name', ''),
                'review_index': None,
                'reviewer_name': None,
                'review_text': None,
                'visit_date': None,
                'visit_count': None,
                'verification_method': None,
                'visit_keywords': None,
                'image_urls': None,
                'image_count': 0,
                'has_owner_response': False,
                'owner_response_text': None,
                'reaction_count': None,
                'scraped_at': review_data.get('scraped_at'),
                'worker_id': review_data.get('worker_id')
            }
            records.append(record)
    
    df = pd.DataFrame(records)
    print(f"✓ Created DataFrame with {len(df)} records")
    
    # Save as CSV
    csv_file = output_dir / f"reviews_merged_{timestamp}.csv"
    print(f"\nSaving CSV...")
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print(f"✓ {csv_file}")
    
    # Save as Parquet (if possible)
    try:
        parquet_file = output_dir / f"reviews_merged_{timestamp}.parquet"
        print(f"\nSaving Parquet...")
        df.to_parquet(parquet_file, index=False)
        print(f"✓ {parquet_file}")
    except Exception as e:
        print(f"⚠ Could not save parquet: {e}")
    
    # Print summary
    print(f"\n{'='*70}")
    print("MERGE COMPLETE")
    print(f"{'='*70}")
    
    with_reviews = df['review_text'].notna().sum()
    
    print(f"\nTotal facilities: {len(all_results):,}")
    print(f"Total records: {len(df):,}")
    print(f"Records with reviews: {with_reviews:,}")
    
    if len(df) > 0:
        avg_reviews = with_reviews / len(all_results)
        print(f"Average reviews per facility: {avg_reviews:.1f}")
    
    print(f"\nOutput files:")
    print(f"  {merged_json.name}")
    print(f"  {csv_file.name}")
    if 'parquet_file' in locals():
        print(f"  {parquet_file.name}")
    
    print(f"\nLocation: {output_dir}/")
    print(f"{'='*70}\n")
    
    return df


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Merge worker JSON files')
    parser.add_argument(
        '--dir',
        default='./data/worker_results',
        help='Directory containing worker JSON files (default: ./data/worker_results)'
    )
    
    args = parser.parse_args()
    
    merge_worker_jsons(args.dir)