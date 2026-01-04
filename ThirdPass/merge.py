#!/usr/bin/env python3
"""
Merge Partition Results
Combines checkpoint files and datasets from multiple partitions
"""

import pandas as pd
import json
from pathlib import Path
from typing import List, Dict
import argparse


def merge_checkpoint_files(data_dir: Path, total_partitions: int) -> Dict:
    """Merge JSON checkpoint files from all partitions"""
    
    print(f"\n{'='*70}")
    print("MERGING CHECKPOINT FILES")
    print(f"{'='*70}")
    
    merged_data = {}
    
    for partition_x in range(1, total_partitions + 1):
        checkpoint_file = data_dir / f"review_scraping_progress_p{partition_x}_of_{total_partitions}.json"
        
        if checkpoint_file.exists():
            print(f"✓ Loading partition {partition_x}/{total_partitions}...")
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                partition_data = json.load(f)
                merged_data.update(partition_data)
                print(f"  Added {len(partition_data):,} facilities")
        else:
            print(f"⚠ Partition {partition_x}/{total_partitions} not found: {checkpoint_file}")
    
    print(f"\n✓ Total merged facilities: {len(merged_data):,}")
    
    # Save merged checkpoint
    merged_file = data_dir / "review_scraping_progress_merged.json"
    with open(merged_file, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, ensure_ascii=False, indent=2)
    
    print(f"✓ Saved merged checkpoint: {merged_file}")
    
    return merged_data


def merge_parquet_files(data_dir: Path, total_partitions: int) -> pd.DataFrame:
    """Merge parquet review datasets from all partitions"""
    
    print(f"\n{'='*70}")
    print("MERGING REVIEW DATASETS")
    print(f"{'='*70}")
    
    dataframes = []
    
    for partition_x in range(1, total_partitions + 1):
        parquet_file = data_dir / f"seoul_medical_reviews_p{partition_x}_of_{total_partitions}.parquet"
        
        if parquet_file.exists():
            print(f"✓ Loading partition {partition_x}/{total_partitions}...")
            df = pd.read_parquet(parquet_file)
            dataframes.append(df)
            print(f"  Rows: {len(df):,}")
        else:
            print(f"⚠ Partition {partition_x}/{total_partitions} not found: {parquet_file}")
    
    if not dataframes:
        print("✗ No parquet files found to merge!")
        return pd.DataFrame()
    
    # Merge all dataframes
    merged_df = pd.concat(dataframes, ignore_index=True)
    
    # Remove duplicates (in case of overlap)
    initial_count = len(merged_df)
    merged_df = merged_df.drop_duplicates(subset=['place_id', 'review_index'], keep='first')
    duplicates_removed = initial_count - len(merged_df)
    
    if duplicates_removed > 0:
        print(f"\n⚠ Removed {duplicates_removed:,} duplicate reviews")
    
    print(f"\n✓ Total merged reviews: {len(merged_df):,}")
    
    # Save merged dataset
    merged_parquet = data_dir / "seoul_medical_reviews_merged.parquet"
    merged_df.to_parquet(merged_parquet, index=False)
    print(f"✓ Saved merged parquet: {merged_parquet}")
    
    # Also save as CSV
    merged_csv = data_dir / "seoul_medical_reviews_merged.csv"
    merged_df.to_csv(merged_csv, index=False, encoding='utf-8-sig')
    print(f"✓ Saved merged CSV: {merged_csv}")
    
    return merged_df


def print_merge_stats(merged_checkpoint: Dict, merged_df: pd.DataFrame):
    """Print statistics about merged data"""
    
    print(f"\n{'='*70}")
    print("MERGE STATISTICS")
    print(f"{'='*70}")
    
    # Checkpoint stats
    total_facilities = len(merged_checkpoint)
    with_reviews = sum(1 for v in merged_checkpoint.values() if v.get('has_reviews'))
    total_review_count = sum(v.get('review_count', 0) for v in merged_checkpoint.values())
    
    print(f"\nCheckpoint data:")
    print(f"  Total facilities processed: {total_facilities:,}")
    print(f"  Facilities with reviews: {with_reviews:,}")
    print(f"  Total review count: {total_review_count:,}")
    
    if with_reviews > 0:
        avg_reviews = total_review_count / with_reviews
        print(f"  Average reviews per facility: {avg_reviews:.1f}")
    
    # Dataset stats
    if len(merged_df) > 0:
        unique_facilities = merged_df['place_id'].nunique()
        total_reviews = len(merged_df)
        
        print(f"\nDataset records:")
        print(f"  Unique facilities: {unique_facilities:,}")
        print(f"  Total review records: {total_reviews:,}")
        
        # Check for facilities without reviews
        no_review_records = merged_df[merged_df['review_text'].isna()]
        if len(no_review_records) > 0:
            print(f"  Records without reviews: {len(no_review_records):,}")
    
    print(f"{'='*70}")


def main():
    """Main merge function"""
    
    parser = argparse.ArgumentParser(description='Merge partition results')
    parser.add_argument(
        '--partitions',
        type=int,
        required=True,
        help='Total number of partitions to merge'
    )
    parser.add_argument(
        '--data-dir',
        type=str,
        default='./data',
        help='Data directory containing partition files (default: ./data)'
    )
    
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    
    if not data_dir.exists():
        print(f"✗ Data directory not found: {data_dir}")
        return
    
    print(f"{'='*70}")
    print(f"MERGING {args.partitions} PARTITIONS")
    print(f"{'='*70}")
    print(f"Data directory: {data_dir}")
    
    # Merge checkpoint files
    merged_checkpoint = merge_checkpoint_files(data_dir, args.partitions)
    
    # Merge parquet files
    merged_df = merge_parquet_files(data_dir, args.partitions)
    
    # Print statistics
    if merged_checkpoint:
        print_merge_stats(merged_checkpoint, merged_df)
    
    print(f"\n{'='*70}")
    print("✅ MERGE COMPLETE")
    print(f"{'='*70}")
    print(f"\nMerged files:")
    print(f"  Checkpoint: {data_dir}/review_scraping_progress_merged.json")
    print(f"  Parquet: {data_dir}/seoul_medical_reviews_merged.parquet")
    print(f"  CSV: {data_dir}/seoul_medical_reviews_merged.csv")


if __name__ == "__main__":
    main()