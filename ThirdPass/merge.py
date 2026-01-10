#!/usr/bin/env python3
"""
Merge Review Partition JSON Files - Clean & Memory Efficient
1. Keeps only place_ids where status != "failed"
2. Removes review_html field (raw data)
3. Merges into final parquet incrementally
"""

import pandas as pd
import json
import csv
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import glob
import gc
import psutil


def get_memory_usage_mb():
    """Get current memory usage in MB"""
    try:
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    except:
        return 0


class ReviewPartitionMerger:
    """Clean and merge review partition JSONs"""
    
    def __init__(self, data_dir: str = "./data", batch_size: int = 5000):
        self.data_dir = Path(data_dir)
        self.batch_size = batch_size
        self.conflict_resolution = {}
        
    def find_partition_files(self) -> List[Path]:
        """Find all partition checkpoint JSON files"""
        pattern = str(self.data_dir / "review_scraping_progress_p*_of_*.json")
        files = glob.glob(pattern)
        
        non_partition_file = self.data_dir / "review_scraping_progress.json"
        if non_partition_file.exists():
            files.append(str(non_partition_file))
        
        return sorted([Path(f) for f in files])
    
    def load_and_clean_partition(self, filepath: Path) -> Dict:
        """
        Load partition and clean it:
        1. Keep only place_ids where status != "failed"
        2. Remove review_html field
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            cleaned_data = {}
            skipped_failed = 0
            
            for place_id, place_data in data.items():
                # Skip if status is "failed"
                if place_data.get('status') == 'failed':
                    skipped_failed += 1
                    continue
                
                # Make a copy and remove review_html
                cleaned_place_data = place_data.copy()
                if 'review_html' in cleaned_place_data:
                    del cleaned_place_data['review_html']
                
                cleaned_data[place_id] = cleaned_place_data
            
            return cleaned_data, skipped_failed
            
        except Exception as e:
            print(f"✗ Error loading {filepath.name}: {e}")
            return {}, 0
    
    def get_partition_stats(self, filepath: Path) -> Dict:
        """Get quick stats from partition"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            total = len(data)
            successful = sum(1 for d in data.values() if d.get('status') == 'success')
            failed = sum(1 for d in data.values() if d.get('status') == 'failed')
            with_reviews = sum(1 for d in data.values() 
                              if d.get('status') == 'success' and d.get('has_reviews'))
            total_reviews = sum(d.get('review_count', 0) for d in data.values() 
                               if d.get('status') == 'success')
            
            return {
                'total': total,
                'successful': successful,
                'failed': failed,
                'with_reviews': with_reviews,
                'total_reviews': total_reviews
            }
        except:
            return {}
    
    def detect_conflicts(self, partition_files: List[Path]) -> Dict[str, List[int]]:
        """
        Detect place_ids in multiple partitions (only for successful ones)
        """
        print("\n🔍 Detecting conflicts...")
        place_id_map = {}
        
        for idx, filepath in enumerate(partition_files):
            print(f"  Scanning {filepath.name}...")
            
            cleaned_data, _ = self.load_and_clean_partition(filepath)
            
            for place_id in cleaned_data.keys():
                if place_id not in place_id_map:
                    place_id_map[place_id] = []
                place_id_map[place_id].append(idx)
            
            del cleaned_data
            gc.collect()
        
        conflicts = {pid: indices for pid, indices in place_id_map.items() 
                    if len(indices) > 1}
        
        print(f"  ✓ Conflicts: {len(conflicts):,}")
        print(f"  ✓ Unique place_ids: {len(place_id_map):,}\n")
        
        return conflicts
    
    def resolve_conflict(self, place_id: str, partition_files: List[Path], 
                        partition_indices: List[int]) -> tuple:
        """
        Resolve which partition wins for a conflicted place_id
        Priority: success > failed, more reviews > fewer, fewer retries > more
        """
        candidates = []
        
        for idx in partition_indices:
            cleaned_data, _ = self.load_and_clean_partition(partition_files[idx])
            
            if place_id in cleaned_data:
                candidates.append((idx, cleaned_data[place_id]))
            
            del cleaned_data
        
        if not candidates:
            return None, None
        
        def priority_score(item):
            idx, data = item
            status = data.get('status')
            review_count = data.get('review_count', 0)
            retry_count = data.get('retry_count', 0)
            
            status_score = 1000 if status == 'success' else 0
            review_score = review_count
            retry_penalty = -retry_count if status == 'failed' else 0
            
            return status_score + review_score + retry_penalty
        
        best = max(candidates, key=priority_score)
        return best
    
    def convert_partition_to_records(self, 
                                    cleaned_data: Dict,
                                    facilities_df: Optional[pd.DataFrame] = None) -> List[Dict]:
        """
        Convert cleaned partition data to flat review records
        """
        records = []
        
        for place_id, place_data in cleaned_data.items():
            # Only process successful scrapes
            if place_data.get('status') != 'success':
                continue
            
            # Get facility name
            facility_name = "Unknown"
            if facilities_df is not None:
                try:
                    facility = facilities_df[facilities_df['place_id'].astype(str) == place_id]
                    if len(facility) > 0:
                        facility_name = facility.iloc[0].get('name', 'Unknown')
                        if pd.isna(facility_name):
                            facility_name = "Unknown"
                        else:
                            facility_name = str(facility_name)
                except:
                    pass
            
            # Create records for reviews
            if place_data.get('has_reviews') and place_data.get('reviews'):
                for review in place_data['reviews']:
                    record = {
                        'place_id': place_id,
                        'facility_name': facility_name,
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
                        'scraped_at': review.get('scraped_at')
                    }
                    records.append(record)
            else:
                # No reviews but successful scrape
                record = {
                    'place_id': place_id,
                    'facility_name': facility_name,
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
                    'scraped_at': place_data.get('scraped_at')
                }
                records.append(record)
        
        return records
    
    def merge_partitions_to_parquet(self,
                                   output_parquet: Path,
                                   partition_files: List[Path],
                                   conflicts: Dict[str, List[int]],
                                   facilities_df: Optional[pd.DataFrame] = None):
        """
        Process each partition and write to separate temp parquets,
        then merge at the end
        """
        print(f"\n{'='*70}")
        print(f"PHASE 1: PROCESSING PARTITIONS")
        print(f"{'='*70}")
        
        temp_files = []
        total_records = 0
        
        for idx, filepath in enumerate(partition_files):
            print(f"\n📂 Processing {filepath.name}...")
            mem_before = get_memory_usage_mb()
            
            # Load and clean
            cleaned_data, skipped_failed = self.load_and_clean_partition(filepath)
            
            print(f"  Loaded: {len(cleaned_data):,} successful place_ids")
            if skipped_failed > 0:
                print(f"  Skipped: {skipped_failed:,} failed place_ids")
            
            # Filter out conflicts that should be handled by other partitions
            filtered_data = {}
            skipped_conflicts = 0
            
            for place_id, place_data in cleaned_data.items():
                if place_id in conflicts:
                    # Check if this partition wins
                    if place_id in self.conflict_resolution:
                        winner_idx, _ = self.conflict_resolution[place_id]
                        if winner_idx == idx:
                            filtered_data[place_id] = place_data
                        else:
                            skipped_conflicts += 1
                    else:
                        skipped_conflicts += 1
                else:
                    # No conflict
                    filtered_data[place_id] = place_data
            
            if skipped_conflicts > 0:
                print(f"  Skipped: {skipped_conflicts:,} conflicts (handled by other partitions)")
            
            print(f"  Processing: {len(filtered_data):,} place_ids")
            
            # Convert to records
            records = self.convert_partition_to_records(filtered_data, facilities_df)
            
            if records:
                # Write to temp parquet
                temp_file = self.data_dir / f"_temp_reviews_{idx:03d}.parquet"
                df = pd.DataFrame(records)
                df.to_parquet(temp_file, index=False)
                temp_files.append(temp_file)
                
                record_count = len(records)
                total_records += record_count
                
                print(f"  ✓ Wrote {record_count:,} records to temp file")
                
                del df
            else:
                print(f"  ⚠ No records to write")
            
            # Cleanup
            del cleaned_data, filtered_data, records
            gc.collect()
            
            mem_after = get_memory_usage_mb()
            print(f"  Memory: {mem_before:.1f} MB → {mem_after:.1f} MB (Δ {mem_after-mem_before:+.1f} MB)")
            print(f"  Running total: {total_records:,} records")
        
        # Merge all temp files
        print(f"\n{'='*70}")
        print(f"PHASE 2: MERGING TEMP FILES")
        print(f"{'='*70}\n")
        
        if not temp_files:
            print("✗ No temp files to merge!")
            return 0
        
        print(f"Merging {len(temp_files)} temp files with PyArrow...")
        mem_before = get_memory_usage_mb()
        
        try:
            import pyarrow.parquet as pq
            import pyarrow as pa
            
            # Read all temp files as tables
            tables = []
            for temp_file in temp_files:
                print(f"  Loading {temp_file.name}...")
                table = pq.read_table(temp_file)
                tables.append(table)
                print(f"    {len(table):,} records")
            
            # Concatenate
            print(f"\n  Concatenating {len(tables)} tables...")
            merged_table = pa.concat_tables(tables)
            
            # Write final parquet
            print(f"  Writing final parquet...")
            pq.write_table(merged_table, output_parquet)
            
            final_count = len(merged_table)
            
            # Cleanup
            del tables, merged_table
            gc.collect()
            
            print(f"\n  ✓ Final parquet: {output_parquet}")
            print(f"  ✓ Total records: {final_count:,}")
            
        except Exception as e:
            print(f"  ⚠ PyArrow merge failed: {e}")
            print(f"  Falling back to pandas concat...")
            
            # Fallback: pandas concat
            dfs = []
            for temp_file in temp_files:
                df = pd.read_parquet(temp_file)
                dfs.append(df)
            
            merged_df = pd.concat(dfs, ignore_index=True)
            merged_df.to_parquet(output_parquet, index=False)
            final_count = len(merged_df)
            
            del dfs, merged_df
            gc.collect()
        
        mem_after = get_memory_usage_mb()
        print(f"  Memory: {mem_before:.1f} MB → {mem_after:.1f} MB (Δ {mem_after-mem_before:+.1f} MB)")
        
        # Delete temp files
        print(f"\n🧹 Cleaning up temp files...")
        for temp_file in temp_files:
            try:
                temp_file.unlink()
            except:
                pass
        print(f"  ✓ Deleted {len(temp_files)} temp files")
        
        return final_count
    
    def merge_all(self, 
                  output_parquet: Path,
                  facilities_df: Optional[pd.DataFrame] = None):
        """Main merge function"""
        partition_files = self.find_partition_files()
        
        if not partition_files:
            print("✗ No partition files found!")
            return 0
        
        print(f"\n{'='*70}")
        print(f"REVIEW PARTITION MERGER")
        print(f"Filter: Keep only status != 'failed'")
        print(f"Clean: Remove 'review_html' field")
        print(f"{'='*70}")
        print(f"Partitions: {len(partition_files)}")
        print(f"Batch size: {self.batch_size:,}")
        print(f"Initial memory: {get_memory_usage_mb():.1f} MB")
        print(f"{'='*70}")
        
        # Show stats
        print(f"\n📊 Partition Statistics:")
        total_successful = 0
        total_failed = 0
        total_reviews = 0
        
        for filepath in partition_files:
            stats = self.get_partition_stats(filepath)
            if stats:
                print(f"\n  {filepath.name}:")
                print(f"    Total: {stats['total']:,}")
                print(f"    ✅ Successful: {stats['successful']:,}")
                print(f"    ❌ Failed: {stats['failed']:,} (will be skipped)")
                print(f"    Reviews: {stats['total_reviews']:,}")
                total_successful += stats['successful']
                total_failed += stats['failed']
                total_reviews += stats['total_reviews']
        
        print(f"\n  TOTALS:")
        print(f"    Successful: {total_successful:,} (will be kept)")
        print(f"    Failed: {total_failed:,} (will be skipped)")
        print(f"    Reviews: {total_reviews:,}")
        
        # Detect conflicts
        conflicts = self.detect_conflicts(partition_files)
        
        # Resolve conflicts
        if conflicts:
            print(f"🔧 Resolving {len(conflicts):,} conflicts...")
            resolved = 0
            
            for place_id, partition_indices in conflicts.items():
                winner_idx, winner_data = self.resolve_conflict(
                    place_id, partition_files, partition_indices
                )
                if winner_idx is not None:
                    self.conflict_resolution[place_id] = (winner_idx, winner_data)
                    resolved += 1
                    
                    if resolved % 1000 == 0:
                        print(f"  Resolved {resolved:,}/{len(conflicts):,}")
            
            print(f"  ✓ Resolved {resolved:,} conflicts\n")
        
        # Merge
        total_records = self.merge_partitions_to_parquet(
            output_parquet=output_parquet,
            partition_files=partition_files,
            conflicts=conflicts,
            facilities_df=facilities_df
        )
        
        print(f"\n{'='*70}")
        print(f"✅ MERGE COMPLETE")
        print(f"{'='*70}")
        print(f"Output: {output_parquet}")
        print(f"Records: {total_records:,}")
        print(f"Final memory: {get_memory_usage_mb():.1f} MB")
        print(f"{'='*70}\n")
        
        return total_records


def load_facilities_dataset(data_dir: str = "./data") -> Optional[pd.DataFrame]:
    """Load facilities dataset"""
    data_dir = Path(data_dir)
    
    for ext in ['csv', 'parquet', 'pkl']:
        filepath = data_dir / f"seoul_medical_facilities.{ext}"
        if filepath.exists():
            try:
                if ext == 'csv':
                    df = pd.read_csv(filepath)
                elif ext == 'parquet':
                    df = pd.read_parquet(filepath)
                elif ext == 'pkl':
                    df = pd.read_pickle(filepath)
                
                print(f"✓ Loaded facilities: {filepath.name} ({len(df):,} facilities)")
                return df
            except Exception as e:
                print(f"⚠️  Could not load {filepath.name}: {e}")
    
    print("⚠️  No facilities dataset found")
    return None


def create_facility_summary(data_dir: Path) -> pd.DataFrame:
    """Create facility-level summary from partition files"""
    print(f"\n{'='*70}")
    print(f"CREATING FACILITY SUMMARY")
    print(f"{'='*70}\n")
    
    merger = ReviewPartitionMerger(data_dir=str(data_dir))
    partition_files = merger.find_partition_files()
    
    summary_records = []
    seen_place_ids = set()
    
    for filepath in partition_files:
        print(f"Processing {filepath.name}...")
        
        cleaned_data, skipped = merger.load_and_clean_partition(filepath)
        
        for place_id, place_data in cleaned_data.items():
            if place_id in seen_place_ids:
                continue
            
            seen_place_ids.add(place_id)
            
            record = {
                'place_id': place_id,
                'status': place_data.get('status'),
                'has_reviews': place_data.get('has_reviews', False),
                'review_count': place_data.get('review_count', 0),
                'retry_count': place_data.get('retry_count', 0),
                'scraped_at': place_data.get('scraped_at')
            }
            summary_records.append(record)
        
        del cleaned_data
        gc.collect()
    
    summary_df = pd.DataFrame(summary_records)
    print(f"✓ Created summary: {len(summary_df):,} facilities\n")
    
    return summary_df


def main():
    """Main execution"""
    
    print(f"{'='*70}")
    print(f"REVIEW PARTITION MERGER")
    print(f"Filters out failed scrapes, removes raw HTML data")
    print(f"{'='*70}\n")
    
    # Load facilities
    print(f"{'='*70}")
    print(f"LOADING FACILITY NAMES")
    print(f"{'='*70}\n")
    facilities_df = load_facilities_dataset(data_dir="./data")
    
    # Merge partitions
    merger = ReviewPartitionMerger(data_dir="./data", batch_size=5000)
    
    output_dir = Path("./data")
    parquet_file = output_dir / "seoul_medical_reviews_merged.parquet"
    
    total_records = merger.merge_all(
        output_parquet=parquet_file,
        facilities_df=facilities_df
    )
    
    if total_records == 0:
        print("✗ No records to save.")
        return
    
    # Create CSV
    print(f"{'='*70}")
    print(f"CREATING CSV")
    print(f"{'='*70}\n")
    
    csv_file = output_dir / "seoul_medical_reviews_merged.csv"
    
    try:
        df = pd.read_parquet(parquet_file)
        df.to_csv(csv_file, index=False, encoding='utf-8-sig',
                 quoting=csv.QUOTE_ALL, escapechar='\\')
        print(f"✓ Saved CSV: {csv_file}\n")
        del df
        gc.collect()
    except Exception as e:
        print(f"✗ Error creating CSV: {e}\n")
    
    # Create summary
    summary_df = create_facility_summary(output_dir)
    
    if facilities_df is not None:
        summary_df = summary_df.merge(
            facilities_df[['place_id', 'name']].rename(columns={'name': 'facility_name'}),
            on='place_id',
            how='left'
        )
    
    # Save summary
    summary_parquet = output_dir / "facility_review_summary_merged.parquet"
    summary_df.to_parquet(summary_parquet, index=False)
    print(f"✓ Saved: {summary_parquet}")
    
    summary_csv = output_dir / "facility_review_summary_merged.csv"
    summary_df.to_csv(summary_csv, index=False, encoding='utf-8-sig',
                      quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
    print(f"✓ Saved: {summary_csv}")
    
    # Final stats
    print(f"\n{'='*70}")
    print(f"✅ COMPLETE!")
    print(f"{'='*70}")
    print(f"\nOutput files:")
    print(f"  📊 {parquet_file.name} ({total_records:,} records)")
    if csv_file.exists():
        print(f"     {csv_file.name}")
    print(f"  📋 {summary_parquet.name} ({len(summary_df):,} facilities)")
    print(f"     {summary_csv.name}")
    
    print(f"\n📊 Stats:")
    print(f"   Facilities: {len(summary_df):,}")
    print(f"   With reviews: {summary_df['has_reviews'].sum():,}")
    print(f"   Review records: {total_records:,}")
    
    print(f"\n💾 Peak memory: {get_memory_usage_mb():.1f} MB")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
