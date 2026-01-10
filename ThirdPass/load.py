"""
Load review scraping JSON data and analyze structure
Excludes 'review_html' field to avoid long outputs
"""

import json
from typing import Dict, List, Set
from pathlib import Path


def load_review_json(file_path: str, exclude_keys: List[str] = None) -> Dict:
    """
    Load JSON file and exclude specified keys from each record
    
    Args:
        file_path: Path to the JSON file
        exclude_keys: List of keys to exclude (default: ['review_html'])
    
    Returns:
        Dictionary with place_id as keys
    """
    if exclude_keys is None:
        exclude_keys = ['review_html']
    
    print(f"Loading: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Remove excluded keys from each record
    cleaned_data = {}
    for place_id, record in data.items():
        if isinstance(record, dict):
            cleaned_record = {k: v for k, v in record.items() if k not in exclude_keys}
            cleaned_data[place_id] = cleaned_record
        else:
            cleaned_data[place_id] = record
    
    return cleaned_data


def analyze_structure(data: Dict) -> None:
    """
    Analyze and display the structure of the loaded data
    
    Args:
        data: Dictionary with place_id as keys
    """
    print("=" * 80)
    print("DATA STRUCTURE ANALYSIS")
    print("=" * 80)
    
    # Total number of places
    print(f"\n📊 Total places: {len(data):,}")
    
    # Get all unique keys across all records
    all_keys: Set[str] = set()
    for record in data.values():
        if isinstance(record, dict):
            all_keys.update(record.keys())
    
    print(f"\n📋 Unique keys found across all records:")
    for i, key in enumerate(sorted(all_keys), 1):
        print(f"   {i:2d}. {key}")
    
    # Show key frequency (how many records have each key)
    print(f"\n📊 Key frequency:")
    key_counts = {key: 0 for key in all_keys}
    for record in data.values():
        if isinstance(record, dict):
            for key in record.keys():
                key_counts[key] += 1
    
    for key in sorted(key_counts.keys()):
        count = key_counts[key]
        percentage = (count / len(data)) * 100
        print(f"   {key:30s}: {count:6,} / {len(data):,} ({percentage:5.1f}%)")
    
    # Sample first record structure
    print(f"\n👀 Sample record structure (first place):")
    first_place_id = next(iter(data.keys()))
    first_record = data[first_place_id]
    
    print(f"\n   Place ID: {first_place_id}")
    if isinstance(first_record, dict):
        print(f"   Keys in this record: {list(first_record.keys())}")
        print(f"\n   Detailed structure:")
        for key, value in first_record.items():
            value_type = type(value).__name__
            if isinstance(value, (list, dict)):
                value_len = len(value)
                print(f"      {key:30s}: {value_type:10s} (length: {value_len})")
            elif isinstance(value, str):
                value_preview = value[:50] + "..." if len(value) > 50 else value
                print(f"      {key:30s}: {value_type:10s} = '{value_preview}'")
            else:
                print(f"      {key:30s}: {value_type:10s} = {value}")
    
    return all_keys, key_counts


def show_sample_records(data: Dict, n: int = 3) -> None:
    """
    Show a few sample records
    
    Args:
        data: Dictionary with place_id as keys
        n: Number of samples to show
    """
    print("\n" + "=" * 80)
    print(f"SAMPLE RECORDS (first {n})")
    print("=" * 80)
    
    for i, (place_id, record) in enumerate(list(data.items())[:n], 1):
        print(f"\n{i}. Place ID: {place_id}")
        print("-" * 80)
        
        if isinstance(record, dict):
            for key, value in record.items():
                if isinstance(value, list):
                    print(f"   {key:25s}: list with {len(value)} items")
                    if value and len(value) > 0:
                        print(f"      First item type: {type(value[0]).__name__}")
                elif isinstance(value, dict):
                    print(f"   {key:25s}: dict with {len(value)} keys")
                elif isinstance(value, str):
                    value_preview = value[:100] + "..." if len(value) > 100 else value
                    print(f"   {key:25s}: '{value_preview}'")
                else:
                    print(f"   {key:25s}: {value}")
        else:
            print(f"   Record type: {type(record).__name__}")
            print(f"   Value: {record}")


def export_keys_summary(all_keys: Set[str], key_counts: Dict[str, int], 
                       output_file: str = "keys_summary.txt") -> None:
    """
    Export keys summary to a text file
    
    Args:
        all_keys: Set of all unique keys
        key_counts: Dictionary with key frequencies
        output_file: Output file path
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("KEYS SUMMARY\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"Total unique keys: {len(all_keys)}\n\n")
        
        f.write("All keys (sorted):\n")
        for i, key in enumerate(sorted(all_keys), 1):
            count = key_counts.get(key, 0)
            f.write(f"   {i:2d}. {key:30s} (appears in {count:,} records)\n")
    
    print(f"\n✅ Keys summary exported to: {output_file}")


if __name__ == "__main__":
    # File path
    json_file = "data/review_scraping_progress_p52_of_60.json"
    
    # Alternative paths to try
    possible_paths = [
        json_file,
        f"data/{json_file}",
        f"/mnt/user-data/uploads/{json_file}",
        f"/home/claude/{json_file}"
    ]
    
    # Find the file
    file_found = None
    for path in possible_paths:
        if Path(path).exists():
            file_found = path
            break
    
    if not file_found:
        print(f"❌ File not found in any of these locations:")
        for path in possible_paths:
            print(f"   - {path}")
        print("\n💡 Please make sure the file exists or update the path.")
        exit(1)
    
    try:
        # Load the JSON (excluding review_html)
        data = load_review_json(file_found, exclude_keys=['review_html'])
        
        print(f"✅ Successfully loaded {len(data):,} records")
        print(f"   (Excluded fields: review_html)")
        
        # Analyze structure
        all_keys, key_counts = analyze_structure(data)
        
        # Show sample records
        show_sample_records(data, n=3)
        
        # Export keys summary
        export_keys_summary(all_keys, key_counts, 
                          output_file="/mnt/user-data/outputs/keys_summary.txt")
        
        # Save the cleaned data (without review_html) for further analysis
        output_json = "/mnt/user-data/outputs/review_data_cleaned.json"
        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"✅ Cleaned data saved to: {output_json}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
