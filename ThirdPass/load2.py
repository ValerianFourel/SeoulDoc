"""
Check the last review in the review scraping JSON data
"""

import json
from typing import Dict, Any, List
from pathlib import Path
from datetime import datetime


def load_json(file_path: str) -> Dict:
    """Load JSON file"""
    print(f"Loading: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"✅ Loaded {len(data):,} places\n")
    return data


def find_last_review(data: Dict) -> None:
    """
    Find and display the last review(s) in the data
    
    Args:
        data: Dictionary with place_id as keys
    """
    print("=" * 80)
    print("ANALYZING REVIEWS STRUCTURE")
    print("=" * 80)
    
    # Check first few places to understand structure
    print("\n🔍 Checking review structure in first place...")
    first_place_id = next(iter(data.keys()))
    first_record = data[first_place_id]
    
    print(f"\nPlace ID: {first_place_id}")
    print(f"Record keys: {list(first_record.keys())}")
    
    # Check if there's a 'reviews' field
    if 'reviews' in first_record:
        reviews = first_record['reviews']
        print(f"\n✅ Found 'reviews' field")
        print(f"   Type: {type(reviews).__name__}")
        
        if isinstance(reviews, list):
            print(f"   Number of reviews: {len(reviews)}")
            if len(reviews) > 0:
                print(f"\n   Last review (index {len(reviews)-1}):")
                last_review = reviews[-1]
                print_review(last_review, indent=6)
                
                # Also show first review for comparison
                print(f"\n   First review (index 0) for comparison:")
                first_review = reviews[0]
                print_review(first_review, indent=6)
        elif isinstance(reviews, dict):
            print(f"   Number of reviews: {len(reviews)}")
            print(f"   Review keys: {list(reviews.keys())[:5]}...")
    else:
        print("\n❌ No 'reviews' field found")
        print(f"   Available fields: {list(first_record.keys())}")
    
    # Show last reviews from multiple places
    print("\n" + "=" * 80)
    print("LAST REVIEWS FROM MULTIPLE PLACES")
    print("=" * 80)
    
    for i, (place_id, record) in enumerate(list(data.items())[:5], 1):
        print(f"\n{i}. Place ID: {place_id}")
        if 'name' in record:
            print(f"   Name: {record['name']}")
        
        if 'reviews' in record and isinstance(record['reviews'], list):
            reviews = record['reviews']
            if len(reviews) > 0:
                print(f"   Total reviews: {len(reviews)}")
                print(f"   Last review:")
                print_review(reviews[-1], indent=6)
            else:
                print(f"   No reviews found")
        else:
            print(f"   No reviews list found")
        print("-" * 80)


def print_review(review: Any, indent: int = 0) -> None:
    """
    Print review details with proper formatting
    
    Args:
        review: Review data (could be dict, string, or other)
        indent: Number of spaces for indentation
    """
    indent_str = " " * indent
    
    if isinstance(review, dict):
        for key, value in review.items():
            if key == 'review_html':
                print(f"{indent_str}{key}: [HTML content - skipped]")
            elif isinstance(value, str):
                if len(value) > 100:
                    print(f"{indent_str}{key}: {value[:100]}...")
                else:
                    print(f"{indent_str}{key}: {value}")
            elif isinstance(value, list):
                print(f"{indent_str}{key}: [list with {len(value)} items]")
                if len(value) > 0 and len(value) <= 3:
                    for item in value:
                        print(f"{indent_str}  - {item}")
            elif isinstance(value, dict):
                print(f"{indent_str}{key}: [dict with {len(value)} keys]")
            else:
                print(f"{indent_str}{key}: {value}")
    elif isinstance(review, str):
        if len(review) > 200:
            print(f"{indent_str}{review[:200]}...")
        else:
            print(f"{indent_str}{review}")
    else:
        print(f"{indent_str}{review}")


def get_review_statistics(data: Dict) -> None:
    """
    Get statistics about reviews across all places
    
    Args:
        data: Dictionary with place_id as keys
    """
    print("\n" + "=" * 80)
    print("REVIEW STATISTICS")
    print("=" * 80)
    
    total_places_with_reviews = 0
    total_reviews = 0
    max_reviews = 0
    max_reviews_place = None
    
    review_keys = set()
    
    for place_id, record in data.items():
        if 'reviews' in record and isinstance(record['reviews'], list):
            reviews = record['reviews']
            if len(reviews) > 0:
                total_places_with_reviews += 1
                total_reviews += len(reviews)
                
                if len(reviews) > max_reviews:
                    max_reviews = len(reviews)
                    max_reviews_place = place_id
                
                # Collect all unique keys from reviews
                for review in reviews:
                    if isinstance(review, dict):
                        review_keys.update(review.keys())
    
    print(f"\n📊 Places with reviews: {total_places_with_reviews:,} / {len(data):,}")
    print(f"📊 Total reviews: {total_reviews:,}")
    if total_places_with_reviews > 0:
        print(f"📊 Average reviews per place: {total_reviews / total_places_with_reviews:.1f}")
    print(f"📊 Max reviews in one place: {max_reviews:,} (Place ID: {max_reviews_place})")
    
    print(f"\n📋 Unique keys found in review objects:")
    for i, key in enumerate(sorted(review_keys), 1):
        print(f"   {i:2d}. {key}")


if __name__ == "__main__":
    # File path
    json_file = "review_scraping_progress_p1_of_60.json"
    
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
        print("\n📝 Expected file name: review_scraping_progress_p1_of_60.json")
        exit(1)
    
    try:
        # Load the JSON
        data = load_json(file_found)
        
        # Find and display last review(s)
        find_last_review(data)
        
        # Get review statistics
        get_review_statistics(data)
        
        print("\n" + "=" * 80)
        print("✅ ANALYSIS COMPLETE")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
