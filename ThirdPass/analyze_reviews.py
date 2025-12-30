#!/usr/bin/env python3
"""
Analyze scraped review data
Shows statistics and insights from the reviews
"""

import pandas as pd
import json
from pathlib import Path
from collections import Counter


def load_review_data():
    """Load review data from parquet file"""
    review_file = Path("./data/seoul_medical_reviews.parquet")
    
    if not review_file.exists():
        print(f"✗ Review file not found: {review_file}")
        print("  Please run the review scraper first")
        return None
    
    df = pd.read_parquet(review_file)
    print(f"✓ Loaded {len(df):,} review records")
    return df


def analyze_basic_stats(df):
    """Print basic statistics"""
    print(f"\n{'='*70}")
    print("BASIC STATISTICS")
    print(f"{'='*70}")
    
    # Total reviews
    total_reviews = len(df[df['review_text'].notna()])
    print(f"Total reviews: {total_reviews:,}")
    
    # Unique facilities
    unique_facilities = df['place_id'].nunique()
    print(f"Unique facilities: {unique_facilities:,}")
    
    # Reviews per facility
    reviews_per_facility = df.groupby('place_id')['review_text'].count()
    print(f"\nReviews per facility:")
    print(f"  Mean: {reviews_per_facility.mean():.1f}")
    print(f"  Median: {reviews_per_facility.median():.1f}")
    print(f"  Max: {reviews_per_facility.max()}")
    print(f"  Min: {reviews_per_facility.min()}")
    
    # Reviews with images
    with_images = len(df[df['image_count'] > 0])
    print(f"\nReviews with images: {with_images:,} ({with_images/total_reviews*100:.1f}%)")
    
    # Reviews with owner responses
    with_responses = len(df[df['has_owner_response'] == True])
    print(f"Reviews with owner responses: {with_responses:,} ({with_responses/total_reviews*100:.1f}%)")


def analyze_visit_keywords(df):
    """Analyze visit keywords"""
    print(f"\n{'='*70}")
    print("VISIT KEYWORDS")
    print(f"{'='*70}")
    
    all_keywords = []
    
    for keywords_json in df['visit_keywords'].dropna():
        try:
            keywords = json.loads(keywords_json)
            all_keywords.extend(keywords)
        except:
            pass
    
    if all_keywords:
        keyword_counts = Counter(all_keywords)
        
        print("\nMost common visit keywords:")
        for keyword, count in keyword_counts.most_common(10):
            print(f"  {keyword}: {count:,} times")


def analyze_verification_methods(df):
    """Analyze verification methods"""
    print(f"\n{'='*70}")
    print("VERIFICATION METHODS")
    print(f"{'='*70}")
    
    verification_counts = df['verification_method'].value_counts()
    
    print("\nVerification methods:")
    for method, count in verification_counts.items():
        percentage = count / len(df[df['verification_method'].notna()]) * 100
        print(f"  {method}: {count:,} ({percentage:.1f}%)")


def analyze_review_length(df):
    """Analyze review text length"""
    print(f"\n{'='*70}")
    print("REVIEW LENGTH ANALYSIS")
    print(f"{'='*70}")
    
    df_with_text = df[df['review_text'].notna()].copy()
    df_with_text['text_length'] = df_with_text['review_text'].str.len()
    
    print(f"\nReview text length (characters):")
    print(f"  Mean: {df_with_text['text_length'].mean():.0f}")
    print(f"  Median: {df_with_text['text_length'].median():.0f}")
    print(f"  Max: {df_with_text['text_length'].max()}")
    print(f"  Min: {df_with_text['text_length'].min()}")
    
    # Categorize by length
    short = len(df_with_text[df_with_text['text_length'] < 50])
    medium = len(df_with_text[(df_with_text['text_length'] >= 50) & (df_with_text['text_length'] < 200)])
    long = len(df_with_text[df_with_text['text_length'] >= 200])
    
    total = len(df_with_text)
    print(f"\nBy length category:")
    print(f"  Short (<50 chars): {short:,} ({short/total*100:.1f}%)")
    print(f"  Medium (50-200 chars): {medium:,} ({medium/total*100:.1f}%)")
    print(f"  Long (>200 chars): {long:,} ({long/total*100:.1f}%)")


def find_top_reviewed_facilities(df):
    """Find facilities with most reviews"""
    print(f"\n{'='*70}")
    print("TOP REVIEWED FACILITIES")
    print(f"{'='*70}")
    
    review_counts = df.groupby(['place_id', 'facility_name']).size().reset_index(name='review_count')
    top_facilities = review_counts.nlargest(10, 'review_count')
    
    print("\nTop 10 facilities by review count:")
    for idx, row in top_facilities.iterrows():
        print(f"  {row['facility_name']}: {row['review_count']} reviews")


def analyze_owner_response_rate(df):
    """Analyze owner response rates"""
    print(f"\n{'='*70}")
    print("OWNER RESPONSE ANALYSIS")
    print(f"{'='*70}")
    
    # Response rate by facility
    facility_stats = df.groupby('place_id').agg({
        'has_owner_response': ['sum', 'count']
    })
    
    facility_stats.columns = ['responses', 'total_reviews']
    facility_stats['response_rate'] = facility_stats['responses'] / facility_stats['total_reviews'] * 100
    
    print(f"\nOwner response rate:")
    print(f"  Mean: {facility_stats['response_rate'].mean():.1f}%")
    print(f"  Median: {facility_stats['response_rate'].median():.1f}%")
    
    # Facilities with 100% response rate
    perfect_response = len(facility_stats[facility_stats['response_rate'] == 100])
    print(f"\nFacilities with 100% response rate: {perfect_response}")
    
    # Facilities with 0% response rate
    no_response = len(facility_stats[facility_stats['response_rate'] == 0])
    print(f"Facilities with 0% response rate: {no_response}")


def show_sample_reviews(df, n=3):
    """Show sample reviews"""
    print(f"\n{'='*70}")
    print("SAMPLE REVIEWS")
    print(f"{'='*70}")
    
    sample_df = df[df['review_text'].notna()].sample(min(n, len(df)))
    
    for idx, row in sample_df.iterrows():
        print(f"\n{'-'*70}")
        print(f"Facility: {row['facility_name']}")
        print(f"Reviewer: {row['reviewer_name']}")
        print(f"Visit Date: {row['visit_date']}")
        print(f"Visit Count: {row['visit_count']}")
        
        if row['visit_keywords']:
            try:
                keywords = json.loads(row['visit_keywords'])
                print(f"Keywords: {', '.join(keywords)}")
            except:
                pass
        
        print(f"\nReview Text:")
        text = row['review_text']
        if len(text) > 200:
            text = text[:200] + "..."
        print(f"  {text}")
        
        print(f"\nImages: {row['image_count']}")
        print(f"Has Owner Response: {row['has_owner_response']}")
        
        if row['has_owner_response'] and pd.notna(row['owner_response_text']):
            response = row['owner_response_text']
            if len(response) > 150:
                response = response[:150] + "..."
            print(f"Owner Response: {response}")


def main():
    """Main analysis function"""
    
    print("="*70)
    print("REVIEW DATA ANALYSIS")
    print("="*70)
    
    # Load data
    df = load_review_data()
    
    if df is None:
        return
    
    # Run analyses
    analyze_basic_stats(df)
    analyze_visit_keywords(df)
    analyze_verification_methods(df)
    analyze_review_length(df)
    find_top_reviewed_facilities(df)
    analyze_owner_response_rate(df)
    show_sample_reviews(df, n=3)
    
    print(f"\n{'='*70}")
    print("✅ ANALYSIS COMPLETE")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
