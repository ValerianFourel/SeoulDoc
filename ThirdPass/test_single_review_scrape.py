#!/usr/bin/env python3
"""
Simple test script to scrape reviews from a single Naver Maps facility
Usage: python test_single_review_scrape.py "facility_name"
"""

import sys
import json
from naver_review_scraper import NaverMapsReviewScraper, ReviewHTMLParser


def scrape_single_facility_reviews(facility_name: str, headless: bool = False):
    """
    Scrape reviews for a single facility
    
    Args:
        facility_name: Name of the facility to search
        headless: Run browser in headless mode (default: False for debugging)
    """
    
    print(f"\n{'='*70}")
    print(f"SCRAPING REVIEWS FOR: {facility_name}")
    print(f"{'='*70}\n")
    
    scraper = NaverMapsReviewScraper(headless=headless)
    parser = ReviewHTMLParser()
    
    try:
        # Setup driver
        scraper.setup_driver()
        print("✓ Browser initialized")
        
        # Navigate to search
        from urllib.parse import quote
        encoded_name = quote(facility_name)
        search_url = f"https://map.naver.com/p/search/{encoded_name}"
        
        print(f"🔍 Navigating to: {search_url}")
        scraper.driver.get(search_url)
        
        # Wait for page load
        import time
        time.sleep(3)
        
        # Try to switch to detail frame
        print("\n📍 Switching to detail view...")
        try:
            from utils.frame_switch import switch_right
            switch_right(scraper.driver)
            print("✓ Switched to detail frame")
        except Exception as e:
            print(f"⚠ Could not switch frames (might already be on detail page): {e}")
        
        time.sleep(2)
        
        # Click review tab
        print("\n📑 Clicking review tab...")
        if not scraper.click_review_tab():
            print("✗ Failed to click review tab")
            return None
        
        print("✓ Review tab opened")
        time.sleep(2)
        
        # Expand all reviews
        print("\n📂 Expanding all reviews...")
        click_count = scraper.click_expand_all_reviews()
        print(f"✓ Clicked expand button {click_count} times")
        
        time.sleep(2)
        
        # Extract HTML
        print("\n📄 Extracting review HTML...")
        review_html = scraper.extract_review_list_html()
        
        if not review_html:
            print("✗ Could not extract review HTML")
            return None
        
        print(f"✓ Extracted HTML ({len(review_html)} characters)")
        
        # Parse reviews
        print("\n⚙️  Parsing reviews...")
        reviews = parser.parse_review_list(review_html)
        
        print(f"\n{'='*70}")
        print(f"RESULTS")
        print(f"{'='*70}")
        print(f"Total reviews found: {len(reviews)}")
        
        # Display sample reviews
        if reviews:
            print(f"\n{'='*70}")
            print("SAMPLE REVIEWS")
            print(f"{'='*70}\n")
            
            for i, review in enumerate(reviews[:3], 1):  # Show first 3 reviews
                print(f"Review #{i}:")
                print(f"  Reviewer: {review.get('reviewer_info', {}).get('reviewer_name', 'N/A')}")
                print(f"  Visit Date: {review.get('visit_info', {}).get('visit_date', 'N/A')}")
                print(f"  Text: {review.get('review_text', 'N/A')[:100]}...")
                print(f"  Images: {len(review.get('images', []))} images")
                print(f"  Has Owner Response: {review.get('owner_response') is not None}")
                print()
            
            # Save to JSON file
            output_file = f"reviews_{facility_name.replace(' ', '_')}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(reviews, f, ensure_ascii=False, indent=2)
            
            print(f"💾 Saved all reviews to: {output_file}")
        
        return reviews
        
    except Exception as e:
        print(f"\n✗ Error during scraping: {e}")
        import traceback
        traceback.print_exc()
        return None
        
    finally:
        # Close browser
        print("\n🔒 Closing browser...")
        scraper.close_driver()
        print("✓ Done!")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_single_review_scrape.py 'facility_name'")
        print("Example: python test_single_review_scrape.py '이선생치과의원'")
        sys.exit(1)
    
    facility_name = sys.argv[1]
    
    # Run with visible browser for debugging
    # Change headless=True if you want to run without seeing browser
    reviews = scrape_single_facility_reviews(facility_name, headless=False)
    
    if reviews:
        print(f"\n✅ Successfully scraped {len(reviews)} reviews!")
    else:
        print("\n❌ Failed to scrape reviews")
