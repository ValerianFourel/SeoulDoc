#!/usr/bin/env python3
"""
Naver Maps Review Scraper
Scrapes reviews from Naver Maps medical facilities
Searches by name and matches place_id from parquet file
"""

import pandas as pd
import time
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from urllib.parse import quote
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options

# Import frame switching utilities (assuming you have this)
import sys
sys.path.insert(0, os.path.dirname(__file__))
from utils.frame_switch import switch_right


# ============================================================================
# REVIEW HTML PARSER
# ============================================================================

class ReviewHTMLParser:
    """Parse review HTML into structured data"""
    
    @staticmethod
    def extract_review_images(review_elem) -> List[str]:
        """Extract image URLs from review"""
        images = []
        try:
            # Look for lazyload-wrapper containing images
            lazyload = review_elem.find('div', class_='lazyload-wrapper')
            if lazyload:
                # Find all img tags
                img_tags = lazyload.find_all('img')
                for img in img_tags:
                    src = img.get('src', '')
                    if src and not src.startswith('data:image'):  # Skip base64 images
                        images.append(src)
        except Exception as e:
            print(f"          ⚠ Error extracting images: {e}")
        
        return images
    
    @staticmethod
    def extract_reviewer_info(review_elem) -> Dict:
        """Extract reviewer information"""
        info = {}
        try:
            # Reviewer name
            name_elem = review_elem.find('span', class_='pui__NMi-Dp')
            if name_elem:
                info['reviewer_name'] = name_elem.get_text(strip=True)
            
            # Reviewer stats (리뷰 X, 사진 Y)
            stats = review_elem.find_all('span', class_='pui__WN-kAf')
            if stats:
                info['reviewer_stats'] = [s.get_text(strip=True) for s in stats]
            
            # Profile link
            profile_link = review_elem.find('a', {'data-pui-click-code': 'profile'})
            if profile_link:
                info['profile_url'] = profile_link.get('href', '')
                
        except Exception as e:
            print(f"          ⚠ Error extracting reviewer info: {e}")
        
        return info
    
    @staticmethod
    def extract_visit_keywords(review_elem) -> List[str]:
        """Extract visit keywords (예약 후 이용, 대기 시간 등)"""
        keywords = []
        try:
            keyword_elems = review_elem.find_all('span', class_='pui__V8F9nN')
            for elem in keyword_elems:
                text = elem.get_text(strip=True)
                if text:
                    keywords.append(text)
        except Exception as e:
            print(f"          ⚠ Error extracting keywords: {e}")
        
        return keywords
    
    @staticmethod
    def extract_review_text(review_elem) -> str:
        """Extract review text content"""
        try:
            # Look for review text in pui__vn15t2
            text_div = review_elem.find('div', class_='pui__vn15t2')
            if text_div:
                # Get text from anchor tag
                text_elem = text_div.find('a', {'data-pui-click-code': 'rvshowmore'})
                if text_elem:
                    # Get all text including line breaks
                    return text_elem.get_text(separator='\n', strip=True)
        except Exception as e:
            print(f"          ⚠ Error extracting review text: {e}")
        
        return ""
    
    @staticmethod
    def extract_review_date(review_elem) -> Dict:
        """Extract visit date and review submission info"""
        dates = {}
        try:
            # Visit date
            date_elems = review_elem.find_all('span', class_='pui__gfuUIT')
            for elem in date_elems:
                text = elem.get_text(strip=True)
                if '방문일' in str(elem):
                    time_elem = elem.find('time')
                    if time_elem:
                        dates['visit_date'] = time_elem.get_text(strip=True)
                elif '번째 방문' in text:
                    dates['visit_count'] = text
                elif '인증' in text:
                    dates['verification_method'] = text
        except Exception as e:
            print(f"          ⚠ Error extracting dates: {e}")
        
        return dates
    
    @staticmethod
    def extract_owner_response(review_elem) -> Optional[Dict]:
        """Extract owner's response if exists"""
        try:
            response_div = review_elem.find('div', class_='pui__GbW8H7')
            if response_div:
                response = {}
                
                # Owner name
                owner_name = response_div.find('span', class_='pui__XE54q7')
                if owner_name:
                    response['owner_name'] = owner_name.get_text(strip=True)
                
                # Response date
                response_date = response_div.find('span', class_='pui__4APmFd')
                if response_date:
                    time_elem = response_date.find('time')
                    if time_elem:
                        response['response_date'] = time_elem.get_text(strip=True)
                
                # Response text
                text_div = response_div.find('div', class_='pui__J0tczd')
                if text_div:
                    # Try to get from anchor or span
                    text_elem = text_div.find(['a', 'span'], {'data-pui-click-code': 'text'})
                    if text_elem:
                        response['response_text'] = text_elem.get_text(separator='\n', strip=True)
                    else:
                        # Fallback to getting all text
                        response['response_text'] = text_div.get_text(separator='\n', strip=True)
                
                return response
        except Exception as e:
            print(f"          ⚠ Error extracting owner response: {e}")
        
        return None
    
    @staticmethod
    def extract_reactions(review_elem) -> Dict:
        """Extract reactions/likes count"""
        reactions = {}
        try:
            # Look for reaction counts
            reaction_div = review_elem.find('div', class_='pui__l8k0-f')
            if reaction_div:
                count_elem = reaction_div.find('em', class_='pui__x-pa-u')
                if count_elem:
                    reactions['reaction_count'] = count_elem.get_text(strip=True)
        except Exception as e:
            print(f"          ⚠ Error extracting reactions: {e}")
        
        return reactions
    
    @staticmethod
    def parse_single_review(review_elem) -> Dict:
        """Parse a single review element into structured data"""
        review_data = {
            'scraped_at': datetime.now().isoformat()
        }
        
        # Extract all components
        review_data['reviewer_info'] = ReviewHTMLParser.extract_reviewer_info(review_elem)
        review_data['review_text'] = ReviewHTMLParser.extract_review_text(review_elem)
        review_data['visit_info'] = ReviewHTMLParser.extract_review_date(review_elem)
        review_data['visit_keywords'] = ReviewHTMLParser.extract_visit_keywords(review_elem)
        review_data['images'] = ReviewHTMLParser.extract_review_images(review_elem)
        review_data['owner_response'] = ReviewHTMLParser.extract_owner_response(review_elem)
        review_data['reactions'] = ReviewHTMLParser.extract_reactions(review_elem)
        
        return review_data
    
    @staticmethod
    def parse_review_list(html_content: str) -> List[Dict]:
        """Parse entire review list HTML"""
        reviews = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find review list
            review_list = soup.find('ul', id='_review_list')
            if not review_list:
                print("          ⚠ No review list found")
                return reviews
            
            # Find all review items
            review_items = review_list.find_all('li', class_='place_apply_pui')
            
            print(f"          ✓ Found {len(review_items)} reviews in HTML")
            
            for idx, review_elem in enumerate(review_items, 1):
                try:
                    review_data = ReviewHTMLParser.parse_single_review(review_elem)
                    review_data['review_index'] = idx
                    reviews.append(review_data)
                except Exception as e:
                    print(f"          ⚠ Error parsing review {idx}: {e}")
            
        except Exception as e:
            print(f"          ✗ Error parsing review list: {e}")
        
        return reviews


# ============================================================================
# REVIEW SCRAPER
# ============================================================================

class NaverMapsReviewScraper:
    """Scrape reviews from Naver Maps"""
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = None
        self.wait = None
        self.parser = ReviewHTMLParser()
    
    def setup_driver(self):
        """Setup Chrome WebDriver"""
        options = Options()
        
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        options.add_argument('--window-size=1380,900')
        
        if self.headless:
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
        
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(3)
        self.wait = WebDriverWait(self.driver, 10)
    
    def close_driver(self):
        """Close the driver"""
        if self.driver:
            self.driver.quit()
    
    def clean_place_id(self, place_id) -> str:
        """
        Clean place_id by removing .0 if present
        Examples: 123.0 -> "123", "123.0" -> "123", 123 -> "123"
        """
        place_id_str = str(place_id)
        if place_id_str.endswith('.0'):
            place_id_str = place_id_str[:-2]
        return place_id_str
    
    def detect_iframe_structure(self) -> str:
        """
        Detect which iframe structure we have
        Returns: 'single' (only entryIframe), 'dual' (both iframes), or 'none'
        """
        try:
            self.driver.switch_to.default_content()
            
            has_entry = False
            has_search = False
            
            try:
                self.driver.find_element(By.ID, "entryIframe")
                has_entry = True
            except NoSuchElementException:
                pass
            
            try:
                self.driver.find_element(By.ID, "searchIframe")
                has_search = True
            except NoSuchElementException:
                pass
            
            if has_entry and has_search:
                return 'dual'
            elif has_entry and not has_search:
                return 'single'
            else:
                return 'none'
                
        except Exception as e:
            print(f"        ⚠ Error detecting iframe structure: {e}")
            return 'none'
    
    def switch_to_entry_iframe(self) -> bool:
        """
        Switch to entry iframe using multiple methods
        Works for both single and dual iframe scenarios
        """
        try:
            self.driver.switch_to.default_content()
            
            # Method 1: Try using switch_right utility
            try:
                switch_right(self.driver)
                print(f"        ✓ Switched using switch_right()")
                return True
            except Exception as e1:
                print(f"        ℹ️  switch_right failed: {e1}")
                
                # Method 2: Direct frame switch by ID
                try:
                    self.driver.switch_to.default_content()
                    self.driver.switch_to.frame("entryIframe")
                    print(f"        ✓ Switched using direct frame ID")
                    return True
                except Exception as e2:
                    print(f"        ℹ️  Direct switch failed: {e2}")
                    
                    # Method 3: Find and switch to frame element
                    try:
                        self.driver.switch_to.default_content()
                        iframe = self.driver.find_element(By.ID, "entryIframe")
                        self.driver.switch_to.frame(iframe)
                        print(f"        ✓ Switched using frame element")
                        return True
                    except Exception as e3:
                        print(f"        ✗ All switch methods failed: {e3}")
                        return False
            
        except Exception as e:
            print(f"        ✗ Error switching to entry iframe: {e}")
            return False
    
    def extract_place_id_from_url(self) -> Optional[str]:
        """Extract place_id from current URL"""
        try:
            current_url = self.driver.current_url
            match = re.search(r'/place/(\d+)', current_url)
            if match:
                return match.group(1)
        except Exception as e:
            print(f"           ⚠ Error extracting place_id: {e}")
        return None
    
    def navigate_to_place_direct(self, facility_name: str, place_id: str) -> bool:
        """
        Navigate directly to place using combined name+place_id URL
        URL format: https://map.naver.com/p/search/{encoded_name}/place/{place_id}
        
        Handles TWO scenarios:
        1. Single iframe: Direct navigation to entryIframe (no searchIframe)
        2. Dual iframes: Both searchIframe and entryIframe exist
        
        Args:
            facility_name: Name to encode in URL
            place_id: Place ID (will be cleaned)
        
        Returns:
            True if navigation successful and on detail page
        """
        try:
            # Clean place_id
            clean_id = self.clean_place_id(place_id)
            
            # Encode name for URL
            encoded_name = quote(facility_name)
            
            # Direct URL with both name and place_id
            direct_url = f"https://map.naver.com/p/search/{encoded_name}/place/{clean_id}"
            
            print(f"        🔗 Direct URL: {direct_url}")
            
            # Reset to default content
            try:
                self.driver.switch_to.default_content()
            except:
                pass
            
            # Navigate to direct URL
            self.driver.get(direct_url)
            time.sleep(3)
            
            # Detect iframe structure
            iframe_structure = self.detect_iframe_structure()
            print(f"        📊 Iframe structure: {iframe_structure}")
            
            if iframe_structure == 'none':
                print(f"        ✗ No iframes found - place may not exist")
                return False
            
            # For both 'single' and 'dual', we need to switch to entryIframe
            if iframe_structure in ['single', 'dual']:
                print(f"        🎯 Switching to detail page...")
                
                # Use robust switching method
                if not self.switch_to_entry_iframe():
                    print(f"        ✗ Could not switch to entry iframe")
                    return False
                
                time.sleep(1)
                
                # Verify detail page content loaded
                try:
                    self.wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div.place_section'))
                    )
                    print(f"        ✅ Detail page loaded successfully")
                    
                    # Verify the place_id in URL matches what we expect
                    current_url = self.driver.current_url
                    if clean_id in current_url:
                        print(f"        ✅ Confirmed place_id: {clean_id}")
                        return True
                    else:
                        print(f"        ⚠ URL doesn't contain expected place_id")
                        # Still return True if detail page loaded
                        return True
                        
                except TimeoutException:
                    print(f"        ⚠ Detail page content didn't load (timeout)")
                    return False
                    
            return False
                
        except Exception as e:
            print(f"        ✗ Navigation error: {e}")
            return False
    
    def click_review_tab(self) -> bool:
        """Click on the review tab with retry logic"""
        try:
            print("        🔍 Looking for review tab...")
            
            # Wait for tab menu to be present
            try:
                self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'a[data-index="1"].tpj9w._tab-menu'))
                )
            except:
                print("        ⚠ Timeout waiting for tabs")
                return False
            
            # Find review tab
            review_tab = self.driver.find_element(
                By.CSS_SELECTOR, 
                'a[data-index="1"].tpj9w._tab-menu'
            )
            
            # Verify it contains "리뷰"
            if '리뷰' not in review_tab.text:
                print("        ⚠ Tab doesn't contain '리뷰'")
                return False
            
            print("        ✓ Found review tab")
            
            # Scroll tab into view
            self.driver.execute_script(
                "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                review_tab
            )
            time.sleep(0.5)
            
            # Click with retries
            for attempt in range(3):
                try:
                    review_tab.click()
                    time.sleep(2)
                    return True
                except Exception as e:
                    if attempt < 2:
                        print(f"        ⚠ Click attempt {attempt+1} failed, retrying...")
                        time.sleep(1)
                        # Try JavaScript click
                        try:
                            self.driver.execute_script("arguments[0].click();", review_tab)
                            time.sleep(2)
                            return True
                        except:
                            continue
                    else:
                        print(f"        ✗ Error clicking review tab: {e}")
                        return False
            
            return False
            
        except NoSuchElementException:
            print("        ✗ Review tab not found")
            return False
        except Exception as e:
            print(f"        ✗ Error finding review tab: {e}")
            return False
    
    def click_expand_all_reviews(self) -> int:
        """Click 'expand more' button until all reviews are loaded"""
        click_count = 0
        max_attempts = 100  # Safety limit
        
        print("        📂 Expanding all reviews...")
        
        for attempt in range(max_attempts):
            try:
                # Find the "펼쳐서 더보기" button
                expand_button = self.driver.find_element(
                    By.CSS_SELECTOR,
                    'a.fvwqf'
                )
                
                # Verify it contains the expand text
                if '펼쳐서 더보기' in expand_button.text or '더보기' in expand_button.text:
                    # Scroll into view
                    self.driver.execute_script(
                        "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                        expand_button
                    )
                    time.sleep(0.5)
                    
                    # Click the button
                    try:
                        expand_button.click()
                    except:
                        # Use JavaScript click if regular click fails
                        self.driver.execute_script("arguments[0].click();", expand_button)
                    
                    click_count += 1
                    time.sleep(1)  # Wait for reviews to load
                    
                    if click_count % 10 == 0:
                        print(f"        ✓ Clicked expand button {click_count} times")
                else:
                    # Button text changed, might be at the end
                    break
                    
            except (NoSuchElementException, StaleElementReferenceException):
                # Button no longer exists - all reviews loaded
                print(f"        ✓ All reviews expanded ({click_count} clicks)")
                break
            except Exception as e:
                print(f"        ⚠ Error during expansion: {e}")
                break
        
        if click_count >= max_attempts:
            print(f"        ⚠ Reached maximum attempts ({max_attempts})")
        
        return click_count
    
    def extract_review_list_html(self) -> Optional[str]:
        """Extract the review list HTML"""
        try:
            # Find the review list element
            review_list = self.driver.find_element(By.ID, '_review_list')
            
            # Get outer HTML to include the ul element itself
            html_content = review_list.get_attribute('outerHTML')
            
            return html_content
            
        except NoSuchElementException:
            print("        ✗ Review list element not found")
            return None
        except Exception as e:
            print(f"        ✗ Error extracting review HTML: {e}")
            return None
    
    def scrape_reviews_for_facility(self, facility_name: str, place_id: str) -> Dict:
        """Scrape all reviews for a single facility using direct URL"""
        result = {
            'has_reviews': False,
            'review_count': 0,
            'reviews': [],
            'review_html': None,
            'scrape_error': None,
            'scraped_at': datetime.now().isoformat()
        }
        
        try:
            print(f"      🔍 Scraping: {facility_name}")
            
            # Navigate directly to place using name+place_id URL
            if not self.navigate_to_place_direct(facility_name, place_id):
                result['scrape_error'] = "Could not navigate to place"
                return result
            
            # We're now on the detail page (already in entryIframe)
            print("        ✓ On detail page")
            
            # Wait for page to load
            try:
                self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.place_section'))
                )
                time.sleep(1)
            except TimeoutException:
                print("        ⚠ Timeout waiting for page")
                result['scrape_error'] = "Page load timeout"
                return result
            
            # Click review tab
            if not self.click_review_tab():
                result['scrape_error'] = "Could not click review tab"
                return result
            
            # Wait for reviews
            time.sleep(2)
            
            # Expand all reviews
            expand_clicks = self.click_expand_all_reviews()
            print(f"        ✓ Expanded with {expand_clicks} clicks")
            
            # Extra wait
            time.sleep(1)
            
            # Extract review HTML
            review_html = self.extract_review_list_html()
            
            if not review_html:
                result['scrape_error'] = "Could not extract review HTML"
                return result
            
            result['review_html'] = review_html
            
            # Parse reviews
            print("        ⚙️  Parsing reviews...")
            reviews = self.parser.parse_review_list(review_html)
            
            if reviews:
                result['has_reviews'] = True
                result['review_count'] = len(reviews)
                result['reviews'] = reviews
                print(f"        ✅ Parsed {len(reviews)} reviews")
            else:
                print("        ⚠ No reviews found")
            
            return result
            
        except Exception as e:
            print(f"        ✗ Error: {e}")
            result['scrape_error'] = str(e)
            return result


# ============================================================================
# CHECKPOINT MANAGER
# ============================================================================

class ReviewCheckpointManager:
    """Manage review scraping progress using JSON file"""
    
    def __init__(self, checkpoint_file="./data/review_scraping_progress.json"):
        self.checkpoint_file = Path(checkpoint_file)
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        self.progress_data = {}
        
        if self.checkpoint_file.exists():
            self.load_progress()
    
    def load_progress(self):
        """Load existing progress from JSON"""
        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                self.progress_data = json.load(f)
            print(f"✓ Loaded existing progress: {len(self.progress_data)} facilities")
        except Exception as e:
            print(f"⚠ Could not load progress file: {e}")
            self.progress_data = {}
    
    def save_progress(self):
        """Save current progress to JSON"""
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"✗ Error saving progress: {e}")
    
    def is_processed(self, place_id: str) -> bool:
        """Check if a place_id has been processed"""
        return place_id in self.progress_data
    
    def add_facility(self, place_id: str, review_data: Dict):
        """Add facility review data to progress"""
        self.progress_data[place_id] = review_data
    
    def get_stats(self) -> Dict:
        """Get statistics about current progress"""
        total = len(self.progress_data)
        with_reviews = sum(1 for v in self.progress_data.values() if v.get('has_reviews'))
        total_reviews = sum(v.get('review_count', 0) for v in self.progress_data.values())
        
        return {
            'total_processed': total,
            'with_reviews': with_reviews,
            'total_reviews_scraped': total_reviews
        }


# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

class ReviewScrapingOrchestrator:
    """Orchestrate the review scraping process"""
    
    def __init__(self, output_dir="./data", partition_x: int = 1, partition_y: int = 1):
        """
        Initialize orchestrator with optional partitioning
        
        Args:
            output_dir: Directory for output files
            partition_x: Which partition to process (1 to partition_y)
            partition_y: Total number of partitions
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Validate partitions
        if partition_y < 1:
            raise ValueError(f"partition_y must be >= 1, got {partition_y}")
        if partition_x < 1 or partition_x > partition_y:
            raise ValueError(f"partition_x must be between 1 and {partition_y}, got {partition_x}")
        
        self.partition_x = partition_x
        self.partition_y = partition_y
        
        # Create partition-specific checkpoint file
        if partition_y > 1:
            checkpoint_file = self.output_dir / f"review_scraping_progress_p{partition_x}_of_{partition_y}.json"
            self.partition_suffix = f"_p{partition_x}_of_{partition_y}"
        else:
            checkpoint_file = self.output_dir / "review_scraping_progress.json"
            self.partition_suffix = ""
        
        self.checkpoint_mgr = ReviewCheckpointManager(checkpoint_file=checkpoint_file)
        
        if partition_y > 1:
            print(f"\n{'='*70}")
            print(f"PARTITION MODE")
            print(f"{'='*70}")
            print(f"Processing partition {partition_x} of {partition_y}")
            print(f"Checkpoint file: {checkpoint_file.name}")
            print(f"{'='*70}\n")
    
    def scrape_all_reviews(self,
                           facilities_df: pd.DataFrame,
                           save_freq: int = 5,
                           headless: bool = True) -> Dict:
        """Scrape reviews for all facilities (or partition subset)"""
        
        # Filter facilities by partition
        if self.partition_y > 1:
            # Get every Yth facility starting from position X-1 (0-indexed)
            # Example: partition_x=2, partition_y=5 → indices 1, 6, 11, 16, 21...
            facilities_df = facilities_df.reset_index(drop=True)
            partition_indices = list(range(self.partition_x - 1, len(facilities_df), self.partition_y))
            facilities_df = facilities_df.iloc[partition_indices].copy()
            
            print(f"\n{'='*70}")
            print(f"PARTITION FILTERING")
            print(f"{'='*70}")
            print(f"Partition {self.partition_x} of {self.partition_y}")
            print(f"Processing {len(facilities_df):,} facilities from this partition")
            print(f"Pattern: Every {self.partition_y}th facility starting from position {self.partition_x}")
            print(f"{'='*70}\n")
        
        scraper = NaverMapsReviewScraper(headless=headless)
        scraper.setup_driver()
        
        stats = self.checkpoint_mgr.get_stats()
        total_facilities = len(facilities_df)
        already_processed = stats['total_processed']
        
        print(f"\n{'='*70}")
        print(f"STARTING REVIEW SCRAPING")
        if self.partition_y > 1:
            print(f"PARTITION {self.partition_x}/{self.partition_y}")
        print(f"{'='*70}")
        print(f"Total facilities in partition: {total_facilities:,}")
        print(f"Already processed: {already_processed:,}")
        print(f"Remaining: {total_facilities - already_processed:,}")
        print(f"Save frequency: every {save_freq} facilities")
        print(f"{'='*70}\n")
        
        processed_count = 0
        
        try:
            for idx, row in facilities_df.iterrows():
                place_id = str(row['place_id'])
                facility_name = row.get('name', 'Unknown')
                
                # Skip if already processed
                if self.checkpoint_mgr.is_processed(place_id):
                    continue
                
                processed_count += 1
                current_total = already_processed + processed_count
                
                print(f"[{current_total}/{total_facilities}] {facility_name}")
                if self.partition_y > 1:
                    print(f"  Partition {self.partition_x}/{self.partition_y}")
                print(f"  Place ID: {place_id}")
                
                try:
                    # Scrape reviews (search and match place_id)
                    review_data = scraper.scrape_reviews_for_facility(facility_name, place_id)
                    
                    # Add to checkpoint
                    self.checkpoint_mgr.add_facility(place_id, review_data)
                    
                    if review_data['has_reviews']:
                        print(f"  ✓ Scraped {review_data['review_count']} reviews")
                    else:
                        if review_data.get('scrape_error'):
                            print(f"  ⚠ Error: {review_data['scrape_error']}")
                        else:
                            print(f"  ℹ No reviews found")
                    
                except Exception as e:
                    print(f"  ✗ Failed: {e}")
                    self.checkpoint_mgr.add_facility(place_id, {
                        'has_reviews': False,
                        'review_count': 0,
                        'reviews': [],
                        'review_html': None,
                        'scrape_error': str(e),
                        'scraped_at': datetime.now().isoformat()
                    })
                
                # Save progress periodically
                if processed_count % save_freq == 0:
                    self.checkpoint_mgr.save_progress()
                    stats = self.checkpoint_mgr.get_stats()
                    print(f"  💾 Progress saved: {stats['total_processed']:,} facilities, {stats['total_reviews_scraped']:,} total reviews")
                
                time.sleep(2)  # Polite delay
            
        finally:
            scraper.close_driver()
            self.checkpoint_mgr.save_progress()
        
        return self.checkpoint_mgr.progress_data
    
    def create_review_dataset(self, facilities_df: pd.DataFrame) -> pd.DataFrame:
        """Create flat dataset with review data"""
        records = []
        
        for place_id, review_data in self.checkpoint_mgr.progress_data.items():
            # Get facility info
            facility = facilities_df[facilities_df['place_id'].astype(str) == place_id]
            
            if len(facility) > 0:
                facility_name = facility.iloc[0]['name']
            else:
                facility_name = "Unknown"
            
            if review_data.get('has_reviews') and review_data.get('reviews'):
                # Create a record for each review
                for review in review_data['reviews']:
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
                # Create a single record for facilities with no reviews
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
                    'scraped_at': review_data.get('scraped_at')
                }
                records.append(record)
        
        return pd.DataFrame(records)
    
    def print_summary(self):
        """Print summary statistics"""
        stats = self.checkpoint_mgr.get_stats()
        
        print(f"\n{'='*70}")
        print(f"REVIEW SCRAPING SUMMARY")
        print(f"{'='*70}")
        print(f"Total facilities processed: {stats['total_processed']:,}")
        print(f"Facilities with reviews: {stats['with_reviews']:,}")
        print(f"Total reviews scraped: {stats['total_reviews_scraped']:,}")
        
        if stats['with_reviews'] > 0:
            avg_reviews = stats['total_reviews_scraped'] / stats['with_reviews']
            print(f"Average reviews per facility: {avg_reviews:.1f}")
        
        print(f"{'='*70}")


# ============================================================================
# DATASET LOADING
# ============================================================================

def load_facilities_dataset(source: str = "local") -> pd.DataFrame:
    """
    Load facilities dataset from local file or HuggingFace
    
    Args:
        source: "local" or "huggingface"
    """
    
    if source == "local":
        # Try multiple formats: CSV (most compatible), Parquet, Pickle
        facilities_file_csv = Path("./data/seoul_medical_facilities.csv")
        facilities_file_parquet = Path("./data/seoul_medical_facilities.parquet")
        facilities_file_pickle = Path("./data/seoul_medical_facilities.pkl")
        
        print("="*70)
        print("LOADING FACILITIES DATASET (LOCAL)")
        print("="*70)
        
        # Try CSV first (most compatible, no PyArrow needed)
        if facilities_file_csv.exists():
            try:
                facilities_df = pd.read_csv(facilities_file_csv)
                print(f"✓ Loaded {len(facilities_df):,} facilities from CSV")
                return facilities_df
            except Exception as e:
                print(f"⚠ Could not load CSV: {e}")
        
        # Try parquet
        if facilities_file_parquet.exists():
            try:
                facilities_df = pd.read_parquet(facilities_file_parquet)
                print(f"✓ Loaded {len(facilities_df):,} facilities from parquet")
                return facilities_df
            except Exception as e:
                print(f"⚠ Could not load parquet: {e}")
        
        # Try pickle
        if facilities_file_pickle.exists():
            try:
                facilities_df = pd.read_pickle(facilities_file_pickle)
                print(f"✓ Loaded {len(facilities_df):,} facilities from pickle")
                return facilities_df
            except Exception as e:
                print(f"⚠ Could not load pickle: {e}")
        
        # No local file found
        print(f"⚠ No local cache found")
        print(f"  Switching to HuggingFace download...")
        return load_facilities_dataset(source="huggingface")
    
    elif source == "huggingface":
        print("="*70)
        print("LOADING FACILITIES DATASET (HUGGINGFACE)")
        print("="*70)
        
        try:
            from datasets import load_dataset
        except (ImportError, ValueError) as e:
            print(f"✗ Cannot import 'datasets' library: {e}")
            print(f"\n" + "="*70)
            print("MANUAL DOWNLOAD REQUIRED")
            print("="*70)
            print(f"\nYour PyArrow installation is broken. Please either:")
            print(f"\n1. Fix PyArrow (recommended):")
            print(f"   pip uninstall pyarrow")
            print(f"   pip install pyarrow --break-system-packages")
            print(f"\n2. OR download dataset manually:")
            print(f"   Visit: https://huggingface.co/datasets/ValerianFourel/seoul-medical-facilities")
            print(f"   Download as CSV")
            print(f"   Save to: ./data/seoul_medical_facilities.csv")
            print(f"\nThen run the script again.")
            raise RuntimeError("Cannot load dataset - PyArrow error. See instructions above.")
        
        print("📥 Downloading from HuggingFace: ValerianFourel/seoul-medical-facilities")
        
        try:
            dataset = load_dataset("ValerianFourel/seoul-medical-facilities")
            facilities_df = dataset['train'].to_pandas()
        except Exception as e:
            print(f"✗ Download failed: {e}")
            print(f"\nPlease download manually:")
            print(f"1. Visit: https://huggingface.co/datasets/ValerianFourel/seoul-medical-facilities")
            print(f"2. Download as CSV")
            print(f"3. Save to: ./data/seoul_medical_facilities.csv")
            raise
        
        print(f"✓ Downloaded {len(facilities_df):,} facilities")
        
        # Save to local cache (CSV for maximum compatibility)
        cache_file_csv = Path("./data/seoul_medical_facilities.csv")
        cache_file_csv.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            facilities_df.to_csv(cache_file_csv, index=False, encoding='utf-8-sig')
            print(f"✓ Cached to: {cache_file_csv}")
        except Exception as e:
            print(f"⚠ Could not save cache: {e}")
        
        return facilities_df
    
    else:
        raise ValueError(f"Invalid source: {source}. Use 'local' or 'huggingface'")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main(partition_x: int = 1, partition_y: int = 1):
    """
    Main execution function
    
    Args:
        partition_x: Which partition to process (1 to partition_y)
        partition_y: Total number of partitions (1 = process all)
    """
    
    print("="*70)
    print("STEP 1: LOADING FACILITIES DATASET")
    print("="*70)
    
    # Try local first, fallback to HuggingFace
    facilities_df = load_facilities_dataset(source="local")
    
    # Clean and validate
    facilities_df = facilities_df[
        facilities_df['place_id'].notna() & 
        facilities_df['name'].notna()
    ].copy()
    
    print(f"✓ {len(facilities_df):,} facilities with valid place_id and name")
    
    # Filter for hospitals/clinics if needed
    if 'name' in facilities_df.columns:
        medical_facilities = facilities_df[
            facilities_df['name'].str.contains('병원|의원', na=False)
        ]
        print(f"✓ Filtered to {len(medical_facilities):,} medical facilities")
    else:
        medical_facilities = facilities_df
    
    print("\n" + "="*70)
    print("STEP 2: SCRAPING REVIEWS")
    if partition_y > 1:
        print(f"PARTITION {partition_x}/{partition_y}")
    print("="*70)
    
    orchestrator = ReviewScrapingOrchestrator(
        output_dir="./data",
        partition_x=partition_x,
        partition_y=partition_y
    )
    
    # Scrape reviews
    progress_data = orchestrator.scrape_all_reviews(
        medical_facilities,
        save_freq=10,  # Save every 10 as requested
        headless=True
    )
    
    print("\n" + "="*70)
    print("STEP 3: REVIEW SCRAPING SUMMARY")
    if partition_y > 1:
        print(f"PARTITION {partition_x}/{partition_y}")
    print("="*70)
    
    orchestrator.print_summary()
    
    print("\n" + "="*70)
    print("STEP 4: CREATING REVIEW DATASET")
    print("="*70)
    
    review_df = orchestrator.create_review_dataset(medical_facilities)
    
    # Add partition suffix to output files
    partition_suffix = orchestrator.partition_suffix
    
    output_file = Path(f"./data/seoul_medical_reviews{partition_suffix}.parquet")
    review_df.to_parquet(output_file, index=False)
    print(f"✓ Saved review dataset: {output_file}")
    print(f"  Total review records: {len(review_df):,}")
    
    # Also save as CSV for easy viewing
    csv_file = Path(f"./data/seoul_medical_reviews{partition_suffix}.csv")
    review_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print(f"✓ Saved CSV version: {csv_file}")
    
    if partition_y > 1:
        print(f"\n💡 NOTE: This is partition {partition_x}/{partition_y}")
        print(f"   Run other partitions (1-{partition_y}) to complete the full dataset")
        print(f"   Then merge all partition files together")
    
    print("\n" + "="*70)
    print("✅ REVIEW SCRAPING COMPLETE!")
    if partition_y > 1:
        print(f"   PARTITION {partition_x}/{partition_y}")
    print("="*70)
    
    return review_df


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Scrape reviews from Naver Maps with optional partitioning for parallel processing'
    )
    parser.add_argument(
        '--partition-x',
        type=int,
        default=1,
        help='Which partition to process (1 to partition-y)'
    )
    parser.add_argument(
        '--partition-y',
        type=int,
        default=1,
        help='Total number of partitions (default: 1 = process all)'
    )
    
    args = parser.parse_args()
    
    review_df = main(
        partition_x=args.partition_x,
        partition_y=args.partition_y
    )