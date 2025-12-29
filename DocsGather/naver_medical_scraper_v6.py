#!/usr/bin/env python3
"""
Naver Maps Medical Facility Scraper V6
Gets li elements directly from DOM structure:
searchIframe > div#_pcmap_list_scroll_container > ul > li
"""

import time
import json
import csv
import random
from urllib.parse import quote
from typing import List, Dict
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Import frame switching utilities
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from utils.frame_switch import switch_left, switch_right


class NaverMedicalScraperV6:
    """
    Medical facility scraper using DOM-based li element extraction
    Structure: searchIframe > div#_pcmap_list_scroll_container > ul > li
    """
    
    def __init__(self, headless: bool = False):
        self.driver = self._setup_driver(headless)
    
    def _setup_driver(self, headless: bool):
        """Setup Chrome WebDriver"""
        options = webdriver.ChromeOptions()
        
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3')
        options.add_argument('window-size=1380,900')
        
        if headless:
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
        
        driver = webdriver.Chrome(options=options)
        driver.implicitly_wait(time_to_wait=3)
        
        return driver
    
    def scroll_list_to_bottom(self):
        """
        Scroll the search results list to load all items
        """
        scroll_container = self.driver.find_element(By.ID, "_pcmap_list_scroll_container")
        prev_height = self.driver.execute_script("return arguments[0].scrollHeight", scroll_container)
        
        print("    📜 Scrolling to load all results...")
        
        while True:
            self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scroll_container)
            time.sleep(0.5)
            new_height = self.driver.execute_script("return arguments[0].scrollHeight", scroll_container)
            
            if new_height == prev_height:
                break
            prev_height = new_height
        
        print("    ✓ Finished scrolling")
    
    def get_list_items(self) -> List:
        """
        Get all li elements from the list structure:
        searchIframe > div#_pcmap_list_scroll_container > ul > li
        
        Returns:
            List of li WebElements
        """
        try:
            # Get the scroll container
            scroll_container = self.driver.find_element(By.ID, "_pcmap_list_scroll_container")
            
            # Get the ul element inside it
            ul_element = scroll_container.find_element(By.TAG_NAME, "ul")
            
            # Get all li elements inside the ul
            li_elements = ul_element.find_elements(By.TAG_NAME, "li")
            
            # Filter out empty or invalid li elements
            valid_lis = []
            for li in li_elements:
                # Check if li has content (not just a placeholder)
                if li.text.strip() or li.find_elements(By.TAG_NAME, "a"):
                    valid_lis.append(li)
            
            print(f"    ✓ Found {len(valid_lis)} valid li elements")
            
            return valid_lis
            
        except Exception as e:
            print(f"    ✗ Error getting li elements: {e}")
            return []
    
    def get_facility_name_from_li(self, li_element) -> str:
        """
        Extract facility name from li element
        """
        try:
            # Try to find name in various possible locations
            selectors = [
                "span.TYaxT",
                "span.place_bluelink span",
                "a span",
                "div span"
            ]
            
            for selector in selectors:
                try:
                    name_elem = li_element.find_element(By.CSS_SELECTOR, selector)
                    name = name_elem.text.strip()
                    if name:
                        return name
                except:
                    continue
            
            # Fallback: get first line of text
            text = li_element.text.strip()
            if text:
                return text.split('\n')[0]
            
            return "Unknown"
            
        except:
            return "Unknown"
    
    def get_clickable_link_from_li(self, li_element):
        """
        Get the clickable link element from li
        """
        try:
            # Try to find the main link
            selectors = [
                "a.tzwk0",
                "a.place_bluelink",
                "a[href]"
            ]
            
            for selector in selectors:
                try:
                    link = li_element.find_element(By.CSS_SELECTOR, selector)
                    if link.is_displayed():
                        return link
                except:
                    continue
            
            # Last resort: find any clickable element
            links = li_element.find_elements(By.TAG_NAME, "a")
            for link in links:
                if link.is_displayed():
                    return link
            
            return None
            
        except:
            return None
    
    def extract_facility_details(self) -> Dict:
        """
        Extract detailed information from detail page (right frame)
        """
        info = {}
        
        try:
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.place_section'))
            )
            
            # Extract name
            try:
                name_elem = self.driver.find_element(By.CSS_SELECTOR, 'span.GHAhO')
                info['name'] = name_elem.text.strip()
            except:
                info['name'] = 'N/A'
            
            # Extract category
            try:
                category_elem = self.driver.find_element(By.CSS_SELECTOR, 'span.lnJFt')
                info['category'] = category_elem.text.strip()
            except:
                info['category'] = 'N/A'
            
            # Extract reviews
            try:
                review_elems = self.driver.find_elements(By.CSS_SELECTOR, 'span.PXMot')
                reviews = [elem.text.strip() for elem in review_elems if elem.text.strip()]
                info['reviews'] = ', '.join(reviews) if reviews else 'N/A'
            except:
                info['reviews'] = 'N/A'
            
            # Extract address
            try:
                addr_elem = self.driver.find_element(By.CSS_SELECTOR, 'span.LDgIH')
                info['address'] = addr_elem.text.strip()
            except:
                info['address'] = 'N/A'
            
            # Extract phone
            try:
                phone_elem = self.driver.find_element(By.CSS_SELECTOR, 'span.xlx7Q')
                info['phone'] = phone_elem.text.strip()
            except:
                info['phone'] = 'N/A'
            
            # Extract hours status
            try:
                status_elem = self.driver.find_element(By.CSS_SELECTOR, 'div.w9QyJ em')
                info['hours_status'] = status_elem.text.strip()
            except:
                info['hours_status'] = 'N/A'
            
            # Try to expand and get detailed hours
            try:
                expand_btn = self.driver.find_element(By.CSS_SELECTOR, 'a.gKP9i.RMgN0')
                if expand_btn.is_displayed():
                    expand_btn.click()
                    time.sleep(0.5)
                    
                    hours_list = []
                    days = self.driver.find_elements(By.CSS_SELECTOR, 'div.w9QyJ')
                    for day in days:
                        try:
                            day_name = day.find_element(By.CSS_SELECTOR, 'span.i8cJw').text.strip()
                            hours_info = day.find_element(By.CSS_SELECTOR, 'div.H3ua4').text.strip()
                            if day_name:
                                hours_list.append(f"{day_name}: {hours_info}")
                        except:
                            continue
                    
                    info['business_hours'] = '; '.join(hours_list) if hours_list else 'N/A'
            except:
                info['business_hours'] = 'N/A'
            
            # Extract amenities
            try:
                amenities_elem = self.driver.find_element(By.CSS_SELECTOR, 'div.xPvPE')
                info['amenities'] = amenities_elem.text.strip()
            except:
                info['amenities'] = 'N/A'
            
            # Extract website
            try:
                website_elem = self.driver.find_element(By.CSS_SELECTOR, 'a.CHmqa')
                info['website'] = website_elem.get_attribute('href')
            except:
                info['website'] = 'N/A'
            
            # Get URL and place ID
            current_url = self.driver.current_url
            info['url'] = current_url
            
            import re
            match = re.search(r'/place/(\d+)', current_url)
            info['place_id'] = match.group(1) if match else 'N/A'
            
            info['scraped_at'] = datetime.now().isoformat()
            
        except Exception as e:
            print(f"    ✗ Error extracting details: {e}")
            info['error'] = str(e)
        
        return info
    
    def scrape_location(self, query: str, location: str, max_pages: int = None) -> List[Dict]:
        """
        Scrape all facilities for a query and location
        Gets li elements from: div#_pcmap_list_scroll_container > ul > li
        
        Args:
            query: Search query (e.g., '병원', '의원')
            location: Location (e.g., '길동')
            max_pages: Maximum pages to scrape (None = all)
        """
        search_term = f"{location} {query}"
        encoded_term = quote(search_term)
        URL = f"https://map.naver.com/v5/search/{encoded_term}"
        
        print(f"\n{'='*60}")
        print(f"🔍 검색: {search_term}")
        print(f"🔗 URL: {URL}")
        print(f"{'='*60}")
        
        self.driver.get(URL)
        time.sleep(2)
        
        # Switch to left frame (search results)
        print("\n📍 Switching to searchIframe...")
        switch_left(self.driver)
        
        all_collected_names = []
        all_facility_data = []
        page_num = 0
        
        while True:
            page_num += 1
            
            if max_pages and page_num > max_pages:
                print(f"\n🛑 Reached max pages limit ({max_pages})")
                break
            
            print(f"\n{'='*60}")
            print(f"📄 Page {page_num}")
            print(f"{'='*60}")
            
            # Scroll to load all results on this page
            self.scroll_list_to_bottom()
            
            # Get all li elements from: div#_pcmap_list_scroll_container > ul > li
            print("\n    📋 Getting li elements from DOM...")
            li_elements = self.get_list_items()
            
            if not li_elements:
                print("    ⚠️  No li elements found on this page")
                break
            
            print(f"    🖱️  Processing {len(li_elements)} facilities...")
            
            # Process each li element
            for idx, li in enumerate(li_elements, 1):
                try:
                    # Get facility name from li
                    facility_name = self.get_facility_name_from_li(li)
                    
                    # Check if already processed
                    if facility_name in all_collected_names:
                        print(f"\n    [{idx}/{len(li_elements)}] ⏭️  Skipping (duplicate): {facility_name}")
                        continue
                    
                    all_collected_names.append(facility_name)
                    
                    print(f"\n    [{idx}/{len(li_elements)}] 🖱️  Clicking: {facility_name}")
                    
                    # Get clickable link from li
                    link = self.get_clickable_link_from_li(li)
                    
                    if not link:
                        print(f"        ✗ No clickable link found")
                        continue
                    
                    # Click the link
                    link.click()
                    time.sleep(random.uniform(1, 2))
                    
                    # Switch to right frame (detail page)
                    switch_right(self.driver)
                    
                    # Extract facility details
                    facility_data = self.extract_facility_details()
                    
                    # Add metadata
                    facility_data['search_query'] = query
                    facility_data['search_location'] = location
                    facility_data['page_number'] = page_num
                    facility_data['position'] = len(all_facility_data) + 1
                    facility_data['preview_name'] = facility_name
                    
                    all_facility_data.append(facility_data)
                    
                    print(f"        ✓ Extracted: {facility_data.get('name', 'N/A')}")
                    print(f"        📍 Place ID: {facility_data.get('place_id', 'N/A')}")
                    print(f"        🔗 URL: {facility_data.get('url', 'N/A')}")
                    
                    # Random delay
                    time.sleep(random.uniform(1, 2))
                    
                    # Switch back to left frame
                    switch_left(self.driver)
                    
                except Exception as e:
                    print(f"        ✗ Error: {e}")
                    # Make sure we're back in left frame
                    try:
                        switch_left(self.driver)
                    except:
                        pass
                    continue
            
            print(f"\n✅ Page {page_num} complete: {len(all_collected_names) - len([n for n in all_collected_names[:len(all_collected_names)-len(li_elements)]])} new facilities")
            
            # Try to go to next page
            try:
                list_buttons = self.driver.find_elements(By.CSS_SELECTOR, "div.zRM9F a[target='_self']")
                
                if not list_buttons:
                    print("\n🏁 No pagination buttons found")
                    break
                
                next_list_button = list_buttons[-1]
                disabled = next_list_button.get_attribute("aria-disabled")
                
                if disabled == "true":
                    print("\n🏁 Next button is disabled - no more pages")
                    break
                else:
                    print(f"\n➡️  Clicking next page...")
                    next_list_button.click()
                    time.sleep(random.uniform(1, 2))
            
            except Exception as e:
                print(f"\n⚠️  Error finding next button: {e}")
                break
        
        print(f"\n{'='*60}")
        print(f"✅ SCRAPING COMPLETE")
        print(f"   Total pages: {page_num}")
        print(f"   Total facilities: {len(all_facility_data)}")
        print(f"   Unique names: {len(all_collected_names)}")
        print(f"{'='*60}")
        
        return all_facility_data
    
    def save_to_json(self, data: List[Dict], filename: str):
        """Save to JSON"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Saved to {filename}")
    
    def save_to_csv(self, data: List[Dict], filename: str):
        """Save to CSV"""
        if not data:
            return
        
        # Collect ALL possible keys from ALL records (not just first one)
        all_keys = set()
        for record in data:
            all_keys.update(record.keys())
        
        # Convert to sorted list for consistent column order
        fieldnames = sorted(list(all_keys))
        
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(data)
        print(f"💾 Saved to {filename}")
    
    def close(self):
        """Close browser"""
        if self.driver:
            self.driver.quit()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main():
    """Example usage"""
    
    query = '의원'
    location = '길동'
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    with NaverMedicalScraperV6(headless=False) as scraper:
        results = scraper.scrape_location(
            query=query,
            location=location,
            max_pages=2
        )
        
        if results:
            scraper.save_to_json(results, f'medical_v6_{timestamp}.json')
            scraper.save_to_csv(results, f'medical_v6_{timestamp}.csv')
            
            print(f"\n📊 Summary:")
            print(f"   Total: {len(results)}")
            print(f"   With phone: {sum(1 for r in results if r.get('phone') != 'N/A')}")
            print(f"   With address: {sum(1 for r in results if r.get('address') != 'N/A')}")


if __name__ == "__main__":
    main()