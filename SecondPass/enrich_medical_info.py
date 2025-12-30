#!/usr/bin/env python3
"""
Seoul Medical Facilities Medical Information Enrichment
Uses JSON-based checkpoint system for resumability and HuggingFace upload
Handles click interception and single-result scenarios
"""

import pandas as pd
import time
import json
import os
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from urllib.parse import quote

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.options import Options

from groq import Groq

# Import frame switching utilities
import sys
sys.path.insert(0, os.path.dirname(__file__))
from utils.frame_switch import switch_left, switch_right


# ============================================================================
# DATASET MANAGER
# ============================================================================

class DatasetManager:
    """Manage downloading and caching of the facilities dataset"""
    
    def __init__(self, dataset_name="ValerianFourel/seoul-medical-facilities", 
                 cache_dir="./data"):
        self.dataset_name = dataset_name
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.facilities_file = self.cache_dir / "seoul_medical_facilities.parquet"
    
    def check_dataset_exists(self) -> bool:
        """Check if dataset is already downloaded"""
        exists = self.facilities_file.exists()
        if exists:
            print(f"✓ Dataset found: {self.facilities_file}")
            file_size = self.facilities_file.stat().st_size / (1024 * 1024)
            print(f"  File size: {file_size:.2f} MB")
        else:
            print(f"✗ Dataset not found: {self.facilities_file}")
        return exists
    
    def download_dataset(self) -> pd.DataFrame:
        """Download dataset from HuggingFace"""
        print(f"\nDownloading dataset from HuggingFace: {self.dataset_name}")
        try:
            from datasets import load_dataset
            
            dataset = load_dataset(self.dataset_name, split='train')
            df = dataset.to_pandas()
            
            # Save to cache
            df.to_parquet(self.facilities_file, index=False)
            print(f"✓ Downloaded and cached {len(df)} facilities")
            print(f"  Saved to: {self.facilities_file}")
            
            return df
            
        except Exception as e:
            print(f"✗ Error downloading dataset: {e}")
            raise
    
    def load_dataset(self, force_download=False) -> pd.DataFrame:
        """Load dataset, downloading if necessary"""
        if force_download or not self.check_dataset_exists():
            df = self.download_dataset()
        else:
            print(f"\nLoading cached dataset...")
            df = pd.read_parquet(self.facilities_file)
            print(f"✓ Loaded {len(df)} facilities from cache")
        
        # Validate required columns
        required_cols = ['place_id', 'name']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Dataset missing required columns: {missing_cols}")
        
        # Show dataset info
        print(f"\nDataset columns: {list(df.columns)}")
        if len(df) > 0:
            print(f"Sample facility:")
            print(df[['place_id', 'name']].head(1).to_string())
        
        return df


# ============================================================================
# MEDICAL INFO ENRICHMENT SCRAPER
# ============================================================================

class MedicalInfoEnrichmentScraper:
    """Scrape and enrich facilities with medical information using LLM"""
    
    def __init__(self, groq_api_key: str = None, headless: bool = True):
        self.headless = headless
        self.driver = None
        self.wait = None
        
        # Initialize Groq client
        if groq_api_key:
            self.groq_client = Groq(api_key=groq_api_key)
        else:
            self.groq_client = Groq()  # Get from environment
    
    def setup_driver(self):
        """Setup Chrome WebDriver"""
        options = Options()
        
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3')
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
    
    def search_and_click_first_result(self, facility_name: str) -> bool:
        """
        Search for facility by name and click first result
        Handles click interception from rapidly loading entryIframe
        
        Args:
            facility_name: Name of facility to search
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Construct search URL
            encoded_name = quote(facility_name)
            search_url = f"https://map.naver.com/p/search/{encoded_name}"
            
            print(f"        🔍 Searching: {search_url}")
            self.driver.get(search_url)
            time.sleep(3)
            
            # Check if entryIframe already exists (auto-navigated to single result)
            try:
                self.driver.switch_to.default_content()
                self.driver.find_element(By.ID, "entryIframe")
                print(f"        ✓ Auto-navigated to detail page")
                return True
            except NoSuchElementException:
                pass
            
            # Switch to search results frame (left frame)
            try:
                switch_left(self.driver)
                print(f"        ✓ Switched to searchIframe")
            except Exception as e:
                print(f"        ✗ Could not switch to searchIframe: {e}")
                return False
            
            # Wait for results to load
            time.sleep(2)
            
            # Check if scroll container exists (multiple results case)
            try:
                scroll_container = self.driver.find_element(By.ID, "_pcmap_list_scroll_container")
                ul_element = scroll_container.find_element(By.TAG_NAME, "ul")
                li_elements = ul_element.find_elements(By.TAG_NAME, "li")
                
                if not li_elements:
                    print(f"        ✗ No search results found")
                    return False
                
                # Get first valid li element
                first_li = None
                for li in li_elements:
                    if li.text.strip() or li.find_elements(By.TAG_NAME, "a"):
                        first_li = li
                        break
                
                if not first_li:
                    print(f"        ✗ No valid results found")
                    return False
                
                # Find clickable link in first result
                link = None
                selectors = ["a.tzwk0", "a.place_bluelink", "a[href]"]
                
                for selector in selectors:
                    try:
                        link = first_li.find_element(By.CSS_SELECTOR, selector)
                        if link.is_displayed():
                            break
                    except:
                        continue
                
                if not link:
                    # Try any link
                    links = first_li.find_elements(By.TAG_NAME, "a")
                    for l in links:
                        if l.is_displayed():
                            link = l
                            break
                
                if not link:
                    print(f"        ✗ No clickable link found in first result")
                    return False
                
                # Click the first result - use JavaScript if regular click fails
                print(f"        🖱️  Clicking first result...")
                try:
                    link.click()
                except Exception as click_error:
                    # Click intercepted - try JavaScript click
                    print(f"        ⚠ Regular click intercepted, using JavaScript...")
                    self.driver.execute_script("arguments[0].click();", link)
                
                time.sleep(2)
                
                # Verify entryIframe appeared (even if click gave error)
                try:
                    self.driver.switch_to.default_content()
                    self.driver.find_element(By.ID, "entryIframe")
                    print(f"        ✓ Detail page loaded")
                    return True
                except NoSuchElementException:
                    print(f"        ⚠ entryIframe did not appear")
                    return False
                
            except NoSuchElementException:
                # No scroll container - might be single result case
                print(f"        ⚠ No scroll container found")
                
                # Check if detail page already loaded in searchIframe
                try:
                    self.driver.find_element(By.CSS_SELECTOR, "div.place_section")
                    print(f"        ✓ Detail content found in searchIframe")
                    return True
                except:
                    pass
                
                # Check if entryIframe appeared anyway
                try:
                    self.driver.switch_to.default_content()
                    self.driver.find_element(By.ID, "entryIframe")
                    print(f"        ✓ entryIframe appeared")
                    return True
                except:
                    print(f"        ✗ No results or detail page found")
                    return False
            
        except Exception as e:
            print(f"        ✗ Error in search: {e}")
            return False
    
    def click_expand_buttons_in_medical_section(self) -> int:
        """
        Click only expand buttons within the medical info section
        Avoids clicking navigation buttons that switch tabs
        
        Returns:
            Number of buttons clicked
        """
        clicked_count = 0
        
        print(f"        📂 Looking for expandable sections in medical info...")
        
        try:
            # First, find the medical info section
            sections = self.driver.find_elements(By.CSS_SELECTOR, "div.place_section")
            
            medical_section = None
            for section in sections:
                try:
                    header = section.find_element(By.CSS_SELECTOR, "h2.place_section_header")
                    title = header.find_element(By.CSS_SELECTOR, "div.place_section_header_title")
                    
                    if "진료정보" in title.text:
                        medical_section = section
                        break
                except:
                    continue
            
            if not medical_section:
                print(f"        ⚠ Medical info section not found for expansion")
                return 0
            
            # Now click expand buttons ONLY within the medical section
            expand_buttons = medical_section.find_elements(By.CSS_SELECTOR, "a.fvwqf")
            
            for button in expand_buttons:
                try:
                    # Check if button text contains "펼쳐서 더보기" or "더보기"
                    button_text = button.text
                    if "펼쳐서 더보기" in button_text or ("더보기" in button_text and "정보" not in button_text):
                        # Scroll button into view
                        self.driver.execute_script(
                            "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                            button
                        )
                        time.sleep(0.3)
                        
                        # Click button
                        button.click()
                        clicked_count += 1
                        time.sleep(0.3)
                        print(f"        ✓ Expanded a section")
                except:
                    continue
            
            if clicked_count > 0:
                print(f"        ✓ Expanded {clicked_count} sections in medical info")
            else:
                print(f"        ℹ No expandable sections found in medical info")
            
            return clicked_count
            
        except Exception as e:
            print(f"        ⚠ Error expanding sections: {e}")
            return 0
    
    def scroll_to_medical_info_section(self) -> bool:
        """Scroll in the detail page to find 진료정보 section"""
        try:
            max_scrolls = 15
            scroll_pause = 0.8
            
            for i in range(max_scrolls):
                # Try to find the medical info section
                try:
                    sections = self.driver.find_elements(By.CSS_SELECTOR, "div.place_section")
                    
                    for section in sections:
                        try:
                            header = section.find_element(By.CSS_SELECTOR, "h2.place_section_header")
                            title = header.find_element(By.CSS_SELECTOR, "div.place_section_header_title")
                            
                            if "진료정보" in title.text:
                                # Found it! Scroll it into view
                                self.driver.execute_script(
                                    "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                                    section
                                )
                                time.sleep(1)
                                return True
                        except NoSuchElementException:
                            continue
                    
                except NoSuchElementException:
                    pass
                
                # Scroll down
                self.driver.execute_script("window.scrollBy(0, 500);")
                time.sleep(scroll_pause)
            
            return False
            
        except Exception as e:
            print(f"        ⚠ Error while scrolling: {e}")
            return False
    
    def extract_medical_info_html(self) -> Optional[str]:
        """Extract the 진료정보 section HTML"""
        try:
            sections = self.driver.find_elements(By.CSS_SELECTOR, "div.place_section")
            
            for section in sections:
                try:
                    header = section.find_element(By.CSS_SELECTOR, "h2.place_section_header")
                    title = header.find_element(By.CSS_SELECTOR, "div.place_section_header_title")
                    
                    if "진료정보" in title.text:
                        # Get the content part
                        content = section.find_element(
                            By.CSS_SELECTOR,
                            "div.place_section_content"
                        )
                        
                        # Return the inner HTML
                        return content.get_attribute('innerHTML')
                        
                except NoSuchElementException:
                    continue
            
            return None
            
        except Exception as e:
            print(f"        ✗ Error extracting HTML: {e}")
            return None
    
    def parse_medical_info_with_llm(self, html_content: str) -> Dict:
        """
        Use LLM to parse HTML content into structured data
        Returns empty dict if parsing fails
        """
        try:
            prompt = f"""You are parsing HTML content from a Korean medical facility webpage. The section is called "진료정보" (Medical Information).

Parse the following HTML and extract ALL information into a structured JSON format. Follow these rules:

1. Extract all visible information from the HTML
2. Organize the data in a logical, structured way using dictionaries and arrays
3. For lists (like 진료과목 with <ul><li> tags), use arrays of strings
4. For tables, convert them to arrays of objects where keys are the column headers
5. Preserve all Korean text exactly as shown
6. Use descriptive field names in English (snake_case)
7. Common patterns you may find:
   - Medical departments (진료과목) - list of departments
   - Specialist information (진료과목별 전문의 정보) - table with departments and counts
   - Special equipment (특수진료장비) - table with equipment names and counts
   - Medical staff (의료인 수) - counts of different staff types
   - Parking information (주차) - parking details
   - Any other relevant medical information

8. Be flexible - extract whatever information is present, even if it doesn't match the common patterns above
9. If a section has a header (h3 tag), use it to name the field appropriately

HTML Content:
{html_content}

Return ONLY valid JSON with ALL extracted information. No markdown, no explanations, just pure JSON.

Example output structure (adapt based on actual content):
{{
  "medical_departments": ["department1", "department2"],
  "specialist_by_department": [{{"department": "산부인과", "specialist_count": "10"}}],
  "special_equipment": [{{"equipment_name": "초음파기", "count": "5"}}],
  "parking": "주차 가능",
  "any_other_field": "extracted data"
}}"""

            chat = self.groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a JSON extraction assistant. You parse HTML and return only valid JSON with all extracted information. No markdown, no explanations, just JSON. Be thorough and extract all available structured information."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0,
                max_tokens=4096
            )
            
            response_text = chat.choices[0].message.content.strip()
            
            # Remove markdown code blocks if present
            if response_text.startswith("```json"):
                response_text = response_text[7:]
            elif response_text.startswith("```"):
                response_text = response_text[3:]
            
            if response_text.endswith("```"):
                response_text = response_text[:-3]
            
            response_text = response_text.strip()
            
            # Parse JSON
            parsed_data = json.loads(response_text)
            
            return parsed_data
            
        except json.JSONDecodeError as e:
            print(f"        ✗ JSON parsing error: {e}")
            return {}
        except Exception as e:
            print(f"        ✗ LLM parsing error: {e}")
            return {}
        
    def extract_medical_information(self) -> Dict:
        """
        Extract medical information from the detail page
        Returns dict with medical info, empty dict for parsed data if nothing found
        """
        result = {
            'has_medical_info': False,
            'medical_info_raw': None,
            'medical_info_parsed': {},  # Empty dict by default
            'parsing_success': False,
            'enrichment_error': None
        }
        
        try:
            # Wait a bit for page to fully load
            time.sleep(1)
            
            print("        🔍 Looking for 진료정보 section...")
            
            # Fast direct search using XPath - finds section containing "진료정보" text
            medical_section = None
            try:
                # Direct XPath search for the medical info header
                xpath = "//h2[@class='place_section_header']//div[contains(text(), '진료정보')]"
                header = self.driver.find_element(By.XPATH, xpath)
                # Get the parent section
                medical_section = header.find_element(By.XPATH, "./ancestor::div[@class='place_section']")
                print("        ✓ Found 진료정보 section (no scroll needed)")
            except NoSuchElementException:
                # Not visible yet, try fast scroll
                medical_section = self.fast_scroll_to_medical_section()
                if not medical_section:
                    print("        ⚠ 진료정보 section not found")
                    result['enrichment_error'] = "Medical info section not found"
                    return result
                print("        ✓ Found 진료정보 section (after scroll)")
            
            # Scroll section into view
            self.driver.execute_script(
                "arguments[0].scrollIntoView({behavior: 'instant', block: 'center'});",
                medical_section
            )
            time.sleep(0.5)
            
            # Try to expand any collapsible content within medical info section
            self.click_expand_buttons_in_medical_section()
            
            # Wait for any animations to complete
            time.sleep(0.5)
            
            # Extract HTML
            html_content = self.extract_medical_info_html()
            
            if not html_content:
                print("        ⚠ Could not extract HTML content")
                result['enrichment_error'] = "Could not extract HTML"
                return result
            
            result['has_medical_info'] = True
            result['medical_info_raw'] = html_content
            
            print("        🤖 Parsing with LLM...")
            
            # Parse with LLM - returns empty dict if fails
            parsed_data = self.parse_medical_info_with_llm(html_content)
            
            if parsed_data:
                result['medical_info_parsed'] = parsed_data
                result['parsing_success'] = True
                
                fields = list(parsed_data.keys())
                print(f"        ✓ Successfully parsed: {len(fields)} fields ({', '.join(fields)})")
            else:
                print("        ⚠ Parsing returned empty - storing empty dict")
                result['medical_info_parsed'] = {}
            
            return result
            
        except Exception as e:
            print(f"        ✗ Error extracting medical info: {e}")
            result['enrichment_error'] = str(e)
            return result

    def fast_scroll_to_medical_section(self) -> Optional[any]:
        """Fast scroll to find 진료정보 section - optimized version"""
        try:
            max_scrolls = 8  # Reduced from 15
            scroll_pause = 0.4  # Reduced from 0.8
            
            xpath = "//h2[@class='place_section_header']//div[contains(text(), '진료정보')]"
            
            for i in range(max_scrolls):
                # Try to find the medical info section
                try:
                    header = self.driver.find_element(By.XPATH, xpath)
                    # Get the parent section
                    section = header.find_element(By.XPATH, "./ancestor::div[@class='place_section']")
                    return section
                except NoSuchElementException:
                    pass
                
                # Scroll down faster
                self.driver.execute_script("window.scrollBy(0, 600);")
                time.sleep(scroll_pause)
            
            return None
            
        except Exception as e:
            print(f"        ⚠ Error while scrolling: {e}")
            return None
        
    def enrich_single_facility(self, facility_name: str, place_id: str) -> Dict:
        """
        Enrich a single facility with medical information
        
        Args:
            facility_name: Name of facility to search
            place_id: Facility place_id
            
        Returns:
            Dict with medical info fields (always includes empty dict for parsed data)
        """
        result = {
            'has_medical_info': False,
            'medical_info_raw': None,
            'medical_info_parsed': {},  # Empty dict by default
            'parsing_success': False,
            'enrichment_error': None,
            'enriched_at': datetime.now().isoformat()
        }
        
        try:
            # Search for facility and click first result
            if not self.search_and_click_first_result(facility_name):
                result['enrichment_error'] = "Could not find or click search result"
                return result
            
            # Switch to detail frame (entryIframe)
            try:
                switch_right(self.driver)
                print(f"        ✓ Switched to entryIframe")
            except Exception as e:
                print(f"        ⚠ Could not switch to detail frame: {e}")
                result['enrichment_error'] = f"Frame switch error: {e}"
                return result
            
            # Wait for page to load
            try:
                self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.place_section'))
                )
            except TimeoutException:
                print(f"        ⚠ Timeout waiting for page to load")
                result['enrichment_error'] = "Page load timeout"
                return result
            
            # Extract medical information (this includes expanding sections)
            med_info = self.extract_medical_information()
            result.update(med_info)
            
            return result
            
        except Exception as e:
            print(f"        ✗ Error enriching facility: {e}")
            result['enrichment_error'] = str(e)
            return result


# ============================================================================
# JSON-BASED CHECKPOINT MANAGER
# ============================================================================

class JSONCheckpointManager:
    """Manage enrichment progress using JSON file with place_id as keys"""
    
    def __init__(self, checkpoint_file="./data/enrichment_progress.json"):
        self.checkpoint_file = Path(checkpoint_file)
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        self.progress_data = {}
        
        # Load existing progress if available
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
    
    def add_facility(self, place_id: str, medical_info: Dict):
        """Add facility enrichment result to progress"""
        self.progress_data[place_id] = medical_info
    
    def get_stats(self) -> Dict:
        """Get statistics about current progress"""
        total = len(self.progress_data)
        with_info = sum(1 for v in self.progress_data.values() if v.get('has_medical_info'))
        parsed = sum(1 for v in self.progress_data.values() if v.get('parsing_success'))
        
        return {
            'total_processed': total,
            'with_medical_info': with_info,
            'successfully_parsed': parsed
        }


# ============================================================================
# ENRICHMENT ORCHESTRATOR
# ============================================================================

class EnrichmentOrchestrator:
    """Orchestrate the enrichment process for all facilities"""
    
    def __init__(self, output_dir="./data", groq_api_key: str = None):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.checkpoint_mgr = JSONCheckpointManager(
            checkpoint_file=self.output_dir / "enrichment_progress.json"
        )
        self.groq_api_key = groq_api_key
    
    def enrich_all_facilities(self,
                             facilities_df: pd.DataFrame,
                             save_freq: int = 10,
                             headless: bool = True) -> Dict:
        """
        Enrich all facilities with medical information
        Resumes from existing progress automatically
        
        Args:
            facilities_df: DataFrame with facilities data
            save_freq: Save progress every N facilities
            headless: Run browser in headless mode
        
        Returns:
            Dict mapping place_id to medical info
        """
        scraper = MedicalInfoEnrichmentScraper(
            groq_api_key=self.groq_api_key,
            headless=headless
        )
        scraper.setup_driver()
        
        # Get current stats
        stats = self.checkpoint_mgr.get_stats()
        total_facilities = len(facilities_df)
        already_processed = stats['total_processed']
        
        print(f"\n{'='*70}")
        print(f"STARTING MEDICAL INFORMATION ENRICHMENT")
        print(f"{'='*70}")
        print(f"Total facilities: {total_facilities:,}")
        print(f"Already processed: {already_processed:,}")
        print(f"Remaining: {total_facilities - already_processed:,}")
        print(f"Save frequency: every {save_freq} facilities")
        print(f"LLM Model: llama-3.3-70b-versatile (Groq)")
        print(f"{'='*70}\n")
        
        processed_count = 0
        
        try:
            for idx, row in facilities_df.iterrows():
                place_id = str(row['place_id'])
                facility_name = row.get('name', 'Unknown')
                # Skip if facility name does NOT contain 의원 or 병원
                if not any(keyword in facility_name for keyword in ("의원", "병원")):
                    continue
                # Skip if already processed
                if self.checkpoint_mgr.is_processed(place_id):
                    continue
                
                processed_count += 1
                current_total = already_processed + processed_count
                
                print(f"[{current_total}/{total_facilities}] {facility_name}")
                print(f"  Place ID: {place_id}")
                
                try:
                    medical_info = scraper.enrich_single_facility(facility_name, place_id)
                    
                    # Add to checkpoint
                    self.checkpoint_mgr.add_facility(place_id, medical_info)
                    
                    # Print status
                    if medical_info['has_medical_info']:
                        if medical_info['parsing_success']:
                            parsed = medical_info['medical_info_parsed']
                            fields = list(parsed.keys()) if parsed else []
                            print(f"  ✓ Extracted and parsed: {len(fields)} fields")
                        else:
                            print(f"  ⚠ Found medical info but parsing returned empty")
                    else:
                        if medical_info.get('enrichment_error'):
                            print(f"  ⚠ Error: {medical_info['enrichment_error']}")
                        else:
                            print(f"  ⚠ No medical info section found")
                    
                except Exception as e:
                    print(f"  ✗ Failed: {e}")
                    # Still save with empty dict
                    self.checkpoint_mgr.add_facility(place_id, {
                        'has_medical_info': False,
                        'medical_info_raw': None,
                        'medical_info_parsed': {},
                        'parsing_success': False,
                        'enrichment_error': str(e),
                        'enriched_at': datetime.now().isoformat()
                    })
                
                # Save progress periodically
                if processed_count % save_freq == 0:
                    self.checkpoint_mgr.save_progress()
                    stats = self.checkpoint_mgr.get_stats()
                    print(f"  💾 Progress saved: {stats['total_processed']:,} total facilities")
                
                # Polite delay between requests
                time.sleep(2)
            
        finally:
            scraper.close_driver()
            # Final save
            self.checkpoint_mgr.save_progress()
        
        return self.checkpoint_mgr.progress_data
    
    def create_enriched_dataset(self, facilities_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge progress data with original dataset
        
        Args:
            facilities_df: Original facilities dataframe
            
        Returns:
            Enriched dataframe
        """
        # Convert progress dict to list of records
        records = []
        for place_id, med_info in self.checkpoint_mgr.progress_data.items():
            record = {'place_id': place_id}
            record.update(med_info)
            records.append(record)
        
        if not records:
            print("⚠ No enrichment data to merge")
            return facilities_df
        
        # Create dataframe from records
        enrichment_df = pd.DataFrame(records)
        
        # Ensure place_id is string in both dataframes
        facilities_df['place_id'] = facilities_df['place_id'].astype(str)
        enrichment_df['place_id'] = enrichment_df['place_id'].astype(str)
        
        # Merge with original dataset
        enriched_df = facilities_df.merge(
            enrichment_df,
            on='place_id',
            how='left'
        )
        
        # Fill missing values
        enriched_df['has_medical_info'] = enriched_df['has_medical_info'].fillna(False)
        enriched_df['parsing_success'] = enriched_df['parsing_success'].fillna(False)
        enriched_df['medical_info_parsed'] = enriched_df['medical_info_parsed'].apply(
            lambda x: x if isinstance(x, dict) else {}
        )
        
        return enriched_df
    
    def upload_to_huggingface(self, enriched_df: pd.DataFrame, dataset_name: str):
        """
        Upload enriched dataset to HuggingFace
        
        Args:
            enriched_df: Enriched dataframe
            dataset_name: HuggingFace dataset name (e.g., "username/dataset-name")
        """
        try:
            from datasets import Dataset
            from huggingface_hub import HfApi
            
            print(f"\n{'='*70}")
            print(f"UPLOADING TO HUGGINGFACE")
            print(f"{'='*70}")
            print(f"Dataset: {dataset_name}")
            print(f"Rows: {len(enriched_df):,}")
            
            # Convert to HuggingFace dataset
            dataset = Dataset.from_pandas(enriched_df)
            
            # Push to hub
            dataset.push_to_hub(dataset_name)
            
            print(f"✓ Successfully uploaded to HuggingFace!")
            print(f"  View at: https://huggingface.co/datasets/{dataset_name}")
            
        except Exception as e:
            print(f"✗ Error uploading to HuggingFace: {e}")
            print(f"  Make sure you're logged in: huggingface-cli login")
    
    def print_summary(self):
        """Print summary statistics"""
        stats = self.checkpoint_mgr.get_stats()
        
        print(f"\n{'='*70}")
        print(f"ENRICHMENT SUMMARY")
        print(f"{'='*70}")
        print(f"Total processed: {stats['total_processed']:,}")
        print(f"With medical info: {stats['with_medical_info']:,}")
        print(f"Successfully parsed: {stats['successfully_parsed']:,}")
        
        # Analyze fields
        all_fields = set()
        for med_info in self.checkpoint_mgr.progress_data.values():
            parsed = med_info.get('medical_info_parsed')
            if isinstance(parsed, dict):
                all_fields.update(parsed.keys())
        
        if all_fields:
            print(f"\nUnique fields found across all facilities:")
            field_counts = {}
            for field in all_fields:
                count = sum(
                    1 for v in self.checkpoint_mgr.progress_data.values()
                    if isinstance(v.get('medical_info_parsed'), dict) and field in v['medical_info_parsed']
                )
                field_counts[field] = count
            
            for field in sorted(field_counts.keys(), key=lambda x: field_counts[x], reverse=True):
                print(f"  {field}: {field_counts[field]:,} facilities")
        
        print(f"{'='*70}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    
    # Get Groq API key from environment
    groq_api_key = os.getenv("GROQ_API_KEY")
    
    if not groq_api_key:
        print("❌ Error: GROQ_API_KEY not set in environment")
        print("Please set it with: export GROQ_API_KEY='your-api-key'")
        return
    
    # Initialize managers
    dataset_mgr = DatasetManager(
        dataset_name="ValerianFourel/seoul-medical-facilities",
        cache_dir="./data"
    )
    
    orchestrator = EnrichmentOrchestrator(
        output_dir="./data",
        groq_api_key=groq_api_key
    )
    
    # Load facilities dataset
    print("="*70)
    print("STEP 1: LOADING FACILITIES DATASET")
    print("="*70)
    
    facilities_df = dataset_mgr.load_dataset(force_download=False)
    
    # Enrich with medical information using LLM
    print("\n" + "="*70)
    print("STEP 2: ENRICHING WITH MEDICAL INFORMATION")
    print("="*70)
    
    progress_data = orchestrator.enrich_all_facilities(
        facilities_df,
        save_freq=10,  # Save every 10 facilities
        headless=True  # Set to False for debugging
    )
    
    # Print summary
    print("\n" + "="*70)
    print("STEP 3: ENRICHMENT SUMMARY")
    print("="*70)
    
    orchestrator.print_summary()
    
    # Create enriched dataset
    print("\n" + "="*70)
    print("STEP 4: CREATING ENRICHED DATASET")
    print("="*70)
    
    enriched_df = orchestrator.create_enriched_dataset(facilities_df)
    
    # Save locally
    output_file = Path("./data/seoul_medical_facilities_enriched.parquet")
    enriched_df.to_parquet(output_file, index=False)
    print(f"✓ Saved enriched dataset locally: {output_file}")
    
    # Upload to HuggingFace
    print("\n" + "="*70)
    print("STEP 5: UPLOAD TO HUGGINGFACE")
    print("="*70)
    
    upload = input("Upload to HuggingFace? (yes/no): ").strip().lower()
    
    if upload == 'yes':
        dataset_name = input("Enter HuggingFace dataset name (e.g., username/dataset-name): ").strip()
        if dataset_name:
            orchestrator.upload_to_huggingface(enriched_df, dataset_name)
    
    print("\n" + "="*70)
    print("✅ ENRICHMENT COMPLETE!")
    print("="*70)
    
    return enriched_df


if __name__ == "__main__":
    enriched_df = main()