#!/usr/bin/env python3
"""
Seoul Medical Facilities Medical Information Enrichment
Features:
- JSON-based checkpoint system with PARTITIONING for concurrent runs
- LOGIC-BASED HTML parsing (no LLM needed!)
- DIRECT NAVIGATION using name+place_id URL (PROVEN METHOD!)

PARTITIONING LOGIC:
- Given X (partition_id) and Y (total_partitions)
- This script processes every Y-th row starting from row X
- Example: X=0, Y=4 processes rows 0, 4, 8, 12, ...
- Example: X=1, Y=4 processes rows 1, 5, 9, 13, ...
- This ensures NO overlap between partitions
"""

import pandas as pd
import time
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from urllib.parse import quote
from bs4 import BeautifulSoup
import numpy as np

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.options import Options

# Import frame switching utilities
import sys
sys.path.insert(0, os.path.dirname(__file__))
from utils.frame_switch import switch_left, switch_right


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def safe_str(value, default='N/A', max_length=None):
    """
    Safely convert value to string, handling NaN, None, and float values
    
    Args:
        value: Any value to convert
        default: Default string if value is NaN/None
        max_length: Optional max length to truncate to
    
    Returns:
        String representation
    """
    # Handle NaN, None, empty
    if pd.isna(value) or value is None or value == '':
        result = default
    else:
        result = str(value)
    
    # Truncate if needed
    if max_length and len(result) > max_length:
        result = result[:max_length] + '...'
    
    return result


# ============================================================================
# HTML PARSER - LOGIC BASED (NO LLM!)
# ============================================================================

class MedicalInfoHTMLParser:
    """Parse medical information HTML using logic-based approach"""
    
    @staticmethod
    def parse_table(table_elem) -> List[Dict]:
        """Parse HTML table into list of dicts"""
        rows = []
        try:
            tbody = table_elem.find('tbody')
            if not tbody:
                return rows
            
            for tr in tbody.find_all('tr'):
                cells = tr.find_all(['th', 'td'])
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True)
                    value = cells[1].get_text(strip=True)
                    rows.append({'key': key, 'value': value})
        except Exception as e:
            print(f"          ⚠ Error parsing table: {e}")
        
        return rows
    
    @staticmethod
    def parse_list(ul_elem) -> List[str]:
        """Parse HTML list into list of strings"""
        items = []
        try:
            for li in ul_elem.find_all('li', class_='zxtJF'):
                text = li.get_text(strip=True)
                if text:
                    items.append(text)
        except Exception as e:
            print(f"          ⚠ Error parsing list: {e}")
        
        return items
    
    @staticmethod
    def parse_medical_info(html_content: str) -> Dict:
        """Parse medical info HTML into structured data"""
        result = {}
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            sections = soup.find_all('div', class_='DAQTB')
            
            for section in sections:
                h3 = section.find('h3', class_='fr6Pj')
                if not h3:
                    continue
                
                header_text = h3.get_text(strip=True)
                
                # Section 1: 진료과목별 전문의 정보
                if '진료과목별 전문의 정보' in header_text:
                    table = section.find('table')
                    if table:
                        table_data = MedicalInfoHTMLParser.parse_table(table)
                        if table_data:
                            result['specialist_by_department'] = [
                                {'department': row['key'], 'specialist_count': row['value']}
                                for row in table_data
                            ]
                
                # Section 2: 진료과목
                elif '진료과목' in header_text and '진료과목별' not in header_text:
                    ul = section.find('ul', class_='xrrcZ')
                    if ul:
                        departments = MedicalInfoHTMLParser.parse_list(ul)
                        if departments:
                            result['medical_departments'] = departments
                
                # Section 3: 특수진료장비
                elif '특수진료장비' in header_text:
                    table = section.find('table')
                    if table:
                        table_data = MedicalInfoHTMLParser.parse_table(table)
                        if table_data:
                            result['special_equipment'] = [
                                {'equipment_name': row['key'], 'count': row['value']}
                                for row in table_data
                            ]
                
                # Section 4: 우수기관 평가정보
                elif '우수기관 평가정보' in header_text:
                    table = section.find('table')
                    if table:
                        table_data = MedicalInfoHTMLParser.parse_table(table)
                        if table_data:
                            result['excellent_institution_evaluation'] = [
                                {'evaluation_item': row['key'], 'evaluation_info': row['value']}
                                for row in table_data
                            ]
                
                # Section 5: 의료인 수
                elif '의료인 수' in header_text:
                    table = section.find('table')
                    if table:
                        table_data = MedicalInfoHTMLParser.parse_table(table)
                        if table_data:
                            result['medical_staff_count'] = [
                                {'staff_type': row['key'], 'count': row['value']}
                                for row in table_data
                            ]
                
                # Section 6: 주차
                elif '주차' in header_text:
                    ul = section.find('ul')
                    if ul:
                        parking_items = MedicalInfoHTMLParser.parse_list(ul)
                        if parking_items:
                            result['parking'] = parking_items
                    else:
                        text = section.get_text(strip=True)
                        text = text.replace(header_text, '').strip()
                        if text:
                            result['parking'] = text
            
            # Extract copyright info
            copyright_div = soup.find('div', class_='w8afO')
            if copyright_div:
                copyright_text = copyright_div.get_text(strip=True)
                if copyright_text:
                    result['copyright_info'] = copyright_text
            
            # Extract more info link
            more_info_div = soup.find('div', class_='x4zu8')
            if more_info_div:
                link = more_info_div.find('a', class_='place_bluelink')
                if link:
                    href = link.get('href')
                    if href:
                        result['more_info_link'] = href
                    link_text = link.get_text(strip=True)
                    if link_text:
                        result['more_info_text'] = link_text
            
        except Exception as e:
            print(f"          ✗ Error parsing HTML: {e}")
        
        return result


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
        
        print(f"\nDataset columns: {list(df.columns)}")
        if len(df) > 0:
            print(f"Sample facility:")
            sample = df[['place_id', 'name']].head(1)
            for col in sample.columns:
                val = sample.iloc[0][col]
                print(f"  {col}: {safe_str(val, max_length=50)}")
        
        return df


# ============================================================================
# MEDICAL INFO ENRICHMENT SCRAPER (WITH PROVEN NAVIGATION!)
# ============================================================================

class MedicalInfoEnrichmentScraper:
    """Scrape and enrich facilities using PROVEN navigation method"""
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = None
        self.wait = None
        self.parser = MedicalInfoHTMLParser()
    
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
            try:
                self.driver.quit()
                print("✓ Browser closed")
            except Exception as e:
                print(f"⚠ Error closing browser: {e}")
    
    def clean_place_id(self, place_id) -> str:
        """
        Clean place_id by removing .0 if present
        Examples: 123.0 -> "123", "123.0" -> "123", 123 -> "123"
        """
        place_id_str = str(place_id)
        if place_id_str.endswith('.0'):
            place_id_str = place_id_str[:-2]
        return place_id_str
    
    def extract_place_id_from_url(self) -> Optional[str]:
        """Extract place_id from current URL"""
        try:
            current_url = self.driver.current_url
            match = re.search(r'/place/(\d+)', current_url)
            if match:
                return match.group(1)
        except Exception as e:
            print(f"        ⚠ Error extracting place_id: {e}")
        return None
    
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
    
    def navigate_to_place_direct(self, facility_name: str, place_id: str) -> bool:
        """
        Navigate directly to place using combined name+place_id URL (PROVEN METHOD!)
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
    
    def click_expand_buttons_in_medical_section(self) -> int:
        """Click only expand buttons within the medical info section"""
        clicked_count = 0
        
        try:
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
                return 0
            
            expand_buttons = medical_section.find_elements(By.CSS_SELECTOR, "a.fvwqf")
            
            for button in expand_buttons:
                try:
                    button_text = button.text
                    if "펼쳐서 더보기" in button_text or ("더보기" in button_text and "정보" not in button_text):
                        self.driver.execute_script(
                            "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                            button
                        )
                        time.sleep(0.3)
                        button.click()
                        clicked_count += 1
                        time.sleep(0.3)
                except:
                    continue
            
            if clicked_count > 0:
                print(f"        ✓ Expanded {clicked_count} sections")
            
            return clicked_count
            
        except Exception as e:
            return 0
    
    def fast_scroll_to_medical_section(self) -> Optional[any]:
        """Fast scroll to find 진료정보 section"""
        try:
            max_scrolls = 8
            scroll_pause = 0.4
            
            xpath = "//h2[@class='place_section_header']//div[contains(text(), '진료정보')]"
            
            for i in range(max_scrolls):
                try:
                    header = self.driver.find_element(By.XPATH, xpath)
                    section = header.find_element(By.XPATH, "./ancestor::div[@class='place_section']")
                    return section
                except NoSuchElementException:
                    pass
                
                self.driver.execute_script("window.scrollBy(0, 600);")
                time.sleep(scroll_pause)
            
            return None
            
        except Exception as e:
            return None
    
    def extract_medical_info_html(self) -> Optional[str]:
        """Extract the 진료정보 section HTML"""
        try:
            sections = self.driver.find_elements(By.CSS_SELECTOR, "div.place_section")
            
            for section in sections:
                try:
                    header = section.find_element(By.CSS_SELECTOR, "h2.place_section_header")
                    title = header.find_element(By.CSS_SELECTOR, "div.place_section_header_title")
                    
                    if "진료정보" in title.text:
                        content = section.find_element(By.CSS_SELECTOR, "div.place_section_content")
                        return content.get_attribute('innerHTML')
                        
                except NoSuchElementException:
                    continue
            
            return None
            
        except Exception as e:
            print(f"        ✗ Error extracting HTML: {e}")
            return None
    
    def extract_medical_information(self) -> Dict:
        """Extract medical information from the detail page"""
        result = {
            'has_medical_info': False,
            'medical_info_raw': None,
            'medical_info_parsed': {},
            'parsing_success': False,
            'enrichment_error': None
        }
        
        try:
            time.sleep(1)
            
            print("        🔍 Looking for 진료정보 section...")
            
            medical_section = None
            try:
                xpath = "//h2[@class='place_section_header']//div[contains(text(), '진료정보')]"
                header = self.driver.find_element(By.XPATH, xpath)
                medical_section = header.find_element(By.XPATH, "./ancestor::div[@class='place_section']")
                print("        ✓ Found 진료정보 section")
            except NoSuchElementException:
                medical_section = self.fast_scroll_to_medical_section()
                if not medical_section:
                    print("        ⚠ 진료정보 section not found")
                    result['enrichment_error'] = "Medical info section not found"
                    return result
                print("        ✓ Found 진료정보 section (after scroll)")
            
            self.driver.execute_script(
                "arguments[0].scrollIntoView({behavior: 'instant', block: 'center'});",
                medical_section
            )
            time.sleep(0.5)
            
            self.click_expand_buttons_in_medical_section()
            time.sleep(0.5)
            
            html_content = self.extract_medical_info_html()
            
            if not html_content:
                print("        ⚠ Could not extract HTML content")
                result['enrichment_error'] = "Could not extract HTML"
                return result
            
            result['has_medical_info'] = True
            result['medical_info_raw'] = html_content
            
            print("        ⚙️  Parsing with logic-based parser...")
            
            parsed_data = self.parser.parse_medical_info(html_content)
            
            if parsed_data:
                result['medical_info_parsed'] = parsed_data
                result['parsing_success'] = True
                
                fields = list(parsed_data.keys())
                print(f"        ✓ Parsed: {len(fields)} fields")
            else:
                print("        ⚠ Parsing returned empty")
                result['medical_info_parsed'] = {}
            
            return result
            
        except Exception as e:
            print(f"        ✗ Error extracting medical info: {e}")
            result['enrichment_error'] = str(e)
            return result
    
    def enrich_single_facility(self, facility_name: str, place_id: str) -> Dict:
        """
        Enrich a single facility with medical information
        Uses PROVEN direct navigation method
        """
        result = {
            'has_medical_info': False,
            'medical_info_raw': None,
            'medical_info_parsed': {},
            'parsing_success': False,
            'enrichment_error': None,
            'enriched_at': datetime.now().isoformat(),
            'verified_place_id': None
        }
        
        try:
            # Use PROVEN direct navigation method
            if not self.navigate_to_place_direct(facility_name, place_id):
                result['enrichment_error'] = "Could not navigate to place"
                return result
            
            # Store verified place_id
            result['verified_place_id'] = self.extract_place_id_from_url()
            
            # We're already in entryIframe after navigate_to_place_direct
            print(f"        ✓ Already in detail page iframe")
            
            # Wait for page to load
            try:
                self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.place_section'))
                )
            except TimeoutException:
                print(f"        ⚠ Timeout waiting for page to load")
                result['enrichment_error'] = "Page load timeout"
                return result
            
            # Extract medical information
            med_info = self.extract_medical_information()
            result.update(med_info)
            
            return result
            
        except Exception as e:
            print(f"        ✗ Error enriching facility: {e}")
            result['enrichment_error'] = str(e)
            return result


# ============================================================================
# PARTITIONED JSON CHECKPOINT MANAGER
# ============================================================================

class PartitionedCheckpointManager:
    """
    Manage enrichment progress with PARTITIONING for concurrent runs
    Each partition saves to a separate JSON file
    
    PARTITIONING EXPLANATION:
    - partition_id (X): Which partition this is (0-indexed)
    - total_partitions (Y): Total number of partitions
    - This partition processes rows where: row_index % Y == X
    - Example: X=0, Y=4 → processes rows 0, 4, 8, 12, 16, ...
    - Example: X=1, Y=4 → processes rows 1, 5, 9, 13, 17, ...
    - NO OVERLAP between partitions!
    """
    
    def __init__(self, partition_id: int, total_partitions: int,
                 checkpoint_dir="./data"):
        self.partition_id = partition_id
        self.total_partitions = total_partitions
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        self.checkpoint_file = self.checkpoint_dir / f"enrichment_progress_partition_{partition_id:03d}_of_{total_partitions:03d}.json"
        
        self.progress_data = {}
        
        if self.checkpoint_file.exists():
            self.load_progress()
    
    def load_progress(self):
        """Load existing progress from JSON"""
        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                self.progress_data = json.load(f)
            print(f"✓ Loaded partition {self.partition_id}: {len(self.progress_data)} facilities")
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
        verified = sum(1 for v in self.progress_data.values() if v.get('verified_place_id'))
        
        return {
            'total_processed': total,
            'with_medical_info': with_info,
            'successfully_parsed': parsed,
            'verified_place_id': verified
        }
    
    @staticmethod
    def merge_all_partitions(checkpoint_dir: str = "./data") -> Dict:
        """Merge all partition JSON files into a single dictionary"""
        checkpoint_path = Path(checkpoint_dir)
        all_data = {}
        
        partition_files = sorted(checkpoint_path.glob("enrichment_progress_partition_*.json"))
        
        print(f"\n{'='*70}")
        print(f"MERGING PARTITIONS")
        print(f"{'='*70}")
        print(f"Found {len(partition_files)} partition files")
        
        for pfile in partition_files:
            try:
                with open(pfile, 'r', encoding='utf-8') as f:
                    partition_data = json.load(f)
                    all_data.update(partition_data)
                    print(f"✓ Merged {pfile.name}: {len(partition_data)} facilities")
            except Exception as e:
                print(f"✗ Error reading {pfile.name}: {e}")
        
        print(f"{'='*70}")
        print(f"Total merged facilities: {len(all_data):,}")
        
        merged_file = checkpoint_path / "enrichment_progress_MERGED.json"
        try:
            with open(merged_file, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=2)
            print(f"✓ Saved merged file: {merged_file}")
        except Exception as e:
            print(f"✗ Error saving merged file: {e}")
        
        return all_data


# ============================================================================
# ENRICHMENT ORCHESTRATOR (WITH PARTITIONING!)
# ============================================================================

class EnrichmentOrchestrator:
    """Orchestrate the enrichment process with partitioning support"""
    
    def __init__(self, partition_id: int = 0, total_partitions: int = 1,
                 output_dir="./data"):
        self.partition_id = partition_id
        self.total_partitions = total_partitions
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.checkpoint_mgr = PartitionedCheckpointManager(
            partition_id=partition_id,
            total_partitions=total_partitions,
            checkpoint_dir=output_dir
        )
    
    def filter_dataframe_by_partition(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter dataframe to only include rows for this partition
        
        PARTITIONING LOGIC:
        - Row belongs to partition X if: (row_index % Y) == X
        - Where Y = total_partitions
        - This ensures NO overlap and complete coverage
        
        Example with 4 partitions:
        - Partition 0: rows 0, 4, 8, 12, 16, ...
        - Partition 1: rows 1, 5, 9, 13, 17, ...
        - Partition 2: rows 2, 6, 10, 14, 18, ...
        - Partition 3: rows 3, 7, 11, 15, 19, ...
        """
        # Reset index to ensure proper modulo calculation
        df_reset = df.reset_index(drop=True)
        
        # Filter: keep rows where index % total_partitions == partition_id
        mask = df_reset.index % self.total_partitions == self.partition_id
        partition_df = df_reset[mask].copy()
        
        print(f"\n{'='*70}")
        print(f"PARTITION FILTERING")
        print(f"{'='*70}")
        print(f"Partition: {self.partition_id} of {self.total_partitions} (X={self.partition_id}, Y={self.total_partitions})")
        print(f"Formula: Processing rows where (row_index % {self.total_partitions}) == {self.partition_id}")
        print(f"Original dataset size: {len(df):,} rows")
        print(f"This partition size: {len(partition_df):,} rows")
        print(f"Percentage: {100 * len(partition_df) / len(df):.1f}%")
        
        # Show sample row indices this partition will process
        sample_indices = partition_df.index[:10].tolist()
        print(f"Sample row indices: {sample_indices}...")
        print(f"{'='*70}\n")
        
        return partition_df
    
    def enrich_all_facilities(self,
                             facilities_df: pd.DataFrame,
                             save_freq: int = 10,
                             headless: bool = True) -> Dict:
        """Enrich facilities assigned to this partition"""
        
        partition_df = self.filter_dataframe_by_partition(facilities_df)
        
        scraper = MedicalInfoEnrichmentScraper(headless=headless)
        scraper.setup_driver()
        
        stats = self.checkpoint_mgr.get_stats()
        total_in_partition = len(partition_df)
        already_processed = stats['total_processed']
        
        print(f"\n{'='*70}")
        print(f"STARTING MEDICAL INFORMATION ENRICHMENT")
        print(f"PARTITION {self.partition_id} OF {self.total_partitions}")
        print(f"{'='*70}")
        print(f"Facilities in this partition: {total_in_partition:,}")
        print(f"Already processed: {already_processed:,}")
        print(f"Remaining: {total_in_partition - already_processed:,}")
        print(f"Save frequency: every {save_freq} facilities")
        print(f"Parser: Logic-based (NO LLM)")
        print(f"Navigation: PROVEN direct method (name+place_id URL)")
        print(f"{'='*70}\n")
        
        processed_count = 0
        
        try:
            for idx, row in partition_df.iterrows():
                place_id = safe_str(row['place_id'])
                facility_name = safe_str(row.get('name', 'Unknown'))
                
                # Skip if facility name does NOT contain 의원 or 병원
                if not any(keyword in facility_name for keyword in ("의원", "병원")):
                    continue
                
                # Skip if already processed
                if self.checkpoint_mgr.is_processed(place_id):
                    continue
                
                processed_count += 1
                current_total = already_processed + processed_count
                
                print(f"[Partition {self.partition_id}] [{current_total}/{total_in_partition}] {facility_name}")
                print(f"  Place ID: {place_id}")
                
                try:
                    medical_info = scraper.enrich_single_facility(facility_name, place_id)
                    
                    self.checkpoint_mgr.add_facility(place_id, medical_info)
                    
                    if medical_info.get('verified_place_id'):
                        print(f"  ✓ Verified: {medical_info['verified_place_id']}")
                    
                    if medical_info['has_medical_info']:
                        if medical_info['parsing_success']:
                            parsed = medical_info['medical_info_parsed']
                            fields = list(parsed.keys()) if parsed else []
                            print(f"  ✓ Extracted: {len(fields)} fields")
                        else:
                            print(f"  ⚠ Found medical info but parsing empty")
                    else:
                        if medical_info.get('enrichment_error'):
                            print(f"  ⚠ Error: {medical_info['enrichment_error']}")
                        else:
                            print(f"  ⚠ No medical info section")
                    
                except Exception as e:
                    print(f"  ✗ Failed: {e}")
                    self.checkpoint_mgr.add_facility(place_id, {
                        'has_medical_info': False,
                        'medical_info_raw': None,
                        'medical_info_parsed': {},
                        'parsing_success': False,
                        'enrichment_error': str(e),
                        'enriched_at': datetime.now().isoformat(),
                        'verified_place_id': None
                    })
                
                if processed_count % save_freq == 0:
                    self.checkpoint_mgr.save_progress()
                    stats = self.checkpoint_mgr.get_stats()
                    print(f"  💾 Progress saved: {stats['total_processed']:,} facilities")
                
                time.sleep(2)
            
        finally:
            scraper.close_driver()
            self.checkpoint_mgr.save_progress()
        
        return self.checkpoint_mgr.progress_data
    
    def print_summary(self):
        """Print summary statistics for this partition"""
        stats = self.checkpoint_mgr.get_stats()
        
        print(f"\n{'='*70}")
        print(f"PARTITION {self.partition_id} ENRICHMENT SUMMARY")
        print(f"{'='*70}")
        print(f"Total processed: {stats['total_processed']:,}")
        print(f"With medical info: {stats['with_medical_info']:,}")
        print(f"Successfully parsed: {stats['successfully_parsed']:,}")
        print(f"Verified place_id: {stats['verified_place_id']:,}")
        
        all_fields = set()
        for med_info in self.checkpoint_mgr.progress_data.values():
            parsed = med_info.get('medical_info_parsed')
            if isinstance(parsed, dict):
                all_fields.update(parsed.keys())
        
        if all_fields:
            print(f"\nFields found:")
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
# DATASET MERGER
# ============================================================================

class DatasetMerger:
    """Merge partition results with original dataset"""
    
    @staticmethod
    def create_enriched_dataset(facilities_df: pd.DataFrame,
                                checkpoint_dir: str = "./data") -> pd.DataFrame:
        """Merge all partition data with original dataset"""
        
        merged_data = PartitionedCheckpointManager.merge_all_partitions(checkpoint_dir)
        
        if not merged_data:
            print("⚠ No enrichment data to merge")
            return facilities_df
        
        records = []
        for place_id, med_info in merged_data.items():
            record = {'place_id': place_id}
            record.update(med_info)
            records.append(record)
        
        enrichment_df = pd.DataFrame(records)
        
        facilities_df['place_id'] = facilities_df['place_id'].astype(str)
        enrichment_df['place_id'] = enrichment_df['place_id'].astype(str)
        
        enriched_df = facilities_df.merge(enrichment_df, on='place_id', how='left')
        
        enriched_df['has_medical_info'] = enriched_df['has_medical_info'].fillna(False)
        enriched_df['parsing_success'] = enriched_df['parsing_success'].fillna(False)
        enriched_df['medical_info_parsed'] = enriched_df['medical_info_parsed'].apply(
            lambda x: x if isinstance(x, dict) else {}
        )
        
        print(f"\n{'='*70}")
        print(f"MERGE COMPLETE")
        print(f"{'='*70}")
        print(f"Original facilities: {len(facilities_df):,}")
        print(f"Enriched facilities: {len(enrichment_df):,}")
        print(f"Final dataset rows: {len(enriched_df):,}")
        
        return enriched_df
    
    @staticmethod
    def upload_to_huggingface(enriched_df: pd.DataFrame, dataset_name: str):
        """Upload enriched dataset to HuggingFace"""
        try:
            from datasets import Dataset
            
            print(f"\n{'='*70}")
            print(f"UPLOADING TO HUGGINGFACE")
            print(f"{'='*70}")
            print(f"Dataset: {dataset_name}")
            print(f"Rows: {len(enriched_df):,}")
            
            dataset = Dataset.from_pandas(enriched_df)
            dataset.push_to_hub(dataset_name)
            
            print(f"✓ Successfully uploaded!")
            print(f"  View at: https://huggingface.co/datasets/{dataset_name}")
            
        except Exception as e:
            print(f"✗ Error uploading: {e}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main(partition_id: int = 0, total_partitions: int = 1):
    """
    Main execution function with partitioning support
    
    Args:
        partition_id (X): Which partition to process (0-indexed, 0 to Y-1)
        total_partitions (Y): Total number of partitions
    
    This partition processes rows where: (row_index % Y) == X
    
    Usage:
        # Single run (no partitioning):
        python script.py
        
        # Run 4 concurrent instances (Y=4):
        python script.py --partition 0 --total 4  # X=0: rows 0,4,8,12,...
        python script.py --partition 1 --total 4  # X=1: rows 1,5,9,13,...
        python script.py --partition 2 --total 4  # X=2: rows 2,6,10,14,...
        python script.py --partition 3 --total 4  # X=3: rows 3,7,11,15,...
    """
    
    print(f"\n{'='*70}")
    print(f"SEOUL MEDICAL FACILITIES ENRICHMENT")
    print(f"PARTITION {partition_id} OF {total_partitions} (X={partition_id}, Y={total_partitions})")
    print(f"{'='*70}\n")
    
    dataset_mgr = DatasetManager(
        dataset_name="ValerianFourel/seoul-medical-facilities",
        cache_dir="./data"
    )
    
    orchestrator = EnrichmentOrchestrator(
        partition_id=partition_id,
        total_partitions=total_partitions,
        output_dir="./data"
    )
    
    print("="*70)
    print("STEP 1: LOADING FACILITIES DATASET")
    print("="*70)
    
    facilities_df = dataset_mgr.load_dataset(force_download=False)
    
    print("\n" + "="*70)
    print("STEP 2: ENRICHING WITH MEDICAL INFORMATION")
    print("="*70)
    
    progress_data = orchestrator.enrich_all_facilities(
        facilities_df,
        save_freq=10,
        headless=True
    )
    
    print("\n" + "="*70)
    print("STEP 3: PARTITION SUMMARY")
    print("="*70)
    
    orchestrator.print_summary()
    
    print("\n" + "="*70)
    print("✅ PARTITION ENRICHMENT COMPLETE!")
    print("="*70)
    print(f"Checkpoint saved to: {orchestrator.checkpoint_mgr.checkpoint_file}")
    print(f"\nTo merge all partitions and create final dataset, run:")
    print(f"  python script.py --merge")
    
    return progress_data


def merge_and_upload():
    """Merge all partitions and optionally upload to HuggingFace"""
    
    print("\n" + "="*70)
    print("MERGING ALL PARTITIONS")
    print("="*70)
    
    dataset_mgr = DatasetManager(
        dataset_name="ValerianFourel/seoul-medical-facilities",
        cache_dir="./data"
    )
    
    facilities_df = dataset_mgr.load_dataset(force_download=False)
    
    enriched_df = DatasetMerger.create_enriched_dataset(
        facilities_df,
        checkpoint_dir="./data"
    )
    
    output_file = Path("./data/seoul_medical_facilities_enriched.parquet")
    enriched_df.to_parquet(output_file, index=False)
    print(f"\n✓ Saved enriched dataset: {output_file}")
    
    upload = input("\nUpload to HuggingFace? (yes/no): ").strip().lower()
    
    if upload == 'yes':
        dataset_name = input("Enter dataset name (e.g., username/dataset-name): ").strip()
        if dataset_name:
            DatasetMerger.upload_to_huggingface(enriched_df, dataset_name)
    
    return enriched_df


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Seoul Medical Facilities Enrichment with Partitioning',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single run (no partitioning):
  python script.py
  
  # Run 4 concurrent instances:
  python script.py --partition 0 --total 4  # Processes rows 0,4,8,12,...
  python script.py --partition 1 --total 4  # Processes rows 1,5,9,13,...
  python script.py --partition 2 --total 4  # Processes rows 2,6,10,14,...
  python script.py --partition 3 --total 4  # Processes rows 3,7,11,15,...
  
  # Merge all partitions:
  python script.py --merge
        """
    )
    parser.add_argument('--partition', type=int, default=0,
                       help='Partition ID X (0-indexed, 0 to Y-1)')
    parser.add_argument('--total', type=int, default=1,
                       help='Total partitions Y (processes every Y-th row)')
    parser.add_argument('--merge', action='store_true',
                       help='Merge all partitions into final dataset')
    
    args = parser.parse_args()
    
    if args.merge:
        enriched_df = merge_and_upload()
    else:
        progress_data = main(
            partition_id=args.partition,
            total_partitions=args.total
        )