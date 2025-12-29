# Naver Maps Detail Scraper - Complete Guide

## Overview

This scraper extracts comprehensive medical facility information from Naver Maps by:
1. Searching for facilities (e.g., "의원 길동")
2. Extracting all results from the search page with place IDs
3. Visiting each facility's detail page
4. Extracting structured information using actual Naver Maps HTML class names

## Files

- **naver_maps_detail_scraper.py** - Main scraper with detail extraction
- **batch_scraper.py** - Batch processing with resume capability
- **requirements.txt** - Python dependencies

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

### 1. Basic Usage - Single Location

```python
from naver_maps_detail_scraper import NaverMapsDetailScraper

with NaverMapsDetailScraper(headless=False, delay=2.0) as scraper:
    results = scraper.scrape_location(
        query='의원',
        location='길동',
        extract_details=True
    )
    
    scraper.save_to_json(results, 'results.json')
    scraper.save_to_csv(results, 'results.csv')
```

### 2. Multiple Locations

```python
from naver_maps_detail_scraper import NaverMapsDetailScraper

queries = ['병원', '의원', '치과']
locations = ['강남역', '명동', '홍대입구동']

with NaverMapsDetailScraper(headless=True, delay=2.5) as scraper:
    results = scraper.scrape_multiple_locations(
        queries=queries,
        locations=locations,
        extract_details=True
    )
    
    scraper.save_to_json(results, 'multiple_locations.json')
```

### 3. Extract Only URLs (Fast Mode)

```python
with NaverMapsDetailScraper(headless=True, delay=1.5) as scraper:
    results = scraper.scrape_location(
        query='병원',
        location='강남역',
        extract_details=False  # Only get URLs, much faster
    )
```

## Batch Scraping All Seoul

### Basic Command

```bash
# Scrape all hospitals and clinics in all Seoul districts
python batch_scraper.py --headless --delay 3.0
```

### Command Line Options

```bash
# Scrape specific queries
python batch_scraper.py --queries 병원 의원 치과 --headless

# Scrape specific districts
python batch_scraper.py --districts 강남구 종로구 --headless

# Custom output directory
python batch_scraper.py --output-dir my_data --headless

# Adjust delay (be respectful!)
python batch_scraper.py --delay 5.0 --headless

# Show statistics
python batch_scraper.py --stats

# Merge all results into one file
python batch_scraper.py --merge
```

### Resume After Interruption

The batch scraper automatically tracks progress. If interrupted (Ctrl+C or crash):

```bash
# Just run the same command again - it will resume
python batch_scraper.py --headless --delay 3.0
```

Progress is saved in `scraped_data/progress.json`

### Start Fresh

```bash
# Delete progress file to start over
rm scraped_data/progress.json
python batch_scraper.py --headless
```

## Extracted Information

The scraper extracts the following fields from each facility:

### Basic Info (from search results)
- `name` - Facility name
- `category` - Type (치과, 병원, 의원, etc.)
- `address_preview` - Brief address from search results
- `place_id` - Naver Maps place ID
- `detail_url` - Clean URL (format: `/place/{id}`)
- `search_query` - Original search query
- `search_location` - Search location
- `result_position` - Position in search results

### Detailed Info (from detail page)
- `address` - Full address
- `subway_info` - Nearest subway station and distance
- `directions` - How to get there
- `status` - Current status (진료 중, 진료 종료, etc.)
- `next_status` - Next opening time
- `holiday` - Holiday type
- `holiday_dates` - Holiday dates
- `phone` - Phone number
- `website` - Official website
- `blog` - Official blog
- `amenities` - Available amenities (parking, WiFi, etc.)
- `wheelchair_accessible` - Boolean
- `visitor_reviews` - Number of visitor reviews
- `blog_reviews` - Number of blog reviews
- `specialists` - Medical specialists (name: count)
- `medical_departments` - List of departments
- `special_equipment` - Medical equipment available
- `scraped_at` - Timestamp

## Output Format

### JSON Example

```json
{
  "name": "강동씨엘치과의원",
  "category": "치과",
  "place_id": "38004385",
  "detail_url": "https://map.naver.com/p/search/의원/place/38004385",
  "address": "서울 강동구 양재대로 1473 3층",
  "subway_info": "5길동역 1번 출구에서 47m",
  "phone": "02-471-7755",
  "website": "http://gangdongcl.com",
  "status": "진료 종료",
  "next_status": "내일 휴무",
  "visitor_reviews": "775",
  "blog_reviews": "1,870",
  "wheelchair_accessible": true,
  "amenities": "무선 인터넷, 남/녀 화장실 구분, 유아시설 (놀이방), 예약, 유아의자, 대기공간, 주차",
  "specialists": "소아치과: 1, 통합치의학과: 1",
  "medical_departments": "소아치과, 통합치의학과, 구강내과, ...",
  "scraped_at": "2024-12-27T21:30:00"
}
```

### CSV Format

All fields are exported to CSV with UTF-8 encoding (compatible with Excel).

## URL Cleaning

The scraper automatically cleans URLs:

**Original:**
```
https://map.naver.com/p/search/의원/place/38004385?placePath=/home?from=map&from=map&fromPanelNum=2&timestamp=202512272123&locale=ko&svcName=map_pcv5&searchText=의원%20길동&c=15.00,0,0,0,dh
```

**Cleaned:**
```
https://map.naver.com/p/search/의원/place/38004385
```

This keeps only the essential query and place ID.

## Performance

### Timing Estimates
- **Single facility (detail):** ~5-7 seconds
- **Search results only (10 facilities):** ~10 seconds
- **Search results + details (10 facilities):** ~60-80 seconds
- **One district (7 locations, 병원):** ~8-12 minutes
- **All Seoul (424 locations, 병원):** ~30-40 hours

### Optimization Tips

1. **Use headless mode** for long runs:
   ```python
   scraper = NaverMapsDetailScraper(headless=True)
   ```

2. **Adjust delay** based on your needs:
   - Fast (risky): `delay=1.5`
   - Balanced: `delay=2.5`
   - Safe: `delay=3.5`

3. **Extract URLs first, details later**:
   ```python
   # Step 1: Fast - get all URLs
   urls = scraper.scrape_location(query, location, extract_details=False)
   
   # Step 2: Process details in batches
   for url_batch in chunks(urls, 50):
       # Process 50 at a time
   ```

4. **Use batch scraper** with resume capability for large jobs

## Best Practices

### 1. Respectful Scraping
```python
# Use appropriate delays
scraper = NaverMapsDetailScraper(delay=3.0)

# Don't run multiple scrapers simultaneously
# Spread out scraping over time
```

### 2. Data Quality
```python
# Check for errors
if result.get('phone'):
    # Process
else:
    # Handle missing data

# Validate data
if result.get('place_id'):
    # Place ID is essential
```

### 3. Error Handling
```python
try:
    results = scraper.scrape_location(query, location)
except Exception as e:
    print(f"Error: {e}")
    # Log and continue
```

## Troubleshooting

### Problem: No results found
**Solution:**
- Check if the location name is correct
- Try broader search terms
- Run in non-headless mode to see what's happening

### Problem: Detail extraction fails
**Solution:**
- Increase delay: `delay=5.0`
- Check if Naver Maps updated their HTML structure
- Look at saved debug HTML files

### Problem: Browser crashes
**Solution:**
- Use headless mode
- Reduce batch size
- Add more memory to your system

### Problem: Interrupted scraping
**Solution:**
- Use batch scraper - it auto-resumes
- Or manually track completed locations

## Advanced Usage

### Custom Data Extraction

Add custom fields to `extract_detail_page_info`:

```python
# In naver_maps_detail_scraper.py, add:

try:
    custom_elem = self.driver.find_element(By.CSS_SELECTOR, "div.custom-class")
    info['custom_field'] = custom_elem.text.strip()
except:
    pass
```

### Filter Results

```python
# Only facilities with phone numbers
results_with_phone = [r for r in results if r.get('phone')]

# Only open now
results_open = [r for r in results if '진료 중' in r.get('status', '')]

# Only wheelchair accessible
results_accessible = [r for r in results if r.get('wheelchair_accessible')]
```

### Combine with Analysis

```python
from analyze_data import HospitalDataAnalyzer

# Scrape
results = scraper.scrape_location('병원', '강남역')

# Analyze
analyzer = HospitalDataAnalyzer(results)
analyzer.summary_statistics()
analyzer.category_breakdown()
```

## Data Storage

### Recommended Structure

```
scraped_data/
├── progress.json                    # Progress tracking
├── 강남구_병원_역삼동_20241227.json
├── 강남구_병원_역삼동_20241227.csv
├── 종로구_의원_종로동_20241227.json
└── all_results_merged.json          # Merged results
```

### Merge All Results

```bash
python batch_scraper.py --merge
```

This creates:
- `all_results_merged.json` - All results in one file
- `all_results_merged.csv` - CSV version

## Search Queries Reference

Common medical facility queries:

**General:**
- `병원` - Hospital
- `의원` - Clinic
- `종합병원` - General hospital

**Specialties:**
- `치과` - Dental
- `내과` - Internal medicine
- `정형외과` - Orthopedics
- `피부과` - Dermatology
- `이비인후과` - ENT
- `안과` - Ophthalmology
- `산부인과` - OB-GYN
- `소아과` - Pediatrics

**Special Services:**
- `영어 가능한 병원` - English-speaking
- `24시간 병원` - 24-hour
- `응급실` - Emergency room

## License & Ethics

- For research and personal use only
- Respect Naver's Terms of Service
- Use appropriate delays
- Don't overload servers
- Give credit to data source

## Support

Issues? Check:
1. Chrome/ChromeDriver versions match
2. Internet connection stable
3. Naver Maps structure hasn't changed
4. Sufficient disk space for results
