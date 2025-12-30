# Naver Maps Review Scraper

Comprehensive review scraper for Naver Maps (네이버 지도) that extracts structured review data including text, images, ratings, and owner responses.

## Features

- ✅ **Automatic Review Tab Navigation**: Finds and clicks the review tab
- ✅ **Auto-Expansion**: Automatically clicks "펼쳐서 더보기" button until all reviews are loaded
- ✅ **Structured Data Extraction**: Parses reviews into clean, structured format
- ✅ **Image URL Extraction**: Captures image URLs without downloading
- ✅ **Owner Response Tracking**: Extracts owner/business responses to reviews
- ✅ **Visit Keywords**: Captures visit metadata (예약 후 이용, 대기 시간, etc.)
- ✅ **Checkpoint System**: Resume scraping from where you left off
- ✅ **Export Formats**: Saves to both Parquet and CSV

## Installation

```bash
# Install required packages
pip install pandas selenium beautifulsoup4 pyarrow --break-system-packages

# Ensure you have ChromeDriver installed
# On Ubuntu:
sudo apt-get install chromium-chromedriver
```

## Quick Start

### Test Single Facility

Test the scraper on a single facility (visible browser for debugging):

```bash
python test_single_review_scrape.py "이선생치과의원"
```

This will:
1. Open browser (visible)
2. Search for the facility
3. Click review tab
4. Expand all reviews
5. Extract and parse reviews
6. Save to JSON file

### Scrape All Facilities

Scrape reviews for all facilities in your dataset:

```bash
python naver_review_scraper.py
```

This assumes you have a `seoul_medical_facilities.parquet` file in `./data/`.

## How It Works

### 1. Review Tab Navigation

The scraper finds and clicks the review tab using:
```python
# Finds tab with data-index="1" containing "리뷰"
review_tab = driver.find_element(By.CSS_SELECTOR, 'a[data-index="1"].tpj9w._tab-menu')
review_tab.click()
```

### 2. Automatic Review Expansion

Keeps clicking the expand button until all reviews are loaded:

```python
# Finds and clicks "펼쳐서 더보기" button
expand_button = driver.find_element(By.CSS_SELECTOR, 'a.fvwqf')
# Clicks until button disappears (all reviews loaded)
```

### 3. HTML Extraction

Extracts the entire review list HTML:

```python
review_list = driver.find_element(By.ID, '_review_list')
html_content = review_list.get_attribute('outerHTML')
```

### 4. Structured Parsing

Parses HTML into structured data:

```python
{
  "reviewer_info": {
    "reviewer_name": "oka****",
    "reviewer_stats": ["리뷰 120", "사진 33"]
  },
  "review_text": "어머니께서 임플란트를 받으셔야 해서...",
  "visit_info": {
    "visit_date": "12.11.목",
    "visit_count": "1번째 방문",
    "verification_method": "영수증"
  },
  "visit_keywords": ["예약 후 이용", "대기 시간 10분 이내"],
  "images": ["https://pup-review-phinf.pstatic.net/..."],
  "owner_response": {
    "owner_name": "이선생치과의원",
    "response_date": "12.17.수",
    "response_text": "안녕하세요 이선생치과입니다..."
  },
  "reactions": {
    "reaction_count": "1"
  }
}
```

## Data Structure

### Extracted Fields

Each review contains:

| Field | Type | Description |
|-------|------|-------------|
| `reviewer_name` | string | Reviewer's display name (e.g., "oka****") |
| `reviewer_stats` | list | Review count and photo count |
| `review_text` | string | Full review text content |
| `visit_date` | string | Date of visit (e.g., "12.11.목") |
| `visit_count` | string | nth visit (e.g., "1번째 방문") |
| `verification_method` | string | How visit was verified (e.g., "영수증") |
| `visit_keywords` | list | Keywords about visit (e.g., ["예약 후 이용"]) |
| `images` | list | List of image URLs |
| `owner_response` | dict | Business owner's response (if any) |
| `reactions` | dict | Number of helpful reactions |

### Output Formats

**Parquet** (`seoul_medical_reviews.parquet`):
- Efficient columnar storage
- Fast loading with pandas
- Compressed format

**CSV** (`seoul_medical_reviews.csv`):
- Human-readable
- Open with Excel/Google Sheets
- UTF-8 with BOM for Korean characters

**JSON** (checkpoint file):
- Preserves nested structure
- Used for checkpoint/resume functionality

## File Structure

```
naver_review_scraper.py          # Main scraper module
test_single_review_scrape.py     # Test script for single facility
data/
  ├── seoul_medical_facilities.parquet      # Input: facilities to scrape
  ├── review_scraping_progress.json         # Checkpoint: progress tracking
  ├── seoul_medical_reviews.parquet         # Output: reviews in Parquet
  └── seoul_medical_reviews.csv             # Output: reviews in CSV
```

## Advanced Usage

### Custom Filtering

Filter facilities before scraping:

```python
from naver_review_scraper import ReviewScrapingOrchestrator
import pandas as pd

# Load facilities
facilities_df = pd.read_parquet("./data/seoul_medical_facilities.parquet")

# Filter for specific district
gangnam_facilities = facilities_df[
    facilities_df['address'].str.contains('강남구', na=False)
]

# Scrape only Gangnam facilities
orchestrator = ReviewScrapingOrchestrator(output_dir="./data")
orchestrator.scrape_all_reviews(gangnam_facilities, save_freq=5, headless=True)
```

### Resume Interrupted Scraping

The scraper automatically saves progress. If interrupted, just run again:

```python
# Progress is saved in review_scraping_progress.json
# Running again will skip already-processed facilities
orchestrator.scrape_all_reviews(facilities_df, save_freq=5)
```

### Headless vs Visible Browser

```python
# Visible browser (for debugging)
scraper = NaverMapsReviewScraper(headless=False)

# Headless browser (for production)
scraper = NaverMapsReviewScraper(headless=True)
```

## Key Components

### ReviewHTMLParser

Parses review HTML into structured data:

```python
parser = ReviewHTMLParser()
reviews = parser.parse_review_list(html_content)
```

### NaverMapsReviewScraper

Handles browser automation:

```python
scraper = NaverMapsReviewScraper(headless=True)
scraper.setup_driver()
review_data = scraper.scrape_reviews_for_facility(name, place_id)
scraper.close_driver()
```

### ReviewCheckpointManager

Manages progress and checkpoints:

```python
checkpoint_mgr = ReviewCheckpointManager()
checkpoint_mgr.add_facility(place_id, review_data)
checkpoint_mgr.save_progress()
```

## Performance

- **Speed**: ~5-10 facilities per minute (varies by review count)
- **Memory**: Minimal (streaming approach)
- **Network**: Polite 2-second delays between requests
- **Checkpoints**: Saves every 5 facilities (configurable)

## Error Handling

The scraper handles:

- ✅ Missing review tabs
- ✅ No reviews available
- ✅ Network timeouts
- ✅ Stale element references
- ✅ Frame switching issues
- ✅ Button click interceptions

Errors are logged and saved in the checkpoint file:

```python
{
  "has_reviews": false,
  "scrape_error": "Review tab not found",
  "scraped_at": "2025-12-30T..."
}
```

## Troubleshooting

### "Review tab not found"

- Facility may not have reviews enabled
- Check if URL is correct
- Try with `headless=False` to see browser

### "Frame switch error"

- Make sure `utils/frame_switch.py` exists
- Check if page structure changed

### "No reviews found"

- Facility might genuinely have no reviews
- Check if expand button was clicked properly
- Look at `review_html` in checkpoint file

### ChromeDriver issues

```bash
# Update ChromeDriver
sudo apt-get update
sudo apt-get install --only-upgrade chromium-chromedriver

# Or download manually from:
# https://chromedriver.chromium.org/
```

## Example Output

### CSV Sample

```csv
place_id,facility_name,reviewer_name,review_text,visit_date,image_count,has_owner_response
1555255676,이선생치과의원,oka****,"어머니께서 임플란트를...",12.11.목,0,true
1555255676,이선생치과의원,benchey94,"이를 꽉 깨물면...",12.10.수,0,true
```

### JSON Sample

```json
{
  "place_id": "1555255676",
  "has_reviews": true,
  "review_count": 10,
  "reviews": [
    {
      "review_index": 1,
      "reviewer_info": {
        "reviewer_name": "oka****",
        "reviewer_stats": ["리뷰 120", "사진 33"]
      },
      "review_text": "어머니께서 임플란트를 받으셔야 해서...",
      "visit_keywords": ["예약 후 이용", "대기 시간 10분 이내"],
      "images": [],
      "owner_response": {
        "owner_name": "이선생치과의원",
        "response_text": "안녕하세요 이선생치과입니다..."
      }
    }
  ]
}
```

## Integration with Medical Info Scraper

Combine with your medical info scraper:

```python
# 1. First scrape medical info
from medical_info_scraper import EnrichmentOrchestrator

med_orchestrator = EnrichmentOrchestrator()
enriched_df = med_orchestrator.enrich_all_facilities(facilities_df)

# 2. Then scrape reviews
from naver_review_scraper import ReviewScrapingOrchestrator

review_orchestrator = ReviewScrapingOrchestrator()
review_df = review_orchestrator.scrape_all_reviews(enriched_df)

# 3. Merge datasets
final_df = enriched_df.merge(
    review_df.groupby('place_id').agg({
        'review_text': 'count'  # Count reviews per facility
    }).rename(columns={'review_text': 'total_reviews'}),
    on='place_id',
    how='left'
)
```

## Notes on Image URLs

- Images are **not downloaded**, only URLs are saved
- URLs are permanent Naver CDN links
- To download images later:

```python
import requests

for image_url in review['images']:
    response = requests.get(image_url)
    with open(f"image_{idx}.jpg", 'wb') as f:
        f.write(response.content)
```

## Rate Limiting

The scraper is polite:
- 2-second delay between facilities
- 1-second delay after expanding reviews
- 0.5-second delay for scroll actions

To be more conservative:

```python
# Increase delays in the scraper
time.sleep(5)  # Instead of time.sleep(2)
```

## License

MIT License - Free to use and modify

## Contributing

Found a bug? Have a feature request?
- Open an issue
- Submit a pull request
- Contact the maintainer

## Disclaimer

This scraper is for educational and research purposes. Always:
- Respect Naver's Terms of Service
- Use reasonable rate limiting
- Don't overload their servers
- Consider using official APIs if available

---

**Happy Scraping! 🚀**
