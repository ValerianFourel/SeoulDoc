# Quick Start Guide - Naver Maps Hospital Scraper

## 🚀 Getting Started in 5 Minutes

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Run Your First Scrape

#### Option A: Run the examples script (Recommended for beginners)
```bash
python examples.py
```
This will give you an interactive menu to choose from different example scenarios.

#### Option B: Quick test with a few locations
```bash
python naver_maps_scraper_improved.py
```
This will scrape hospitals in 명동, 이태원동, and 강남역 areas.

### Step 3: View Your Results
The scraper will create two files:
- `naver_hospitals_YYYYMMDD_HHMMSS.json` - JSON format
- `naver_hospitals_YYYYMMDD_HHMMSS.csv` - Excel-compatible CSV

## 📊 Analyzing Your Data

After scraping, analyze your results:

```bash
python analyze_data.py naver_hospitals_YYYYMMDD_HHMMSS.json
```

This will show:
- Summary statistics
- Breakdown by category
- Results by location
- Rating analysis
- English-friendly facilities

## 🎯 Common Use Cases

### Use Case 1: Find English-Speaking Hospitals in Popular Areas
```python
from naver_maps_scraper_improved import ImprovedNaverMapsScraper

locations = ['이태원동', '한남동', '역삼동', '강남역']
queries = ['영어 가능한 병원', '외국인 클리닉']

with ImprovedNaverMapsScraper(headless=True, delay=2.5) as scraper:
    results = scraper.scrape_batch(queries, locations)
    scraper.save_to_json(results, 'english_hospitals.json')
```

### Use Case 2: Find 24-Hour Hospitals
```python
from naver_maps_scraper_improved import ImprovedNaverMapsScraper

locations = ['강남역', '명동', '종로', '신촌동']
queries = ['24시간 병원', '응급실']

with ImprovedNaverMapsScraper(headless=True, delay=2.5) as scraper:
    results = scraper.scrape_batch(queries, locations)
    scraper.save_to_csv(results, '24hour_hospitals.csv')
```

### Use Case 3: Find Specialists in Your Neighborhood
```python
from naver_maps_scraper_improved import ImprovedNaverMapsScraper

# Replace with your neighborhood
my_area = '연남동'

specialties = [
    '내과',      # Internal medicine
    '치과',      # Dental
    '피부과',    # Dermatology
]

with ImprovedNaverMapsScraper(headless=False, delay=2.0) as scraper:
    results = scraper.scrape_batch(specialties, [my_area])
    scraper.save_to_json(results, f'{my_area}_specialists.json')
```

### Use Case 4: Scrape a Whole District
```python
from naver_maps_scraper_improved import scrape_full_district

# Scrape all hospitals in Gangnam-gu
scrape_full_district('강남구', '병원')
```

## 🔍 Search Query Reference

### General Facilities
- `병원` - Hospital (general)
- `의원` - Clinic
- `종합병원` - General hospital
- `클리닉` - Clinic

### For Foreigners
- `영어 가능한 병원` - English-speaking hospital
- `외국인 클리닉` - Foreigner clinic
- `국제진료센터` - International medical center

### Specialties
- `내과` - Internal medicine
- `외과` - Surgery
- `정형외과` - Orthopedics
- `피부과` - Dermatology
- `치과` - Dental
- `안과` - Ophthalmology
- `이비인후과` - ENT
- `산부인과` - OB-GYN
- `소아과` - Pediatrics
- `정신과` - Psychiatry

### Service Types
- `24시간 병원` - 24-hour hospital
- `야간 진료` - Evening care
- `주말 진료` - Weekend care
- `응급실` - Emergency room

## ⚙️ Configuration Tips

### For Faster Scraping
```python
scraper = ImprovedNaverMapsScraper(
    headless=True,    # No browser window
    delay=1.5         # Shorter delay (be respectful!)
)
```

### For Debugging
```python
scraper = ImprovedNaverMapsScraper(
    headless=False,   # See what's happening
    delay=3.0         # Longer delay to watch
)
```

### For Maximum Results
```python
results = scraper.scrape_batch(
    queries=queries,
    locations=locations,
    max_results_per_search=100  # Get more results
)
```

## 📁 Output Format

### JSON Output Example
```json
{
  "name": "서울대학교병원",
  "category": "종합병원",
  "address": "서울특별시 종로구 대학로 101",
  "phone": "02-2072-2114",
  "url": "https://map.naver.com/...",
  "rating": "4.5",
  "search_query": "병원",
  "search_location": "혜화동",
  "scraped_at": "2024-01-15T10:30:00"
}
```

### CSV Output
Opens directly in Excel with all the same fields in columns.

## 🛠️ Troubleshooting

### "ChromeDriver not found"
→ The script will auto-download it. If it fails, check your internet connection.

### "No results found"
→ Try running with `headless=False` to see what's happening
→ Increase the delay: `delay=5.0`

### Timeout errors
→ Increase delay between requests
→ Check your internet connection

### Incomplete results
→ Increase `max_results_per_search`
→ Modify scroll iterations in the code

## 📈 Next Steps

1. **Start small**: Test with 2-3 locations first
2. **Review results**: Check the output files
3. **Analyze data**: Use `analyze_data.py` to understand your results
4. **Scale up**: Once working, scrape more locations
5. **Customize**: Modify the code for your specific needs

## 💡 Pro Tips

1. **Batch by district**: Scrape one district at a time and save separately
2. **Use filters**: Search for specific types first, then general hospitals
3. **Check data quality**: Some results may have incomplete information
4. **Respect rate limits**: Use delays of at least 2 seconds
5. **Save incrementally**: For large jobs, save after each district

## 🆘 Need Help?

1. Check the README.md for detailed documentation
2. Look at examples.py for usage patterns
3. Run analyze_data.py to understand your data structure
4. Check debug HTML files if scraping fails

## ⏱️ Time Estimates

- Single location: ~5-10 seconds
- 10 locations: ~1-2 minutes
- One district: ~5-10 minutes
- All Seoul: ~1-2 hours (with 2-3 second delays)

Happy scraping! 🏥
