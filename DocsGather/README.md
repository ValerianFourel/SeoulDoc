---
license: cc-by-4.0
task_categories:
- other
language:
- ko
tags:
- medical
- healthcare
- seoul
- south-korea
- facilities
- geospatial
pretty_name: Seoul Medical Facilities Dataset
size_categories:
- 10K<n<100K
---

# Seoul Medical Facilities Dataset

## Dataset Description

This dataset contains comprehensive information about **unique** medical facilities (hospitals, clinics) across all administrative districts (구) and neighborhoods (동) in Seoul, South Korea.

**Note**: This dataset contains only unique facilities. Duplicates have been removed based on `place_id`, with the most complete record retained for each facility.

### Dataset Summary

- **Unique Facilities**: 5,727
- **Districts Covered**: 18
- **Neighborhoods (Dong) Covered**: 204
- **Collection Period**: 2025-12-28 to 2025-12-30
- **Source**: Naver Maps
- **Language**: Korean
- **Deduplication**: Yes (by place_id)

### Data Collection

Data was collected by systematically scraping Naver Maps for medical facilities across Seoul's administrative divisions:
- **Keywords**: 병원 (hospital), 의원 (clinic), 클리닉 (clinic)
- **Coverage**: All 25 districts (구) and 424+ neighborhoods (동)
- **Method**: Automated web scraping with Selenium
- **Deduplication**: Facilities appearing in multiple keyword searches are deduplicated by `place_id`

### Facility Types

The dataset includes three types of medical facilities:
1. **병원 (Byeongwon)** - Hospitals
2. **의원 (Uiwon)** - Clinics/Medical offices
3. **클리닉 (Keullinik)** - Specialized clinics

**Note**: Each facility appears only once, even if it matched multiple search keywords.

## Dataset Structure

### Data Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Facility name |
| `category` | string | Facility category/type |
| `address` | string | Street address |
| `phone` | string | Contact phone number |
| `place_id` | string | **Unique** Naver Maps place identifier |
| `url` | string | Naver Maps URL |
| `reviews` | string | Review ratings and counts |
| `hours_status` | string | Current operating status |
| `business_hours` | string | Detailed business hours |
| `amenities` | string | Available amenities/facilities |
| `website` | string | Official website (if available) |
| `file_district` | string | Seoul district (구) |
| `file_dong` | string | Neighborhood (동) |
| `file_keyword` | string | Search keyword used (from original search) |
| `scraped_at` | string | Timestamp of data collection |

**Important**: `place_id` is the unique identifier. Each `place_id` appears exactly once in the dataset.

### Geographic Coverage

Seoul's 25 districts (구):
- Gangnam-gu, Gangdong-gu, Gangbuk-gu, Gangseo-gu, Gwanak-gu, Gwangjin-gu, Guro-gu, Geumcheon-gu, Nowon-gu, Dobong-gu, Dongdaemun-gu, Dongjak-gu, Mapo-gu, Seodaemun-gu, Seocho-gu, Seongdong-gu, Seongbuk-gu, Songpa-gu, Yangcheon-gu, Yeongdeungpo-gu, Yongsan-gu, Eunpyeong-gu, Jongno-gu, Jung-gu, Jungnang-gu

## Usage

### Load with Pandas

```python
import pandas as pd

# Load parquet file
df = pd.read_parquet("hf://datasets/ValerianFourel/seoul-medical-facilities/seoul_medical_facilities.parquet")

# Basic exploration
print(f"Total unique facilities: {len(df):,}")
print(f"Districts: {df['file_district'].nunique()}")

# Each place_id is unique
assert df['place_id'].is_unique

# Filter by district
gangnam = df[df['file_district'] == 'Gangnam-gu']
print(f"Gangnam facilities: {len(gangnam):,}")

# Filter by facility type
hospitals = df[df['file_keyword'] == '병원']
print(f"Hospitals: {len(hospitals):,}")
```

### Load with Datasets

```python
from datasets import load_dataset

dataset = load_dataset("ValerianFourel/seoul-medical-facilities")
df = dataset['train'].to_pandas()

# Verify uniqueness
print(f"Unique facilities: {len(df):,}")
print(f"Unique place_ids: {df['place_id'].nunique():,}")
assert len(df) == df['place_id'].nunique()
```

## Use Cases

- **Healthcare Access Analysis**: Study distribution of medical facilities across Seoul
- **Geographic Analysis**: Map healthcare infrastructure by district/neighborhood
- **Urban Planning**: Identify underserved areas
- **Public Health Research**: Analyze healthcare availability patterns
- **Business Intelligence**: Market analysis for medical services
- **Navigation/Directory Apps**: Build medical facility finders

## Data Quality Notes

- **Deduplication**: Each facility appears exactly once based on `place_id`
- **Completeness**: For duplicate entries, the record with most complete information was retained
- **Unique Identifier**: Use `place_id` to reference specific facilities
- Phone numbers and websites may not be available for all facilities
- Business hours may change; check official sources for current information
- Review data is a snapshot at collection time

## Limitations

- Data represents a snapshot at collection time
- Some fields may be incomplete (N/A values)
- Limited to facilities discoverable via Naver Maps
- Does not include detailed medical specialties or services
- Operating hours and contact information may change

## Citation

If you use this dataset, please cite:

```bibtex
@dataset{seoul_medical_facilities_2024,
  author = {Fourel, Valerian},
  title = {Seoul Medical Facilities Dataset},
  year = {2024},
  publisher = {Hugging Face},
  url = {https://huggingface.co/datasets/ValerianFourel/seoul-medical-facilities}
}
```

## License

This dataset is released under the Creative Commons Attribution 4.0 International (CC BY 4.0) license.

## Maintenance

- **Maintainer**: ValerianFourel
- **Last Updated**: 2025-12-30

## Acknowledgments

Data sourced from Naver Maps. This dataset is intended for research and educational purposes.
