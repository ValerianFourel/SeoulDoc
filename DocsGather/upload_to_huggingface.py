#!/usr/bin/env python3
"""
Upload Seoul Medical Facilities Data to HuggingFace
Combines all scraped data into parquet and uploads to HF Hub
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from huggingface_hub import HfApi, create_repo, upload_file
import os

# HuggingFace Configuration
HF_USERNAME = "ValerianFourel"
HF_TOKEN = os.environ("HF_TOKEN")  # Ensure your token is set in env variables
DATASET_NAME = "seoul-medical-facilities"
REPO_ID = f"{HF_USERNAME}/{DATASET_NAME}"


def load_all_data(data_dir: str = "seoul_medical_data") -> pd.DataFrame:
    """
    Load all JSON files from scraped data directory
    
    Args:
        data_dir: Base directory with scraped data
        
    Returns:
        DataFrame with all combined data
    """
    data_path = Path(data_dir)
    
    if not data_path.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")
    
    print(f"\n{'='*60}")
    print(f"Loading data from: {data_dir}")
    print(f"{'='*60}")
    
    # Find all JSON files (exclude progress.json)
    json_files = [f for f in data_path.rglob("*.json") 
                  if f.name not in ['progress.json']]
    
    if not json_files:
        raise ValueError(f"No JSON files found in {data_dir}")
    
    print(f"Found {len(json_files)} JSON files")
    
    all_data = []
    files_processed = 0
    files_empty = 0
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                if data:
                    # Add file metadata
                    relative_path = json_file.relative_to(data_path)
                    for item in data:
                        item['file_district'] = relative_path.parts[0]
                        item['file_dong'] = relative_path.parts[1]
                        item['file_keyword'] = relative_path.stem
                    
                    all_data.extend(data)
                    files_processed += 1
                else:
                    files_empty += 1
        
        except Exception as e:
            print(f"✗ Error reading {json_file}: {e}")
            continue
    
    print(f"\n📊 Summary:")
    print(f"  Files processed: {files_processed}")
    print(f"  Empty files: {files_empty}")
    print(f"  Total records: {len(all_data):,}")
    
    if not all_data:
        raise ValueError("No data found in JSON files")
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data)
    
    # Print column info
    print(f"\n📋 Columns: {len(df.columns)}")
    print(f"  {', '.join(df.columns[:10])}...")
    
    # Print stats before deduplication
    print(f"\n🔢 Before deduplication:")
    print(f"  Total records: {len(df):,}")
    if 'place_id' in df.columns:
        print(f"  Unique place_ids: {df['place_id'].nunique():,}")
    
    # Remove duplicates based on place_id
    print(f"\n🔄 Removing duplicates...")
    
    if 'place_id' in df.columns:
        # Count duplicates
        duplicates = len(df) - df['place_id'].nunique()
        
        # For duplicates, keep the record with most non-null values
        def count_non_null(row):
            return row.notna().sum()
        
        # Add helper column
        df['_completeness'] = df.apply(count_non_null, axis=1)
        
        # Sort by completeness (descending) then drop duplicates
        df = df.sort_values('_completeness', ascending=False)
        df = df.drop_duplicates(subset='place_id', keep='first')
        
        # Remove helper column
        df = df.drop('_completeness', axis=1)
        
        print(f"  ✅ Removed {duplicates:,} duplicate entries")
        print(f"  ✅ Kept records with most complete information")
    else:
        print(f"  ⚠️  No place_id column found, skipping deduplication")
    
    # Print stats after deduplication
    print(f"\n🔢 After deduplication:")
    print(f"  Unique facilities: {len(df):,}")
    if 'file_district' in df.columns:
        print(f"  Districts: {df['file_district'].nunique()}")
    if 'file_dong' in df.columns:
        print(f"  Dongs: {df['file_dong'].nunique()}")
    if 'file_keyword' in df.columns:
        print(f"  Keywords represented: {df['file_keyword'].nunique()}")
    
    return df


def create_dataset_card(df: pd.DataFrame, output_path: str = "README.md") -> str:
    """
    Create a README.md dataset card for HuggingFace
    
    Args:
        df: DataFrame with the data (already deduplicated)
        output_path: Where to save README
        
    Returns:
        Path to created README
    """
    # df is already deduplicated, so len(df) = unique facilities
    unique_facilities = len(df)
    districts = df['file_district'].nunique() if 'file_district' in df.columns else 'Unknown'
    dongs = df['file_dong'].nunique() if 'file_dong' in df.columns else 'Unknown'
    
    # Get date range if available
    date_info = ""
    if 'scraped_at' in df.columns:
        try:
            df['scraped_at_dt'] = pd.to_datetime(df['scraped_at'])
            min_date = df['scraped_at_dt'].min().strftime('%Y-%m-%d')
            max_date = df['scraped_at_dt'].max().strftime('%Y-%m-%d')
            date_info = f"\n- **Collection Period**: {min_date} to {max_date}"
        except:
            pass
    
    readme_content = f"""---
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

- **Unique Facilities**: {unique_facilities:,}
- **Districts Covered**: {districts}
- **Neighborhoods (Dong) Covered**: {dongs}{date_info}
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
df = pd.read_parquet("hf://datasets/{REPO_ID}/seoul_medical_facilities.parquet")

# Basic exploration
print(f"Total unique facilities: {{len(df):,}}")
print(f"Districts: {{df['file_district'].nunique()}}")

# Each place_id is unique
assert df['place_id'].is_unique

# Filter by district
gangnam = df[df['file_district'] == 'Gangnam-gu']
print(f"Gangnam facilities: {{len(gangnam):,}}")

# Filter by facility type
hospitals = df[df['file_keyword'] == '병원']
print(f"Hospitals: {{len(hospitals):,}}")
```

### Load with Datasets

```python
from datasets import load_dataset

dataset = load_dataset("{REPO_ID}")
df = dataset['train'].to_pandas()

# Verify uniqueness
print(f"Unique facilities: {{len(df):,}}")
print(f"Unique place_ids: {{df['place_id'].nunique():,}}")
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
@dataset{{seoul_medical_facilities_2024,
  author = {{Fourel, Valerian}},
  title = {{Seoul Medical Facilities Dataset}},
  year = {{2024}},
  publisher = {{Hugging Face}},
  url = {{https://huggingface.co/datasets/{REPO_ID}}}
}}
```

## License

This dataset is released under the Creative Commons Attribution 4.0 International (CC BY 4.0) license.

## Maintenance

- **Maintainer**: ValerianFourel
- **Last Updated**: {datetime.now().strftime('%Y-%m-%d')}

## Acknowledgments

Data sourced from Naver Maps. This dataset is intended for research and educational purposes.
"""
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(readme_content)
    
    print(f"\n✅ Created dataset card: {output_path}")
    
    return output_path


def save_to_parquet(df: pd.DataFrame, output_path: str = "seoul_medical_facilities.parquet") -> str:
    """
    Save DataFrame to parquet format
    
    Args:
        df: DataFrame to save
        output_path: Output file path
        
    Returns:
        Path to saved file
    """
    print(f"\n{'='*60}")
    print(f"Saving to parquet...")
    print(f"{'='*60}")
    
    # Convert datetime columns if present
    for col in df.columns:
        if 'time' in col.lower() or 'date' in col.lower() or col == 'scraped_at':
            try:
                df[col] = pd.to_datetime(df[col], errors='ignore')
            except:
                pass
    
    # Save to parquet
    df.to_parquet(output_path, index=False, compression='snappy')
    
    # Get file size
    file_size_mb = Path(output_path).stat().st_size / (1024 * 1024)
    
    print(f"✅ Saved to: {output_path}")
    print(f"   Rows: {len(df):,}")
    print(f"   Columns: {len(df.columns)}")
    print(f"   File size: {file_size_mb:.2f} MB")
    
    return output_path


def upload_to_huggingface(
    parquet_path: str,
    readme_path: str,
    token: str = HF_TOKEN,
    repo_id: str = REPO_ID,
    private: bool = False
) -> str:
    """
    Upload dataset to HuggingFace Hub
    
    Args:
        parquet_path: Path to parquet file
        readme_path: Path to README.md
        token: HuggingFace API token
        repo_id: Repository ID (username/dataset-name)
        private: Whether to make repo private
        
    Returns:
        URL to the dataset
    """
    print(f"\n{'='*60}")
    print(f"Uploading to HuggingFace...")
    print(f"{'='*60}")
    
    api = HfApi()
    
    # Create repository
    print(f"📦 Creating/updating repository: {repo_id}")
    try:
        repo_url = create_repo(
            repo_id=repo_id,
            token=token,
            repo_type="dataset",
            private=private,
            exist_ok=True
        )
        print(f"✅ Repository ready: {repo_url}")
    except Exception as e:
        print(f"⚠️  Repository may already exist: {e}")
        repo_url = f"https://huggingface.co/datasets/{repo_id}"
    
    # Upload parquet file
    print(f"\n📤 Uploading parquet file...")
    try:
        api.upload_file(
            path_or_fileobj=parquet_path,
            path_in_repo=Path(parquet_path).name,
            repo_id=repo_id,
            repo_type="dataset",
            token=token
        )
        print(f"✅ Uploaded: {parquet_path}")
    except Exception as e:
        print(f"❌ Error uploading parquet: {e}")
        raise
    
    # Upload README
    print(f"\n📤 Uploading README...")
    try:
        api.upload_file(
            path_or_fileobj=readme_path,
            path_in_repo="README.md",
            repo_id=repo_id,
            repo_type="dataset",
            token=token
        )
        print(f"✅ Uploaded: {readme_path}")
    except Exception as e:
        print(f"❌ Error uploading README: {e}")
        raise
    
    print(f"\n{'='*60}")
    print(f"🎉 Upload complete!")
    print(f"{'='*60}")
    print(f"🔗 Dataset URL: {repo_url}")
    print(f"\nYou can now access your dataset:")
    print(f"  - Web: {repo_url}")
    print(f"  - Python: load_dataset('{repo_id}')")
    print(f"{'='*60}\n")
    
    return repo_url


def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Upload Seoul medical facilities data to HuggingFace',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--data-dir', default='seoul_medical_data',
                       help='Directory with scraped data')
    parser.add_argument('--output', default='seoul_medical_facilities.parquet',
                       help='Output parquet filename')
    parser.add_argument('--repo-name', default=DATASET_NAME,
                       help='HuggingFace dataset repository name')
    parser.add_argument('--username', default=HF_USERNAME,
                       help='HuggingFace username')
    parser.add_argument('--token', default=HF_TOKEN,
                       help='HuggingFace API token')
    parser.add_argument('--private', action='store_true',
                       help='Make repository private')
    parser.add_argument('--no-upload', action='store_true',
                       help='Only create files, do not upload')
    
    args = parser.parse_args()
    
    # Update repo_id if username or repo name changed
    repo_id = f"{args.username}/{args.repo_name}"
    
    print(f"\n{'#'*60}")
    print(f"Seoul Medical Facilities → HuggingFace Upload")
    print(f"{'#'*60}")
    print(f"Data directory: {args.data_dir}")
    print(f"Repository: {repo_id}")
    print(f"Private: {args.private}")
    print(f"{'#'*60}\n")
    
    try:
        # Step 1: Load all data
        df = load_all_data(args.data_dir)
        
        # Step 2: Create dataset card
        readme_path = create_dataset_card(df)
        
        # Step 3: Save to parquet
        parquet_path = save_to_parquet(df, args.output)
        
        # Step 4: Upload to HuggingFace
        if not args.no_upload:
            upload_to_huggingface(
                parquet_path=parquet_path,
                readme_path=readme_path,
                token=args.token,
                repo_id=repo_id,
                private=args.private
            )
        else:
            print(f"\n⏭️  Skipping upload (--no-upload specified)")
            print(f"Files ready for upload:")
            print(f"  - {parquet_path}")
            print(f"  - {readme_path}")
        
        print(f"\n✅ All done!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())