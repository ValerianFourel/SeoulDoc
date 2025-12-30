import os

# API Configuration
GROQ_MODEL = "llama-3.3-70b-versatile"

# Dataset Configuration
DATASET_NAME = "ValerianFourel/seoul-medical-facilities"
DATA_DIR = "./data"
CHECKPOINT_DIR = "./data/enrichment_checkpoints"

# Scraping Configuration
HEADLESS = True
CHECKPOINT_FREQUENCY = 50
REQUEST_DELAY = 2
# Output Files
FACILITIES_FILE = "seoul_medical_facilities.parquet"
ENRICHED_FILE = "seoul_medical_facilities_enriched.parquet"
FAILED_LOG = "enrichment_failed.csv"
PARSED_JSON = "medical_info_parsed.json"