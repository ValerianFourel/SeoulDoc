# Dataset configuration
DATASET_NAME = "ValerianFourel/seoul-medical-facilities"
DATA_DIR = "./data"
CHECKPOINT_DIR = "./data/checkpoints"

# Scraping configuration
HEADLESS = True
CHECKPOINT_FREQUENCY = 10
MAX_SCROLLS = 30
SCROLL_DELAY = 1.5
REQUEST_DELAY = 2

# Output files
FACILITIES_FILE = "seoul_medical_facilities.parquet"
REVIEWS_FILE = "seoul_medical_reviews.parquet"
FAILED_LOG = "failed_facilities.csv"
