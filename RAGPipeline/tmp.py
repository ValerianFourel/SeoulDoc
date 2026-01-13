import pandas as pd

# Load the parquet file
df = pd.read_parquet("../../seoul-medical-facilities/seoul_medical_facilities_enriched.parquet")

# Get unique categories
categories = sorted(df["category"].dropna().unique())

print(categories)

