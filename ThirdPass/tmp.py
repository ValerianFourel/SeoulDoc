import pyarrow.parquet as pq

file_path = "data/seoul_medical_reviews_merged.parquet"
pf = pq.ParquetFile(file_path)

# Nur die erste Row Group lesen
table = pf.read_row_group(0, columns=["review_text"])

# Exakt die ersten 5 Zeilen
table = table.slice(0, 5)

df = table.to_pandas()

print("\nreview_text – erste 5 Einträge:")
for i, txt in enumerate(df["review_text"], 1):
    print(f"\n--- Review {i} ---")
    print(txt)
