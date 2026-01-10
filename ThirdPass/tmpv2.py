import polars as pl

file_path = "data/seoul_medical_reviews_merged.parquet"

df = (
    pl.scan_parquet(file_path)
    .filter(
        pl.col("review_text")
        .str.contains("영어|[Ee]nglish")
    )
    .select([
        pl.col("place_id"),
        pl.col("facility_name"),
        pl.col("review_text")
    ])
    .collect()
)

print("Anzahl gefundener Reviews:", df.height)
print(df.head(10))
