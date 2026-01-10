import pyarrow.parquet as pq
import sys

def main(parquet_path: str):
    parquet_file = pq.ParquetFile(parquet_path)

    # Anzahl der Zeilen (Entries)
    num_rows = parquet_file.metadata.num_rows

    # Spaltennamen
    schema = parquet_file.schema_arrow
    columns = schema.names

    print(f"Number of entries (rows): {num_rows}\n")
    print("Columns:")
    for col in columns:
        print(col)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python read_parquet_metadata.py <path_to_parquet>")
        sys.exit(1)

    main(sys.argv[1])
