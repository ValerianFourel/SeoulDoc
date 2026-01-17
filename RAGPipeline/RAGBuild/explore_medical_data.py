import pandas as pd
import numpy as np
from pathlib import Path

def print_section(title):
    """Print a formatted section header"""
    print("\n" + "="*80)
    print(f" {title}")
    print("="*80 + "\n")

def is_array_column(series):
    """Check if column contains arrays/lists"""
    try:
        first_valid = series.dropna().iloc[0] if len(series.dropna()) > 0 else None
        return isinstance(first_valid, (np.ndarray, list))
    except:
        return False

def explore_dataframe(df, name):
    """Explore a single dataframe"""
    print_section(f"{name} - Overview")
    
    # Basic info
    print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns\n")
    
    # Column types
    print("Column Data Types:")
    print("-" * 80)
    for col, dtype in df.dtypes.items():
        null_count = df[col].isna().sum()
        null_pct = (null_count / len(df)) * 100
        print(f"  {col:40s} | {str(dtype):15s} | Nulls: {null_count:6,} ({null_pct:5.1f}%)")
    
    print("\n")
    
    # First few rows
    print("Sample Rows (first 3):")
    print("-" * 80)
    try:
        print(df.head(3).to_string())
    except:
        print("(Cannot display - contains complex data types)")
        print(df.head(3))
    print("\n")
    
    # For each column, show unique values or statistics
    for col in df.columns:
        print(f"\nColumn: {col}")
        print("-" * 80)
        
        # Check if it's an array column
        if is_array_column(df[col]):
            print(f"  Type: Array/List column (embeddings or structured data)")
            non_null = df[col].dropna()
            if len(non_null) > 0:
                first_item = non_null.iloc[0]
                if isinstance(first_item, np.ndarray):
                    print(f"  Array shape: {first_item.shape}")
                    print(f"  Array dtype: {first_item.dtype}")
                    print(f"  Sample array (first 10 elements): {first_item[:10]}")
                elif isinstance(first_item, list):
                    print(f"  List length: {len(first_item)}")
                    print(f"  Sample list (first 10 elements): {first_item[:10]}")
            continue
        
        if df[col].dtype == 'object' or df[col].dtype.name == 'category':
            try:
                unique_count = df[col].nunique()
                print(f"  Unique values: {unique_count:,}")
                
                if unique_count <= 20:
                    print(f"  All unique values:")
                    value_counts = df[col].value_counts()
                    for val, count in value_counts.items():
                        print(f"    - {val}: {count:,} times")
                else:
                    print(f"  Top 10 most common:")
                    value_counts = df[col].value_counts().head(10)
                    for val, count in value_counts.items():
                        val_str = str(val)[:100]  # Truncate long values
                        print(f"    - {val_str}: {count:,} times")
                
                # Show some examples
                print(f"\n  Sample values:")
                samples = df[col].dropna().head(3).tolist()
                for i, sample in enumerate(samples, 1):
                    # Truncate if too long
                    sample_str = str(sample)
                    if len(sample_str) > 200:
                        sample_str = sample_str[:200] + "..."
                    print(f"    [{i}] {sample_str}")
            except Exception as e:
                print(f"  ⚠️ Cannot analyze (complex data type): {e}")
                print(f"  Sample values (first 3 non-null):")
                samples = df[col].dropna().head(3)
                for i, sample in enumerate(samples, 1):
                    sample_str = str(sample)[:200]
                    print(f"    [{i}] {sample_str}")
        
        else:  # Numerical column
            try:
                print(f"  Statistics:")
                print(f"    Min:    {df[col].min()}")
                print(f"    Max:    {df[col].max()}")
                print(f"    Mean:   {df[col].mean():.2f}")
                print(f"    Median: {df[col].median():.2f}")
                print(f"    Std:    {df[col].std():.2f}")
                
                print(f"\n  Sample values:")
                samples = df[col].dropna().head(5).tolist()
                for i, sample in enumerate(samples, 1):
                    print(f"    [{i}] {sample}")
            except Exception as e:
                print(f"  ⚠️ Cannot compute statistics: {e}")

def main():
    # Define paths
    base_path = Path("../../../seoul-medical-facilities")
    
    files = {
        "RAG Knowledge": "seoul_medical_rag_knowledge.parquet",
        "Facilities Grouped": "seoul_medical_facilities_grouped.parquet",
        "Reviews Merged": "seoul_medical_reviews_merged.parquet"
    }
    
    print_section("SEOUL MEDICAL FACILITIES DATA EXPLORATION")
    print(f"Base directory: {base_path.absolute()}\n")
    
    # Load and explore each file
    dataframes = {}
    
    for name, filename in files.items():
        filepath = base_path / filename
        
        if not filepath.exists():
            print(f"⚠️  File not found: {filepath}")
            continue
        
        print(f"\n📂 Loading {filename}...")
        df = pd.read_parquet(filepath)
        dataframes[name] = df
        
        explore_dataframe(df, name)
    
    # Show relationships between dataframes
    if len(dataframes) > 1:
        print_section("RELATIONSHIPS BETWEEN DATASETS")
        
        # Check for common columns
        all_columns = {name: set(df.columns) for name, df in dataframes.items()}
        
        for i, (name1, cols1) in enumerate(all_columns.items()):
            for name2, cols2 in list(all_columns.items())[i+1:]:
                common = cols1 & cols2
                if common:
                    print(f"\n{name1} ↔ {name2}")
                    print(f"  Common columns: {', '.join(sorted(common))}")
        
        # Check if there are ID-like columns for joining
        print("\n\nPotential Join Keys:")
        print("-" * 80)
        for name, df in dataframes.items():
            id_cols = [col for col in df.columns if 'id' in col.lower() or 'code' in col.lower()]
            if id_cols:
                print(f"\n{name}:")
                for col in id_cols:
                    if not is_array_column(df[col]):
                        try:
                            print(f"  - {col}: {df[col].nunique():,} unique values")
                        except:
                            print(f"  - {col}: (complex type)")

if __name__ == "__main__":
    main()
