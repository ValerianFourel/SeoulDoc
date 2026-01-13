"""
Inspect Seoul Medical Facilities Parquet Files
===============================================
Simple, robust inspection showing column information.
"""

import pandas as pd
from pathlib import Path
import sys

def inspect_parquet(filepath: str):
    """
    Inspect a parquet file and show column information.
    
    Parameters
    ----------
    filepath : str
        Path to the parquet file
    """
    print("\n" + "="*80)
    print(f"FILE: {filepath}")
    print("="*80)
    
    try:
        # Load the file
        df = pd.read_parquet(filepath)
        
        # Basic info
        print(f"\n📊 BASIC INFO")
        print("-" * 80)
        print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # Column info - safe version
        print(f"\n📋 COLUMNS ({len(df.columns)} total)")
        print("-" * 80)
        
        col_data = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            non_null = df[col].count()
            null_pct = (df[col].isna().sum() / len(df)) * 100
            
            # Safely get unique count
            unique = '-'
            if df[col].dtype == 'object':
                try:
                    # Check if contains dict/list
                    sample = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                    if isinstance(sample, (dict, list)):
                        unique = 'dict/list'
                    else:
                        unique = str(df[col].nunique())
                except:
                    unique = 'error'
            elif pd.api.types.is_numeric_dtype(df[col]):
                unique = '-'
            
            col_data.append({
                'Column': col,
                'Type': dtype,
                'Non-Null': f"{non_null:,}",
                'Null %': f"{null_pct:.2f}%",
                'Unique': unique
            })
        
        # Print as formatted table
        df_info = pd.DataFrame(col_data)
        print(df_info.to_string(index=False))
        
        return df
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    """Main execution."""
    # Define file paths
    base_dir = Path("../../seoul-medical-facilities")
    
    files = [
        "seoul_medical_facilities.parquet",
        "seoul_medical_facilities_enriched.parquet",
        "seoul_medical_reviews_merged.parquet"
    ]
    
    print("="*80)
    print("SEOUL MEDICAL FACILITIES - PARQUET FILE INSPECTOR")
    print("="*80)
    print(f"\nBase directory: {base_dir.resolve()}")
    
    # Check if base directory exists
    if not base_dir.exists():
        print(f"\n❌ ERROR: Directory not found: {base_dir}")
        sys.exit(1)
    
    # Inspect each file
    dfs = {}
    for filename in files:
        filepath = base_dir / filename
        if filepath.exists():
            df = inspect_parquet(str(filepath))
            dfs[filename] = df
        else:
            print(f"\n⚠️  File not found: {filepath}")
            dfs[filename] = None
    
    # Final summary
    print("\n" + "="*80)
    print("📊 SUMMARY")
    print("="*80)
    
    for name, df in dfs.items():
        if df is not None:
            print(f"\n{name}:")
            print(f"  Rows: {df.shape[0]:,}")
            print(f"  Columns: {df.shape[1]}")
            print(f"  Column names: {', '.join(df.columns.tolist())}")
    
    # Recommendation
    print("\n" + "="*80)
    print("💡 RECOMMENDATION FOR CLUSTERING")
    print("="*80)
    
    review_file = None
    max_rows = 0
    
    for name, df in dfs.items():
        if df is not None and df.shape[0] > max_rows:
            has_review_cols = any('review' in col.lower() or 'comment' in col.lower() 
                                 for col in df.columns)
            if has_review_cols:
                review_file = name
                max_rows = df.shape[0]
    
    if review_file:
        print(f"\n✅ Use file: {review_file}")
        print(f"   Reviews: {max_rows:,}")
        print(f"   Required columns: place_id, review_text")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    main()
