import pandas as pd
import os

INPUT_FILE = "../../seoul-medical-facilities/seoul_medical_facilities_grouped.parquet"

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: File not found. Run inject_groups.py first.")
        return

    print("Loading grouped parquet file...")
    df = pd.read_parquet(INPUT_FILE)
    
    if 'is_healthcare' in df.columns:
        healthcare_count = df['is_healthcare'].sum()
        total_count = len(df)
        non_health_count = total_count - healthcare_count
        
        print("\n" + "="*40)
        print("       HEALTHCARE DATA SUMMARY       ")
        print("="*40)
        print(f"Total Rows           : {total_count}")
        print(f"Healthcare (True)    : {healthcare_count}  ({(healthcare_count/total_count)*100:.1f}%)")
        print(f"Non-Healthcare (False): {non_health_count}  ({(non_health_count/total_count)*100:.1f}%)")
        print("-" * 40)
        
        print("\nBreakdown by Group:")
        # Group by code and name for clarity
        print(df.groupby(['group_code', 'group_name']).size())
        print("="*40 + "\n")
    else:
        print("Column 'is_healthcare' not found.")

if __name__ == "__main__":
    main()
