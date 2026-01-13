import pandas as pd
import os

# Define file paths
INPUT_FILE = "../../seoul-medical-facilities/seoul_medical_facilities_enriched.parquet"
OUTPUT_FILE = "../../seoul-medical-facilities/seoul_medical_facilities_grouped.parquet"

# --- MAPPING CONFIGURATION ---
# Comprehensive mapping of all 56 categories provided
CATEGORY_MAPPING = {
    # Group A: Medical Specialties (Specific Departments)
    'A': [
        '가정의학과', '내과', '대장,항문과', '마취통증의학과', '비뇨의학과', '산부인과', 
        '성형외과', '소아청소년과', '신경과', '신경외과', '안과', '영상의학과', 
        '외과', '이비인후과', '재활의학과', '정신건강의학과', '정형외과', '치과', 
        '피부과', '흉부외과'
    ],
    
    # Group B: Medical Facilities (Hospitals, Clinics, Institutions)
    'B': [
        '국립병원', '노인전문병원', '병원,의원', '병원부속시설', '보건소', '보건지소', 
        '보훈병원', '시립,도립병원', '여성전문병원', '요양병원', '응급실', 
        '종합병원', '한방병원', '한의원'
    ],
    
    # Group C: Specialized Care, Therapy & Support
    'C': [
        '건강검진', '건강관리', '모유수유', '아동,청소년상담', '언어치료', 
        '조산원', '치료,재활'
    ],
    
    # Group D: Beauty, Grooming & Wellness (Non-Medical)
    'D': [
        '머리염색', '미용', '미용기기,재료', '미용실', '피부,체형관리', '헬스장'
    ],
    
    # Group E: Non-Healthcare / General Services
    'E': [
        'N/A', '건물,빌딩', '교습학원,교습소', '백숙,삼계탕', '보험', '세탁소', 
        '자동차정비,수리', '장례식장', '종합대행업체'
    ]
}

GROUP_NAMES = {
    'A': 'Medical Specialty',
    'B': 'Medical Facility',
    'C': 'Therapy & Support',
    'D': 'Beauty & Wellness',
    'E': 'Non-Healthcare'
}

# Groups that count as "Healthcare" for your RAG pipeline
HEALTHCARE_GROUPS = ['A', 'B', 'C']

def get_group_info(category):
    """
    Returns (group_code, group_name, is_healthcare) for a category.
    Returns None if category is not found in mapping (for validation).
    """
    # Handle NaN or None values safely
    if pd.isna(category):
        category = 'N/A'
        
    for group_code, categories in CATEGORY_MAPPING.items():
        if category in categories:
            is_healthcare = group_code in HEALTHCARE_GROUPS
            return group_code, GROUP_NAMES[group_code], is_healthcare
            
    return None # Return None to flag unmapped items

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: File not found at {INPUT_FILE}")
        return

    print("Loading parquet file...")
    df = pd.read_parquet(INPUT_FILE)
    
    print(f"Total rows loaded: {len(df)}")

    # 1. Validation Step: Check for unmapped categories
    # Flatten the mapping list to check coverage
    all_mapped_cats = set([item for sublist in CATEGORY_MAPPING.values() for item in sublist])
    current_cats = set(df['category'].dropna().unique())
    
    # Find any categories in the file that are NOT in our mapping
    unmapped = current_cats - all_mapped_cats
    
    if unmapped:
        print(f"⚠️  WARNING: Found {len(unmapped)} categories not in the mapping list!")
        print(f"These will be defaulted to Group E (Non-Healthcare): {unmapped}")
    else:
        print("✅  Validation Passed: All categories in file are correctly mapped.")

    # 2. Apply Mapping
    print("Injecting group data...")
    
    # We use a wrapper function to handle the None (unmapped) case defaults
    def apply_mapping_safe(cat):
        result = get_group_info(cat)
        if result is None:
            # Default fallback for unknown items
            return 'E', 'Non-Healthcare', False
        return result

    group_data = df['category'].apply(apply_mapping_safe)
    
    # 3. Create new columns
    df['group_code'] = group_data.apply(lambda x: x[0])
    df['group_name'] = group_data.apply(lambda x: x[1])
    df['is_healthcare'] = group_data.apply(lambda x: x[2])
    
    # 4. Save
    print(f"Saving enriched data to {OUTPUT_FILE}...")
    df.to_parquet(OUTPUT_FILE)
    print("Done.")

if __name__ == "__main__":
    main()
