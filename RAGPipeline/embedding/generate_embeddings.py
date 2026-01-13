"""
Generate Embeddings for Seoul Medical Reviews
==============================================
Processes 1.7M healthcare reviews in chunks to avoid OOM errors.
Saves embeddings to parquet for reuse.
"""

import pandas as pd
import numpy as np
import torch
import gc
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import os
import re

# ==========================================
# CONFIGURATION
# ==========================================
FACILITIES_PATH = "../../../seoul-medical-facilities/seoul_medical_facilities_grouped.parquet"
REVIEWS_PATH = "../../../seoul-medical-facilities/seoul_medical_reviews_merged.parquet"
OUTPUT_EMBEDDINGS = "../../../seoul-medical-facilities/seoul_medical_reviews_embeddings.parquet"
OUTPUT_METADATA = "../../../seoul-medical-facilities/seoul_medical_reviews_with_embeddings_metadata.parquet"

# Embedding Model
EMBED_MODEL_ID = "dragonkue/BGE-m3-ko"

# Memory-safe parameters
CHUNK_SIZE = 50000  # Process 50k reviews at a time
BATCH_SIZE_EMBED = 1024  # Reduced from 2048

# Healthcare groups to include
HEALTHCARE_GROUPS = ['Medical Specialty', 'Medical Facility', 'Therapy & Support']

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def classify_script(text):
    """Classify text into Hangul/Roman/Mixed/Other"""
    if not isinstance(text, str) or not text.strip():
        return 'Other/Empty'
    
    has_hangul = bool(re.search(r'[가-힣ㄱ-ㅎㅏ-ㅣ]', text))
    has_roman = bool(re.search(r'[a-zA-Z]', text))
    
    if has_hangul and has_roman:
        return 'Mixed'
    elif has_hangul:
        return 'Hangul'
    elif has_roman:
        return 'Roman'
    else:
        return 'Other'

def clear_gpu_memory():
    """Aggressively clear GPU memory"""
    gc.collect()
    torch.cuda.empty_cache()
    torch.cuda.synchronize()

# ==========================================
# MAIN EMBEDDING GENERATION
# ==========================================
def main():
    print("="*60)
    print("Seoul Medical Reviews - Embedding Generation")
    print("="*60)
    
    # ---------------------------------------------------------
    # STEP 1: LOAD AND FILTER DATA
    # ---------------------------------------------------------
    print("\n[1/4] Loading data...")
    
    # Load facilities
    if not os.path.exists(FACILITIES_PATH):
        print(f"❌ Facilities file not found: {FACILITIES_PATH}")
        return
    
    df_facilities = pd.read_parquet(FACILITIES_PATH)
    df_facilities = df_facilities[['place_id', 'group_name', 'group_code']]
    
    # Load reviews
    if not os.path.exists(REVIEWS_PATH):
        print(f"❌ Reviews file not found: {REVIEWS_PATH}")
        return
    
    df_reviews = pd.read_parquet(REVIEWS_PATH)
    print(f"   Total reviews loaded: {len(df_reviews):,}")
    
    # Display available columns
    print(f"   Available columns: {df_reviews.columns.tolist()}")
    
    # Merge with facilities
    df_merged = df_reviews.merge(df_facilities, on='place_id', how='left')
    
    # Filter for healthcare only
    df_healthcare = df_merged[df_merged['group_name'].isin(HEALTHCARE_GROUPS)].copy()
    print(f"   Healthcare reviews: {len(df_healthcare):,}")
    
    # Classify script type
    print("   Classifying script types...")
    df_healthcare['script_type'] = df_healthcare['review_text'].apply(classify_script)
    
    # Filter valid reviews (non-empty, minimum length)
    df_healthcare = df_healthcare[
        (df_healthcare['review_text'].str.len() > 5) &
        (df_healthcare['script_type'] != 'Other/Empty')
    ].copy()
    
    print(f"   Valid reviews after filtering: {len(df_healthcare):,}")
    
    # Script type distribution
    print("\n   Script Type Distribution:")
    for script_type, count in df_healthcare['script_type'].value_counts().items():
        pct = count / len(df_healthcare) * 100
        print(f"     {script_type}: {count:,} ({pct:.1f}%)")
    
    # Reset index and create unique review_id
    print("\n   Creating unique review IDs...")
    df_healthcare = df_healthcare.reset_index(drop=True)
    df_healthcare['review_id'] = df_healthcare.index.astype(str)
    
    # Alternative: Create compound ID from place_id and review_index
    # df_healthcare['review_id'] = df_healthcare['place_id'] + '_' + df_healthcare['review_index'].fillna(0).astype(int).astype(str)
    
    print(f"   Created {len(df_healthcare):,} unique review IDs")
    
    # ---------------------------------------------------------
    # STEP 2: INITIALIZE EMBEDDING MODEL
    # ---------------------------------------------------------
    print(f"\n[2/4] Loading embedding model: {EMBED_MODEL_ID}")
    embedder = SentenceTransformer(EMBED_MODEL_ID, device="cuda")
    print(f"   Model loaded on GPU")
    print(f"   Embedding dimension: {embedder.get_sentence_embedding_dimension()}")
    
    # ---------------------------------------------------------
    # STEP 3: GENERATE EMBEDDINGS IN CHUNKS
    # ---------------------------------------------------------
    print(f"\n[3/4] Generating embeddings in chunks of {CHUNK_SIZE:,}")
    print(f"   Batch size: {BATCH_SIZE_EMBED}")
    
    total_reviews = len(df_healthcare)
    num_chunks = (total_reviews + CHUNK_SIZE - 1) // CHUNK_SIZE
    
    all_embeddings = []
    
    for chunk_idx in range(num_chunks):
        start_idx = chunk_idx * CHUNK_SIZE
        end_idx = min((chunk_idx + 1) * CHUNK_SIZE, total_reviews)
        
        print(f"\n   Chunk {chunk_idx + 1}/{num_chunks}: Reviews {start_idx:,} to {end_idx:,}")
        
        # Get chunk data
        chunk_texts = df_healthcare['review_text'].iloc[start_idx:end_idx].tolist()
        
        # Generate embeddings for this chunk
        try:
            chunk_embeddings = embedder.encode(
                chunk_texts,
                batch_size=BATCH_SIZE_EMBED,
                show_progress_bar=True,
                convert_to_numpy=True,
                normalize_embeddings=True
            )
            
            all_embeddings.append(chunk_embeddings)
            
            # Report memory usage
            if torch.cuda.is_available():
                allocated = torch.cuda.memory_allocated() / 1e9
                reserved = torch.cuda.memory_reserved() / 1e9
                print(f"   GPU Memory: {allocated:.2f} GB allocated, {reserved:.2f} GB reserved")
            
            # Clear memory between chunks
            clear_gpu_memory()
            
        except torch.cuda.OutOfMemoryError as e:
            print(f"\n❌ OOM Error in chunk {chunk_idx + 1}")
            print(f"   Try reducing CHUNK_SIZE or BATCH_SIZE_EMBED")
            print(f"   Current: CHUNK_SIZE={CHUNK_SIZE}, BATCH_SIZE_EMBED={BATCH_SIZE_EMBED}")
            raise e
    
    # Concatenate all embeddings
    print("\n   Concatenating all embeddings...")
    embeddings_array = np.vstack(all_embeddings)
    print(f"   Final embeddings shape: {embeddings_array.shape}")
    
    # Clean up model
    del embedder
    clear_gpu_memory()
    
    # ---------------------------------------------------------
    # STEP 4: SAVE RESULTS
    # ---------------------------------------------------------
    print(f"\n[4/4] Saving results...")
    
    # Convert embeddings to list of lists for parquet
    embeddings_list = embeddings_array.tolist()
    
    # Create embeddings DataFrame
    df_embeddings = pd.DataFrame({
        'review_id': df_healthcare['review_id'].values,
        'place_id': df_healthcare['place_id'].values,
        'embedding': embeddings_list
    })
    
    print(f"   Saving embeddings to: {OUTPUT_EMBEDDINGS}")
    df_embeddings.to_parquet(OUTPUT_EMBEDDINGS, index=False)
    
    # Save full metadata (without embeddings in text form for efficiency)
    # Include all useful columns from the original data
    metadata_columns = ['review_id', 'place_id', 'facility_name', 'review_text', 
                        'script_type', 'group_name']
    
    # Only include columns that exist
    available_metadata_cols = [col for col in metadata_columns if col in df_healthcare.columns]
    
    df_metadata = df_healthcare[available_metadata_cols].copy()
    
    print(f"   Saving metadata to: {OUTPUT_METADATA}")
    print(f"   Metadata columns: {available_metadata_cols}")
    df_metadata.to_parquet(OUTPUT_METADATA, index=False)
    
    # ---------------------------------------------------------
    # SUMMARY
    # ---------------------------------------------------------
    print("\n" + "="*60)
    print("✅ Embedding Generation Complete!")
    print("="*60)
    print(f"Total reviews processed: {len(df_healthcare):,}")
    print(f"Embedding dimension: {embeddings_array.shape[1]}")
    print(f"Embeddings file size: {os.path.getsize(OUTPUT_EMBEDDINGS) / 1e9:.2f} GB")
    print(f"Metadata file size: {os.path.getsize(OUTPUT_METADATA) / 1e9:.2f} GB")
    print("\nOutput files:")
    print(f"  1. {OUTPUT_EMBEDDINGS}")
    print(f"  2. {OUTPUT_METADATA}")
    print("\nColumns in output:")
    print(f"  Embeddings: {df_embeddings.columns.tolist()}")
    print(f"  Metadata: {df_metadata.columns.tolist()}")
    print("="*60)

if __name__ == "__main__":
    main()
