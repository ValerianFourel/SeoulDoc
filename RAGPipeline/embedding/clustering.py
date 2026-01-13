"""
Seoul Medical Reviews - Part 1-4: Clustering, Labeling & Prompt Preparation
===========================================================================
Enhanced with percentile-based cluster labeling
WITH PROXIMITY ORDERING: Reviews ranked by distance to cluster centroid
Global cluster ranking + facility-specific cluster ranking
INCLUDING CLOSEST REVIEW TO CENTROID
"""

import cupy as cp
import cuml
import numpy as np
import json
import torch
import gc
import pandas as pd
from transformers import AutoTokenizer, AutoModelForCausalLM
from tqdm import tqdm
import os
import pickle

# Set CuPy cache to scratch
os.environ['CUPY_CACHE_DIR'] = '/p/scratch/obdifflearn/fourel/.cupy_cache'
os.makedirs(os.environ['CUPY_CACHE_DIR'], exist_ok=True)
os.environ['PYTORCH_CUDA_ALLOC_CONF'] = 'expandable_segments:True'

# ==========================================
# CONFIGURATION
# ==========================================
INPUT_EMBEDDINGS = "../../../seoul-medical-facilities/seoul_medical_reviews_embeddings.parquet"
INPUT_METADATA = "../../../seoul-medical-facilities/seoul_medical_reviews_with_embeddings_metadata.parquet"

# Output files for intermediate data
OUTPUT_CLUSTERS = "../../../seoul-medical-facilities/seoul_medical_cluster_labels.parquet"
OUTPUT_VOCABULARY = "../../../seoul-medical-facilities/seoul_medical_cluster_vocabulary.pkl"
OUTPUT_PROMPTS = "../../../seoul-medical-facilities/seoul_medical_facility_prompts.pkl"
OUTPUT_METADATA = "../../../seoul-medical-facilities/seoul_medical_facility_metadata.pkl"

LABEL_LLM_ID = "Qwen/Qwen2.5-14B-Instruct"

N_GLOBAL_CLUSTERS = 1500  # Increased from 1250
MIN_REVIEWS_FACILITY = 10
BATCH_SIZE_LABEL = 12

PRIMARY_GPU = 0

# Clustering method: 'kmeans' or 'gmm'
CLUSTERING_METHOD = 'kmeans'

# Percentiles for representative review selection (after the closest one)
REPRESENTATIVE_PERCENTILES = [10, 20, 30, 50, 90]  # 5 reviews at different distances
# Total: 6 reviews (1 closest + 5 at percentiles)

# ==========================================
# TRUNCATION FLAGS (MODIFIED)
# ==========================================
# Set to True to cut reviews to save context, False to keep full text
TRUNCATE_REVIEWS = False 

# Length to cut at if TRUNCATE_REVIEWS is True
TRUNCATION_LENGTH = 120

# ==========================================
# ADAPTIVE CALCULATORS
# ==========================================
def calculate_num_summaries(n_reviews):
    """Adaptive summary count based on review volume"""
    if n_reviews < 10:
        return 0
    elif n_reviews < 30:
        return 3
    elif n_reviews < 75:
        return 5
    elif n_reviews < 150:
        return 6
    elif n_reviews < 300:
        return 7
    elif n_reviews < 500:
        return 8
    elif n_reviews < 1000:
        return 9
    else:
        return 10

def calculate_min_relevance_threshold(n_reviews):
    """Calculate minimum relevance threshold"""
    if n_reviews < 50:
        return 0.10
    elif n_reviews < 100:
        return 0.08
    elif n_reviews < 200:
        return 0.05
    elif n_reviews < 500:
        return 0.03
    elif n_reviews < 1000:
        return 0.02
    else:
        return 0.01

def calculate_max_highlights(n_reviews):
    """Calculate maximum highlights"""
    if n_reviews < 50:
        return 10
    elif n_reviews < 100:
        return 15
    elif n_reviews < 200:
        return 20
    elif n_reviews < 500:
        return 25
    elif n_reviews < 1000:
        return 30
    else:
        return 40

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def clear_gpu_memory(gpu_ids=None):
    """Clear GPU memory"""
    gc.collect()
    if gpu_ids:
        for gpu_id in gpu_ids:
            with torch.cuda.device(gpu_id):
                torch.cuda.empty_cache()
                torch.cuda.synchronize()
    else:
        for i in range(torch.cuda.device_count()):
            with torch.cuda.device(i):
                torch.cuda.empty_cache()
                torch.cuda.synchronize()

def setup_llm_single_gpu(model_id, gpu_id):
    """Setup LLM on a single GPU"""
    try:
        print(f"Loading {model_id} on GPU {gpu_id} (FP16)...")
        
        tokenizer = AutoTokenizer.from_pretrained(
            model_id, 
            trust_remote_code=True,
            padding_side='left'
        )
        
        if tokenizer.pad_token is None:
            tokenizer.pad_token = tokenizer.eos_token
        
        model = AutoModelForCausalLM.from_pretrained(
            model_id,
            device_map={"": gpu_id},
            trust_remote_code=True,
            torch_dtype=torch.float16,
            low_cpu_mem_usage=True
        )
        
        model.eval()
        allocated = torch.cuda.memory_allocated(gpu_id) / 1e9
        print(f"   ✓ Model loaded: {allocated:.2f} GB on GPU {gpu_id}")
        
        return model, tokenizer
        
    except Exception as e:
        print(f"❌ Model loading failed: {e}")
        return None, None

def generate_batch(model, tokenizer, prompts, max_new_tokens=100, temperature=0.3):
    """Generate text for batch"""
    inputs = tokenizer(
        prompts, 
        return_tensors="pt", 
        padding=True, 
        truncation=True,
        max_length=2560
    )
    
    device = next(model.parameters()).device
    inputs = {k: v.to(device) for k, v in inputs.items()}
    
    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            temperature=temperature,
            do_sample=True,
            top_p=0.95,
            top_k=50,
            pad_token_id=tokenizer.pad_token_id,
            eos_token_id=tokenizer.eos_token_id,
            use_cache=True
        )
    
    generated_texts = []
    for i, output in enumerate(outputs):
        input_length = inputs['input_ids'][i].shape[0]
        generated = output[input_length:]
        text = tokenizer.decode(generated, skip_special_tokens=True)
        generated_texts.append(text)
    
    return generated_texts

def extract_json(text):
    """Extract JSON from text"""
    try:
        if "{" in text and "}" in text:
            json_str = text[text.find('{'):text.rfind('}')+1]
            return json.loads(json_str)
        return None
    except:
        return None

def main():
    print("="*70)
    print("Seoul Medical Reviews - Part 1-4: Preparation")
    print(f"Clusters: {N_GLOBAL_CLUSTERS}")
    print(f"Method: {CLUSTERING_METHOD.upper()}")
    print(f"Representatives: CLOSEST + Percentiles {REPRESENTATIVE_PERCENTILES}")
    print(f"TRUNCATE REVIEWS: {TRUNCATE_REVIEWS} (Len: {TRUNCATION_LENGTH if TRUNCATE_REVIEWS else 'FULL'})")
    print("="*70)
    
    # ---------------------------------------------------------
    # STEP 1: LOAD EMBEDDINGS
    # ---------------------------------------------------------
    print(f"\n[1/4] Loading embeddings...")
    
    df_embeddings = pd.read_parquet(INPUT_EMBEDDINGS)
    df_metadata = pd.read_parquet(INPUT_METADATA)
    
    df_full = df_metadata.merge(df_embeddings[['review_id', 'embedding']], on='review_id')
    df_host = df_full[df_full['script_type'].isin(['Hangul', 'Mixed'])].copy()
    
    print(f"   Filtered: {len(df_host):,} reviews")
    
    reviews_per_facility = df_host.groupby('place_id').size()
    print(f"   Facilities: {len(reviews_per_facility):,}")
    print(f"   Median: {reviews_per_facility.median():.0f} | Mean: {reviews_per_facility.mean():.1f}")
    
    # Load to GPU
    print(f"\n   Loading to GPU {PRIMARY_GPU}...")
    with cp.cuda.Device(PRIMARY_GPU):
        embeddings_list = df_host['embedding'].tolist()
        embeddings_array = np.array(embeddings_list, dtype=np.float32)
        embeddings_gpu = cp.asarray(embeddings_array)
        print(f"   ✓ Memory: {embeddings_array.nbytes / 1e9:.2f} GB")
    
    texts = df_host['review_text'].tolist()
    del embeddings_array, embeddings_list

    # ---------------------------------------------------------
    # STEP 2: CLUSTERING
    # ---------------------------------------------------------
    print(f"\n[2/4] Clustering on GPU {PRIMARY_GPU} ({CLUSTERING_METHOD.upper()})")
    
    with cp.cuda.Device(PRIMARY_GPU):
        if CLUSTERING_METHOD == 'gmm':
            print(f"   Using GMM (Gaussian Mixture Models)...")
            from cuml.mixture import GaussianMixture
            
            gmm = GaussianMixture(
                n_components=N_GLOBAL_CLUSTERS,
                covariance_type='diag',
                max_iter=300,
                random_state=42,
                verbose=0
            )
            gmm.fit(embeddings_gpu)
            
            global_labels = gmm.predict(embeddings_gpu)
            centroids = gmm.means_
            centroids_cpu = centroids.get()
            
        else:  # kmeans (default)
            print(f"   Using KMeans...")
            kmeans = cuml.KMeans(
                n_clusters=N_GLOBAL_CLUSTERS, 
                n_init=10,
                max_iter=900,
                random_state=42,
                verbose=0
            )
            kmeans.fit(embeddings_gpu)
            
            global_labels = kmeans.labels_
            centroids = kmeans.cluster_centers_
            centroids_cpu = centroids.get()
    
    df_host['cluster_id'] = global_labels.get()
    
    cluster_sizes = df_host['cluster_id'].value_counts()
    print(f"   ✓ Range: {cluster_sizes.min()}-{cluster_sizes.max()} | Median: {cluster_sizes.median():.0f}")
    
    # ---------------------------------------------------------
    # COMPUTE PROXIMITY TO CENTROID
    # ---------------------------------------------------------
    print(f"\n   Computing proximity to cluster centroids...")
    
    with cp.cuda.Device(PRIMARY_GPU):
        # Calculate cosine similarity to assigned centroid for each review
        centroid_similarities = cp.zeros(len(df_host), dtype=cp.float32)
        
        for cluster_id in tqdm(range(N_GLOBAL_CLUSTERS), desc="Similarities"):
            cluster_mask = global_labels == cluster_id
            cluster_indices = cp.where(cluster_mask)[0]
            
            if len(cluster_indices) == 0:
                continue
            
            # Get centroid for this cluster
            centroid = cp.asarray(centroids_cpu[cluster_id])
            
            # Get embeddings for reviews in this cluster
            cluster_embeddings = embeddings_gpu[cluster_indices]
            
            # Calculate cosine similarity
            centroid_norm = centroid / cp.linalg.norm(centroid)
            embeddings_norm = cluster_embeddings / cp.linalg.norm(cluster_embeddings, axis=1, keepdims=True)
            
            similarities = cp.dot(embeddings_norm, centroid_norm)
            centroid_similarities[cluster_indices] = similarities
        
        # Convert to numpy
        similarity_scores = centroid_similarities.get()
    
    df_host['centroid_similarity'] = similarity_scores
    
    print(f"   ✓ Similarity range: [{similarity_scores.min():.4f}, {similarity_scores.max():.4f}]")
    print(f"   ✓ Mean similarity: {similarity_scores.mean():.4f}")
    
    # ---------------------------------------------------------
    # GLOBAL CLUSTER RANKING
    # ---------------------------------------------------------
    print(f"\n   Ranking reviews globally within each cluster...")
    
    # Global rank: rank within cluster_id only (across all facilities)
    df_host['global_cluster_rank'] = df_host.groupby('cluster_id')['centroid_similarity'].rank(
        method='first', 
        ascending=False
    ).astype(int)
    
    # Global cluster size
    df_host['global_cluster_size'] = df_host.groupby('cluster_id')['cluster_id'].transform('count')
    
    print(f"   ✓ Average reviews per cluster: {df_host['global_cluster_size'].mean():.1f}")
    print(f"   ✓ Max reviews in one cluster: {df_host['global_cluster_size'].max()}")
    
    # ---------------------------------------------------------
    # FACILITY-SPECIFIC CLUSTER RANKING
    # ---------------------------------------------------------
    print(f"\n   Ranking reviews within (place_id, cluster_id) groups...")
    
    # Facility-specific rank
    df_host['facility_cluster_rank'] = df_host.groupby(['place_id', 'cluster_id'])['centroid_similarity'].rank(
        method='first', 
        ascending=False
    ).astype(int)
    
    # Cluster size within each facility
    df_host['facility_cluster_size'] = df_host.groupby(['place_id', 'cluster_id'])['cluster_id'].transform('count')
    
    print(f"   ✓ Average reviews per (place_id, cluster_id): {df_host['facility_cluster_size'].mean():.1f}")
    print(f"   ✓ Max reviews in one (place_id, cluster_id): {df_host['facility_cluster_size'].max()}")
    
    # Save clusters with proximity information
    cluster_df = df_host[[
        'review_id', 
        'place_id', 
        'cluster_id',
        'centroid_similarity',
        'global_cluster_rank',
        'global_cluster_size',
        'facility_cluster_rank',
        'facility_cluster_size'
    ]].copy()
    
    cluster_df.to_parquet(OUTPUT_CLUSTERS, index=False)
    print(f"   ✓ Saved to {OUTPUT_CLUSTERS}")
    
    # Show sample
    print(f"\n   📊 Sample Rankings (first cluster):")
    sample = cluster_df[cluster_df['cluster_id'] == 0].nsmallest(10, 'global_cluster_rank')
    print(sample[['cluster_id', 'global_cluster_rank', 'facility_cluster_rank', 
                  'centroid_similarity', 'global_cluster_size']].to_string())

    # ---------------------------------------------------------
    # STEP 3: PERCENTILE-BASED LABELING (WITH CLOSEST)
    # ---------------------------------------------------------
    print(f"\n[3/4] Percentile-based cluster labeling...")
    print(f"   Using CLOSEST review + reviews at percentiles: {REPRESENTATIVE_PERCENTILES}")
    
    cluster_representatives = {}
    cluster_percentile_info = {}
    
    # Create a mapping from global index to review text and cluster
    print(f"\n   Extracting representatives (closest + percentiles)...")
    
    for cluster_id in tqdm(range(N_GLOBAL_CLUSTERS), desc="Representatives"):
        # Get all reviews in this cluster
        cluster_mask = df_host['cluster_id'] == cluster_id
        cluster_data = df_host[cluster_mask].copy()
        
        if len(cluster_data) == 0:
            cluster_representatives[cluster_id] = ["No reviews in cluster"]
            cluster_percentile_info[cluster_id] = {}
            continue
        
        # Sort by similarity (descending = closer to centroid)
        cluster_data = cluster_data.sort_values('centroid_similarity', ascending=False)
        
        # Get reviews: CLOSEST first, then percentiles
        representatives = []
        percentile_details = {}
        
        # 1. CLOSEST REVIEW TO CENTROID (Rank 1)
        closest_review = cluster_data.iloc[0]
        representatives.append(closest_review['review_text'])
        percentile_details['CLOSEST'] = {
            'position': 1,
            'total': len(cluster_data),
            'similarity': closest_review['centroid_similarity']
        }
        
        # 2. REVIEWS AT PERCENTILES
        for percentile in REPRESENTATIVE_PERCENTILES:
            # Convert percentile to index (percentile from the top)
            position = int((percentile / 100.0) * len(cluster_data))
            position = min(position, len(cluster_data) - 1)
            
            # Skip if it's the same as closest (for very small clusters)
            if position == 0:
                continue
            
            review_row = cluster_data.iloc[position]
            review_text = review_row['review_text']
            similarity = review_row['centroid_similarity']
            
            representatives.append(review_text)
            percentile_details[percentile] = {
                'position': position + 1,
                'total': len(cluster_data),
                'similarity': similarity
            }
        
        cluster_representatives[cluster_id] = representatives
        cluster_percentile_info[cluster_id] = percentile_details
    
    # Free GPU 0
    print(f"\n   Freeing GPU {PRIMARY_GPU}...")
    del embeddings_gpu, centroids, centroids_cpu, centroid_similarities, similarity_scores
    if CLUSTERING_METHOD == 'kmeans':
        del kmeans
    else:
        del gmm
    del global_labels
    
    with cp.cuda.Device(PRIMARY_GPU):
        cp.get_default_memory_pool().free_all_blocks()
    
    clear_gpu_memory([PRIMARY_GPU])
    
    # Load labeling model
    print(f"\n   Loading Labeling LLM on GPU 1...")
    label_model, label_tokenizer = setup_llm_single_gpu(LABEL_LLM_ID, gpu_id=1)
    
    if label_model is None:
        print("❌ Failed to load labeling model!")
        return
    
    # PERCENTILE-BASED LABEL PROMPTS (WITH CLOSEST)
    prompts = []
    keys = sorted(cluster_representatives.keys())
    
    for k in keys:
        reps = cluster_representatives[k]
        percentile_info = cluster_percentile_info[k]
        
        # Format reviews with position information
        reps_formatted = []
        
        # First one is always CLOSEST
        if 'CLOSEST' in percentile_info:
            info = percentile_info['CLOSEST']
            reps_formatted.append(
                f"[CLOSEST - Rank 1/{info['total']} - Sim: {info['similarity']:.3f}]\n{reps[0]}"
            )
            
            # Then add percentile-based reviews
            for i, percentile in enumerate(REPRESENTATIVE_PERCENTILES, start=1):
                if i < len(reps) and percentile in percentile_info:
                    info = percentile_info[percentile]
                    reps_formatted.append(
                        f"[P{percentile} - Rank {info['position']}/{info['total']} - Sim: {info['similarity']:.3f}]\n{reps[i]}"
                    )
        else:
            # Fallback if no reviews
            reps_formatted = [str(r) for r in reps]
        
        reps_text = "\n\n".join(reps_formatted)
        
        prompt = f"""You are analyzing Korean medical facility reviews to identify the SPECIFIC topic of this cluster.

REPRESENTATIVE REVIEWS FROM CLUSTER:
{reps_text}

EXPLANATION OF REVIEW SELECTION:
- CLOSEST: The most representative review, closest to the cluster centroid
- P10-P30: Still close to centroid, representative of core cluster
- P50: Middle of the cluster  
- P90: Further from centroid (edge cases, still relevant)

TASK:
Based on these {len(reps)} reviews spanning from closest to centroid to edge cases, identify the SPECIFIC medical topic or aspect being discussed.

REQUIREMENTS:
1. The label MUST be DESCRIPTIVE and SPECIFIC (3-8 words)
2. Be CONCRETE, not vague or generic
3. Capture the CORE medical aspect that unifies all reviews
4. Focus primarily on CLOSEST and P10-P30 reviews as they are most representative

GOOD EXAMPLES:
- "Friendly pediatric staff with detailed explanations"
- "Long waiting times during peak hours"
- "Modern clean facilities and equipment"
- "Skilled orthopedic surgeons with experience"
- "Affordable prices compared to other clinics"
- "Convenient parking and accessibility"
- "Professional dermatology consultation service"

BAD EXAMPLES (too vague):
- "General Service" ❌
- "Good" ❌
- "Medical care" ❌
- "Treatment" ❌

OUTPUT ONLY THIS JSON FORMAT:
{{"en": "Specific descriptive label (3-8 words)", "ko": "구체적인 설명 라벨 (3-8단어)"}}

EXAMPLE OUTPUT:
{{"en": "Experienced pediatric doctors with gentle care", "ko": "부드러운 진료를 제공하는 경험 많은 소아과 의사"}}"""
        
        prompts.append(prompt)
    
    cluster_vocabulary = {}
    
    print(f"   Labeling {len(prompts)} clusters with closest + percentile-based prompts...")
    for i in tqdm(range(0, len(prompts), BATCH_SIZE_LABEL), desc="Labeling"):
        batch_prompts = prompts[i:i+BATCH_SIZE_LABEL]
        batch_keys = keys[i:i+BATCH_SIZE_LABEL]
        
        outputs = generate_batch(
            label_model, 
            label_tokenizer, 
            batch_prompts, 
            max_new_tokens=120,
            temperature=0.3
        )
        
        for k, output_text in zip(batch_keys, outputs):
            data = extract_json(output_text)
            if data and 'en' in data and 'ko' in data:
                # Validate minimum length
                en_words = len(data['en'].split())
                if en_words >= 3:
                    cluster_vocabulary[k] = data
                else:
                    # Fallback if too short
                    cluster_vocabulary[k] = {
                        "en": f"{data['en']} medical service",
                        "ko": f"{data['ko']} 의료 서비스"
                    }
            else:
                # Better fallback based on cluster ID
                cluster_vocabulary[k] = {
                    "en": f"Medical service cluster {k}",
                    "ko": f"의료 서비스 클러스터 {k}"
                }
    
    print(f"   ✓ Labeled {len(cluster_vocabulary)} clusters")
    
    # Show sample labels with info
    print(f"\n   📋 Sample Labels (first 10 clusters):")
    for i, (cid, label) in enumerate(list(cluster_vocabulary.items())[:10], 1):
        cluster_size = df_host[df_host['cluster_id'] == cid]['global_cluster_size'].iloc[0]
        closest_sim = cluster_percentile_info[cid].get('CLOSEST', {}).get('similarity', 0)
        print(f"   {i}. [{cid}] (n={cluster_size}, closest_sim={closest_sim:.3f})")
        print(f"       EN: {label['en']}")
        print(f"       KO: {label['ko']}")
    
    # Save vocabulary with percentile info
    vocabulary_with_info = {
        'labels': cluster_vocabulary,
        'percentile_info': cluster_percentile_info,
        'percentiles_used': ['CLOSEST'] + REPRESENTATIVE_PERCENTILES
    }
    
    with open(OUTPUT_VOCABULARY, 'wb') as f:
        pickle.dump(vocabulary_with_info, f)
    print(f"\n   ✓ Saved vocabulary to {OUTPUT_VOCABULARY}")
    
    del label_model, label_tokenizer
    clear_gpu_memory([1])

    # ---------------------------------------------------------
    # STEP 4: PREPARE FACILITY PROMPTS
    # ---------------------------------------------------------
    print("\n[4/4] Preparing facility prompts...")
    grouped = df_host.groupby('place_id')
    
    facility_prompts = []
    facility_metadata = []
    skipped = 0
    
    highlight_stats = {
        'total_facilities': 0,
        'total_highlights': 0,
        'min_highlights': float('inf'),
        'max_highlights': 0
    }
    
    for place_id, group in tqdm(grouped, desc="Facilities"):
        n_reviews = len(group)
        n_summaries = calculate_num_summaries(n_reviews)
        
        if n_summaries == 0:
            skipped += 1
            continue
        
        min_relevance = calculate_min_relevance_threshold(n_reviews)
        max_highlights = calculate_max_highlights(n_reviews)
        
        counts = group['cluster_id'].value_counts()
        total = len(group)
        
        highlights = []
        for cid, count in counts.items():
            relevance = count / total
            
            if relevance >= min_relevance or len(highlights) < n_summaries:
                labels = cluster_vocabulary.get(cid, {"en": "General medical service", "ko": "일반 의료 서비스"})
                highlights.append({
                    'cid': cid,
                    'en': labels['en'],
                    'ko': labels['ko'],
                    'relevance': relevance,
                    'count': count
                })
            
            if len(highlights) >= max_highlights:
                break
        
        highlights.sort(key=lambda x: x['relevance'], reverse=True)
        
        highlight_stats['total_facilities'] += 1
        highlight_stats['total_highlights'] += len(highlights)
        highlight_stats['min_highlights'] = min(highlight_stats['min_highlights'], len(highlights))
        highlight_stats['max_highlights'] = max(highlight_stats['max_highlights'], len(highlights))
        
        highlights_text = []
        for h in highlights:
            highlights_text.append(
                f"- {h['en']} / {h['ko']} ({h['relevance']:.1%}) [{h['count']} reviews]"
            )
        
        # Use facility_cluster_rank to get most representative reviews
        samples = []
        for h in highlights[:n_summaries + 5]:
            # Get top 2 most representative reviews (lowest facility_cluster_rank) for this cluster
            cluster_reviews = group[group['cluster_id'] == h['cid']].nsmallest(2, 'facility_cluster_rank')
            sample_texts = cluster_reviews['review_text'].tolist()
            
            # --- MODIFIED TRUNCATION LOGIC ---
            for txt in sample_texts:
                if TRUNCATE_REVIEWS:
                    # Original behavior: Cut and add "..."
                    review_content = f"{txt[:TRUNCATION_LENGTH]}..."
                else:
                    # New behavior: Use full text
                    review_content = txt
                    
                samples.append(f"({h['en']}): {review_content}")
            # ---------------------------------
        
        prompt = f"""MEDICAL FACILITY COMPREHENSIVE ANALYSIS

FACILITY: {place_id}
TOTAL REVIEWS: {n_reviews}
MINIMUM RELEVANCE THRESHOLD: {min_relevance:.1%}

ALL IDENTIFIED TOPICS (English / Korean / Relevance / Count):
{chr(10).join(highlights_text)}

REPRESENTATIVE REVIEW SAMPLES (most similar to cluster centroids):
{chr(10).join(samples[:20])}

TASK:
1. Review ALL {len(highlights)} topics listed above
2. Generate {n_summaries} comprehensive summary sentences focusing on the MOST IMPORTANT aspects
3. Include ALL {len(highlights)} topics in Key_Highlights with their exact relevance scores

Each summary should:
- Address distinct aspects of patient experience
- Be detailed and specific (20-40 words)
- Reflect actual patient feedback

OUTPUT FORMAT (JSON only):
{{
    "Facility": "{place_id}",
    "Total_Reviews": {n_reviews},
    "Key_Highlights": [
        {{"topic_en": "{highlights[0]['en']}", "topic_ko": "{highlights[0]['ko']}", "relevance": {highlights[0]['relevance']:.3f}}},
        ... (include ALL {len(highlights)} topics with exact relevance values)
    ],
    "Summaries": [
        "Detailed English summary 1...",
        ... ({n_summaries} summaries total)
    ],
    "Summaries_Korean": [
        "상세한 한국어 요약 1...",
        ... ({n_summaries} summaries total)
    ]
}}"""
        
        facility_prompts.append(prompt)
        facility_metadata.append({
            'place_id': place_id,
            'n_reviews': n_reviews,
            'n_summaries': n_summaries,
            'n_highlights': len(highlights),
            'min_relevance': min_relevance
        })
    
    print(f"   ✓ Prepared {len(facility_prompts):,} prompts (skipped {skipped})")
    
    # Save prompts and metadata
    with open(OUTPUT_PROMPTS, 'wb') as f:
        pickle.dump(facility_prompts, f)
    
    with open(OUTPUT_METADATA, 'wb') as f:
        pickle.dump(facility_metadata, f)
    
    print(f"   ✓ Saved prompts to {OUTPUT_PROMPTS}")
    print(f"   ✓ Saved metadata to {OUTPUT_METADATA}")
    
    # Statistics
    print(f"\n📊 Highlights Statistics:")
    print(f"   Average: {highlight_stats['total_highlights'] / highlight_stats['total_facilities']:.1f}")
    print(f"   Range: {highlight_stats['min_highlights']} - {highlight_stats['max_highlights']}")
    
    print(f"\n" + "="*70)
    print(f"✅ Part 1-4 Complete!")
    print(f"="*70)
    print(f"   Clustering Method: {CLUSTERING_METHOD.upper()}")
    print(f"   Number of Clusters: {N_GLOBAL_CLUSTERS}")
    print(f"   Representatives: CLOSEST + Percentiles {REPRESENTATIVE_PERCENTILES}")
    print(f"   Total representatives per cluster: {1 + len(REPRESENTATIVE_PERCENTILES)}")
    print(f"   Global + Facility-specific rankings")
    print(f"   Ready for Part 5 (Summary Generation)")
    print(f"   Run clustering_part5.py to continue")
    print(f"="*70)

if __name__ == "__main__":
    main()
