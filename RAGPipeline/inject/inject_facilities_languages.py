import pandas as pd
import os
import re
import torch
import json
import torch.multiprocessing as mp
from transformers import AutoModelForCausalLM, AutoTokenizer
from tqdm import tqdm
import math

# --- CONFIGURATION ---
FACILITIES_PATH = "../../../seoul-medical-facilities/seoul_medical_facilities_grouped.parquet"
REVIEWS_INPUT_PATH = "../../../seoul-medical-facilities/seoul_medical_reviews_merged.parquet"
OUTPUT_FACILITIES_PATH = "../../../seoul-medical-facilities/seoul_medical_facilities_enriched_multigpu.parquet"
CHECKPOINT_DIR = "../../../seoul-medical-facilities/checkpoints_multigpu"

# LLM Configuration
MODEL_ID = "Qwen/Qwen2.5-14B-Instruct"
BATCH_SIZE = 32         # A100 40GB can handle this easily
MAX_NEW_TOKENS = 10
SAVE_EVERY_N = 25       # Save per-worker checkpoint every 25 facilities

# Number of GPUs to use (Set to 4 for your setup)
NUM_GPUS = 4

def classify_script(text):
    """Classifies text into Hangul, Roman, Mixed, or Other."""
    if not isinstance(text, str) or not text.strip(): return 'Other/Empty'
    has_hangul = bool(re.search(r'[가-힣ㄱ-ㅎㅏ-ㅣ]', text))
    has_roman = bool(re.search(r'[a-zA-Z]', text))
    if has_hangul and has_roman: return 'Mixed'
    elif has_hangul: return 'Hangul'
    elif has_roman: return 'Roman'
    else: return 'Other'

def get_batch_score(model, tokenizer, review_batch, device):
    """
    Sends a BATCH of reviews to LLM and asks for a confidence score (1-7).
    """
    reviews_text = "\n".join([f"- {r}" for r in review_batch])

    prompt = f"""You are analyzing a batch of reviews for a medical facility in Seoul.

USER REVIEWS:
{reviews_text}

TASK:
Based strictly on the text above, how confident are you that this facility offers services in English?
Rate on a Likert scale from 1 to 7:
1 = Definitely No (Explicit complaints about language barrier)
4 = Unsure (Reviews are in English but don't mention staff capabilities)
7 = Definite Yes (Explicit praise for English speaking staff)

OUTPUT FORMAT:
Return ONLY the single number (1-7).
"""
    messages = [{"role": "system", "content": "Output only a single integer."}, {"role": "user", "content": prompt}]
    
    text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    
    # Move inputs to the specific GPU for this process
    model_inputs = tokenizer([text], return_tensors="pt").to(device)
    
    with torch.no_grad():
        generated_ids = model.generate(
            **model_inputs, 
            max_new_tokens=MAX_NEW_TOKENS, 
            temperature=0.1
        )
        
    generated_ids = [output_ids[len(input_ids):] for input_ids, output_ids in zip(model_inputs.input_ids, generated_ids)]
    response = tokenizer.batch_decode(generated_ids, skip_special_tokens=True)[0]
    
    match = re.search(r'\d', response)
    return int(match.group()) if match else 4

def process_facility_full(model, tokenizer, all_reviews, device):
    if not all_reviews:
        return {'avg': 0, 'max': 0, 'count': 0}
        
    batch_scores = []
    # Process in chunks
    for i in range(0, len(all_reviews), BATCH_SIZE):
        batch = all_reviews[i : i + BATCH_SIZE]
        score = get_batch_score(model, tokenizer, batch, device)
        batch_scores.append(score)
        
    return {
        'avg': round(sum(batch_scores) / len(batch_scores), 2),
        'max': max(batch_scores),
        'count': len(all_reviews)
    }

def worker_process(gpu_id, facility_ids, reviews_by_facility_chunk):
    """
    The main logic that runs on each separate GPU process.
    """
    # 1. Setup specific GPU
    device = torch.device(f"cuda:{gpu_id}")
    print(f"🚀 [Worker {gpu_id}] Initializing on {device}...")

    # 2. Checkpoint Logic (Per-GPU file)
    ckpt_file = os.path.join(CHECKPOINT_DIR, f"checkpoint_gpu_{gpu_id}.json")
    processed_data = {}
    if os.path.exists(ckpt_file):
        with open(ckpt_file, 'r') as f:
            processed_data = json.load(f)
        print(f"🔄 [Worker {gpu_id}] Resuming {len(processed_data)} records...")

    # Filter remaining work
    remaining_ids = [fid for fid in facility_ids if str(fid) not in processed_data]
    print(f"📋 [Worker {gpu_id}] Assigned {len(remaining_ids)} facilities.")

    if not remaining_ids:
        print(f"✅ [Worker {gpu_id}] All tasks complete.")
        return

    # 3. Load Model (Independent copy per GPU)
    # REMOVED: attn_implementation="flash_attention_2" to fix your error
    try:
        tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, trust_remote_code=True)
        model = AutoModelForCausalLM.from_pretrained(
            MODEL_ID,
            device_map=None, # We manually handle device placement
            torch_dtype=torch.float16,
            trust_remote_code=True
        ).to(device)
        model.eval()
    except Exception as e:
        print(f"❌ [Worker {gpu_id}] Model load failed: {e}")
        return

    # 4. Inference Loop
    for i, place_id in enumerate(tqdm(remaining_ids, desc=f"GPU {gpu_id}", position=gpu_id)):
        reviews = reviews_by_facility_chunk.get(place_id, [])
        
        try:
            stats = process_facility_full(model, tokenizer, reviews, device)
            processed_data[str(place_id)] = stats
        except Exception as e:
            print(f"⚠️ [Worker {gpu_id}] Error {place_id}: {e}")
            processed_data[str(place_id)] = {'avg': 0, 'max': 0, 'count': 0}

        # Save checkpoint periodically
        if (i + 1) % SAVE_EVERY_N == 0:
            with open(ckpt_file, 'w') as f:
                json.dump(processed_data, f)
    
    # Final Save
    with open(ckpt_file, 'w') as f:
        json.dump(processed_data, f)
    print(f"💤 [Worker {gpu_id}] Finished.")

def main():
    # 0. Setup
    mp.set_start_method('spawn', force=True)
    if not os.path.exists(CHECKPOINT_DIR):
        os.makedirs(CHECKPOINT_DIR)

    # 1. Load & Prepare Data (Main Process)
    print("📊 [Main] Loading Dataset...")
    if not os.path.exists(FACILITIES_PATH): 
        print(f"❌ Error: {FACILITIES_PATH} not found")
        return

    df_facilities = pd.read_parquet(FACILITIES_PATH)
    df_reviews = pd.read_parquet(REVIEWS_INPUT_PATH)
    
    # Classify
    print("🔤 [Main] Classifying languages...")
    df_reviews['script_type'] = df_reviews['review_text'].apply(classify_script)
    
    # Identify Targets
    target_reviews = df_reviews[df_reviews['script_type'].isin(['Roman', 'Mixed'])].copy()
    
    # Create Flags for ALL facilities before splitting
    lang_pivot = pd.crosstab(df_reviews['place_id'], df_reviews['script_type'])
    lang_flags = pd.DataFrame(index=lang_pivot.index)
    lang_flags['has_english'] = (lang_pivot.get('Roman', 0) > 0)
    lang_flags['has_mixed'] = (lang_pivot.get('Mixed', 0) > 0)
    
    df_facilities = df_facilities.merge(lang_flags, on='place_id', how='left')
    df_facilities['has_english'] = df_facilities['has_english'].fillna(False)
    df_facilities['has_mixed'] = df_facilities['has_mixed'].fillna(False)

    target_facilities = df_facilities[
        (df_facilities['has_english'] == True) | (df_facilities['has_mixed'] == True)
    ]['place_id'].unique().tolist()
    
    print(f"🎯 [Main] Total Target Facilities: {len(target_facilities):,}")

    # Prepare Reviews Dictionary (Shared Read-Only Memory-ish)
    reviews_by_facility = target_reviews.groupby('place_id')['review_text'].apply(list).to_dict()

    # 2. Split Work across GPUs
    chunk_size = math.ceil(len(target_facilities) / NUM_GPUS)
    processes = []

    print(f"🔥 [Main] Spawning {NUM_GPUS} workers (~{chunk_size} tasks each)...")

    for i in range(NUM_GPUS):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(target_facilities))
        
        # Slice the list of IDs for this worker
        chunk_ids = target_facilities[start_idx:end_idx]
        
        # Create process
        p = mp.Process(
            target=worker_process, 
            args=(i, chunk_ids, reviews_by_facility)
        )
        p.start()
        processes.append(p)

    # 3. Wait for completion
    for p in processes:
        p.join()

    print("✅ [Main] All workers finished. Merging results...")

    # 4. Merge Checkpoints
    final_stats = {}
    for i in range(NUM_GPUS):
        ckpt_file = os.path.join(CHECKPOINT_DIR, f"checkpoint_gpu_{i}.json")
        if os.path.exists(ckpt_file):
            with open(ckpt_file, 'r') as f:
                data = json.load(f)
                final_stats.update(data)

    # 5. Save Final Parquet
    final_avg = {}
    final_max = {}
    
    for pid, stats in final_stats.items():
        try:
            pid_typed = type(df_facilities['place_id'].iloc[0])(pid)
        except:
            pid_typed = pid
        final_avg[pid_typed] = stats['avg']
        final_max[pid_typed] = stats['max']

    df_facilities['english_confidence_score'] = df_facilities['place_id'].map(final_avg).fillna(0)
    df_facilities['english_max_score'] = df_facilities['place_id'].map(final_max).fillna(0)

    df_facilities.to_parquet(OUTPUT_FACILITIES_PATH)
    print(f"🎉 FINAL SUCCESS! Saved {len(df_facilities)} records to {OUTPUT_FACILITIES_PATH}")

if __name__ == "__main__":
    main()
