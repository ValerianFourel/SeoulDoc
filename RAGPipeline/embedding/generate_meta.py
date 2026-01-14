"""
Seoul Medical Reviews - Part 5: Meta Summary Generation (LOCAL GPUs - ROBUST)
===============================================================================
Parallel generation with 2x Qwen2.5-14B-Instruct instances on 4x A100 40GB
UPDATED VERSION: Fixed Offload Folder Issue (Solution 1)
"""

import json
import torch
import gc
import pandas as pd
from transformers import AutoTokenizer, AutoModelForCausalLM
from tqdm import tqdm
import os
import pickle
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import threading
import random
import re
import shutil
from typing import List, Dict, Any, Optional

# ==========================================
# CONFIGURATION
# ==========================================
INPUT_PROMPTS = "../../../seoul-medical-facilities/seoul_medical_facility_prompts.pkl"
OUTPUT_FILE = "../../../seoul-medical-facilities/seoul_medical_rag_knowledge.parquet"
STATE_FILE = "../../../seoul-medical-facilities/generation_state_local.json"
TEST_MODE_OUTPUT = "../../../seoul-medical-facilities/test_mode_results_local.json"

# BACKUP FILES
BACKUP_OUTPUT_FILE = "../../../seoul-medical-facilities/seoul_medical_rag_knowledge_backup.parquet"
BACKUP_STATE_FILE = "../../../seoul-medical-facilities/generation_state_local_backup.json"

# Model Configuration
WRITER_LLM_ID = "Qwen/Qwen2.5-14B-Instruct"

# MODE SELECTION
TEST_MODE = False
TEST_SAMPLE_SIZE = 5

# PARALLEL CONFIGURATION
INSTANCE_1_GPUS = [0, 1]
INSTANCE_2_GPUS = [2, 3]
NUM_INSTANCES = 2

# BATCH SIZES
BATCH_SIZE_PER_INSTANCE = 16  # Per instance (total: 32 samples in parallel)

# SAVE FREQUENCY CONTROL
SAVE_EVERY_N_BATCHES = 1  
BACKUP_EVERY_N_BATCHES = 10  
PRINT_PROGRESS_EVERY_N_BATCHES = 5  

# Generation Parameters
MAX_NEW_TOKENS = 4000 
TEMPERATURE = 0.5 
TOP_P = 0.9
TOP_K = 50

# Memory allocation
MAX_MEMORY_PER_GPU = "38GiB" # Leave slight buffer on A100 40GB

os.environ['PYTORCH_CUDA_ALLOC_CONF'] = 'expandable_segments:True'

# ==========================================
# UTILITY FUNCTIONS
# ==========================================
def clean_cluster_references(prompt: str) -> str:
    """Remove cluster number references from prompts"""
    prompt = re.sub(r'\bcluster\s*\d+\b', '', prompt, flags=re.IGNORECASE)
    prompt = re.sub(r'클러스터\s*\d+', '', prompt)
    prompt = re.sub(r'\s{2,}', ' ', prompt)
    prompt = re.sub(r'\s+/', ' /', prompt)
    
    lines = []
    for line in prompt.split('\n'):
        cleaned = line.strip()
        if cleaned and not re.match(r'^[A-Za-z\s가-힣]+/\s*$', cleaned):
            lines.append(line)
        elif cleaned and '/' in cleaned and len(cleaned.split('/')[0].strip()) > 3:
            lines.append(line)
    
    return '\n'.join(lines)

def create_meta_prompt(original_prompt: str, n_reviews: int, n_summaries: int) -> str:
    """Enhance original prompt with meta-review context"""
    if n_summaries == 0:
        return None
    
    context_addition = f"""

## META-REVIEW CONTEXT:
- This facility has {n_reviews:,} reviews
- The reviews provided have been pre-filtered by relevance threshold
- Each highlight group contains ALL relevant reviews (no artificial limits)
- Based on review volume, generate {n_summaries} comprehensive meta-summaries

### Your Task:
Generate meta-summaries that synthesize insights from the pre-filtered reviews.
- Create {n_summaries} distinct summaries in English
- Create {n_summaries} corresponding summaries in Korean
- Each summary should be 2-4 sentences
- Focus on different aspects per summary
- Use natural language (NO bullet points)
- Reference patterns across multiple reviews

The reviews have been carefully selected and grouped by topic relevance.
Synthesize this rich context into {n_summaries} high-quality summaries.
"""
    
    return original_prompt + context_addition + "\n\nProvide the output strictly as a JSON object."

def combine_results(facility_obj: Dict, llm_response: Dict) -> Dict:
    result = {
        'Facility': facility_obj['Facility'],
        'Total_Reviews': facility_obj['Total_Reviews'],
        'Key_Highlights': facility_obj['Key_Highlights']
    }
    result['Summaries'] = llm_response.get('Summaries', [])
    result['Summaries_Korean'] = llm_response.get('Summaries_Korean', [])
    return result

def extract_json(text: str) -> Optional[Dict[Any, Any]]:
    if not text:
        return None
    try:
        clean_text = text.replace("```json", "").replace("```", "").strip()
        return json.loads(clean_text)
    except:
        try:
            start = text.find('{')
            end = text.rfind('}') + 1
            if start != -1 and end != -1:
                return json.loads(text[start:end])
        except:
            return None
    return None

# ==========================================
# MODEL SETUP (FIXED WITH OFFLOAD)
# ==========================================
def setup_llm_instance(model_id, target_gpus, instance_name):
    """
    Setup Qwen2.5-14B instance with explicit offload support
    """
    try:
        print(f"    🚀 Loading {instance_name} on GPUs {target_gpus}...")
        
        tokenizer = AutoTokenizer.from_pretrained(model_id)
        
        # Max memory constraint to isolate instances
        num_gpus = torch.cuda.device_count()
        max_memory_dict = {}
        for i in range(num_gpus):
            if i in target_gpus:
                max_memory_dict[i] = MAX_MEMORY_PER_GPU
            else:
                max_memory_dict[i] = "0GiB"
        
        # Add CPU limit just in case
        max_memory_dict["cpu"] = "200GiB" 

        # Create a unique offload directory for this instance
        offload_dir = f"offload_{instance_name}"
        os.makedirs(offload_dir, exist_ok=True)

        model = AutoModelForCausalLM.from_pretrained(
            model_id,
            torch_dtype="auto",
            device_map="auto",
            max_memory=max_memory_dict,
            offload_folder=offload_dir,  # <--- FIX: Provides safe place for spillover
            low_cpu_mem_usage=True
        )
        
        print(f"      ✅ {instance_name} loaded successfully")
        return model, tokenizer
        
    except Exception as e:
        print(f"      ❌ {instance_name} loading failed: {e}")
        return None, None

# ==========================================
# GENERATION LOGIC (Standard Qwen2.5)
# ==========================================
def generate_batch(model, tokenizer, prompts, max_new_tokens=MAX_NEW_TOKENS, temperature=TEMPERATURE):
    """
    Generate text using Standard Qwen2.5 Instruct format
    """
    if not prompts:
        return []
    
    formatted_prompts = []
    
    # 1. Apply Template
    for prompt in prompts:
        # Added System Prompt for better JSON control
        messages = [
            {"role": "system", "content": "You are a helpful assistant. You must output valid JSON only. No markdown, no explanations."},
            {"role": "user", "content": prompt}
        ]
        
        text = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True
        )
        formatted_prompts.append(text)
    
    # 2. Tokenize
    model_inputs = tokenizer(
        formatted_prompts, 
        return_tensors="pt", 
        padding=True, 
        truncation=True
    ).to(model.device)
    
    # 3. Generate
    with torch.no_grad():
        generated_ids = model.generate(
            **model_inputs,
            max_new_tokens=max_new_tokens,
            temperature=temperature,
            do_sample=True if temperature > 0 else False,
            top_p=TOP_P,
            top_k=TOP_K,
            pad_token_id=tokenizer.pad_token_id,
            eos_token_id=tokenizer.eos_token_id
        )
    
    # 4. Decode
    results = []
    for i, output_ids_full in enumerate(generated_ids):
        # Calculate where the new tokens start
        input_len = model_inputs.input_ids[i].shape[0]
        output_ids = output_ids_full[input_len:]
        
        # Standard decoding
        text = tokenizer.decode(output_ids, skip_special_tokens=True)
        results.append(text)
        
    return results

# ==========================================
# PARALLEL ENGINE
# ==========================================
class ParallelGenerator:
    def __init__(self, model_id, instance_configs):
        self.instances = []
        self.locks = []
        
        for name, gpus in instance_configs:
            model, tokenizer = setup_llm_instance(model_id, gpus, name)
            if model is None:
                raise RuntimeError(f"Failed to load {name}")
            
            self.instances.append({
                'model': model,
                'tokenizer': tokenizer
            })
            self.locks.append(threading.Lock())
            
        print(f"\n    ✅ All {len(self.instances)} instances ready!")
    
    def generate_parallel(self, prompts_batch, batch_size_per_instance, **gen_kwargs):
        # Split batch for instances
        instance_batches = []
        
        # Simple distribution: Give chunks to each instance
        chunk_size = (len(prompts_batch) + len(self.instances) - 1) // len(self.instances)
        
        for i in range(len(self.instances)):
            start = i * chunk_size
            end = min(start + chunk_size, len(prompts_batch))
            instance_batches.append(prompts_batch[start:end])
        
        results_container = [None] * len(self.instances)
        
        def run_instance(idx, p_batch):
            if not p_batch:
                results_container[idx] = []
                return
            
            with self.locks[idx]:
                inst = self.instances[idx]
                out = generate_batch(inst['model'], inst['tokenizer'], p_batch, **gen_kwargs)
                results_container[idx] = out
                
        with ThreadPoolExecutor(max_workers=len(self.instances)) as executor:
            futures = []
            for i, batch in enumerate(instance_batches):
                futures.append(executor.submit(run_instance, i, batch))
            
            for f in futures:
                f.result()
                
        # Flatten results
        final_output = []
        for res in results_container:
            if res:
                final_output.extend(res)
                
        return final_output

    def cleanup(self):
        for instance in self.instances:
            # Clean up offload directories if they exist
            # Note: We don't delete the model object explicitly as gc handles it,
            # but we could remove temp folders if desired.
            pass
        clear_gpu_memory()

def clear_gpu_memory():
    gc.collect()
    torch.cuda.empty_cache()

# ==========================================
# STATE MANAGEMENT
# ==========================================
class RobustGenerationState:
    def __init__(self, state_file, backup_file):
        self.state_file = state_file
        self.backup_file = backup_file
        self.state = self.load_state()

    def load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f: return json.load(f)
            except: pass
        if os.path.exists(self.backup_file):
            try:
                with open(self.backup_file, 'r') as f: return json.load(f)
            except: pass
        return {'last_batch_idx': 0, 'facilities_processed': 0, 'success_rate': 0.0}

    def save_state(self, batch_idx, processed, saved, force_backup=False):
        self.state.update({
            'last_batch_idx': batch_idx,
            'facilities_processed': processed,
            'records_saved': saved,
            'timestamp': datetime.now().isoformat()
        })
        with open(self.state_file, 'w') as f: json.dump(self.state, f)
        if force_backup: shutil.copy2(self.state_file, self.backup_file)

# ==========================================
# MAIN EXECUTION
# ==========================================
def save_results_safely(results, output_file, backup_file):
    try:
        temp = output_file + ".tmp"
        pd.DataFrame(results).to_parquet(temp)
        shutil.move(temp, output_file)
        return True
    except Exception as e:
        print(f"Save failed: {e}")
        return False

def main():
    print("="*70)
    print("Seoul Medical - Qwen2.5-14B-Instruct Parallel Generation")
    print("="*70)
    
    # 1. Load Data
    if not os.path.exists(INPUT_PROMPTS):
        print(f"❌ Input file not found: {INPUT_PROMPTS}")
        return
        
    with open(INPUT_PROMPTS, 'rb') as f:
        facility_data = pickle.load(f)
    print(f"Loaded {len(facility_data)} facilities")

    # 2. Init Generator
    try:
        generator = ParallelGenerator(
            WRITER_LLM_ID,
            [("Instance-1", INSTANCE_1_GPUS), ("Instance-2", INSTANCE_2_GPUS)]
        )
    except Exception as e:
        print(f"❌ Generator init failed: {e}")
        return

    # 3. Processing Loop
    state_mgr = RobustGenerationState(STATE_FILE, BACKUP_STATE_FILE)
    
    # Resume logic
    existing_results = []
    if os.path.exists(OUTPUT_FILE):
        try:
            existing_results = pd.read_parquet(OUTPUT_FILE).to_dict('records')
        except: pass
    
    start_idx = len(existing_results) # Simple resume based on count
    
    print(f"Starting from index {start_idx}...")
    
    batch_size = BATCH_SIZE_PER_INSTANCE * NUM_INSTANCES
    current_results = []
    
    # Batch Processing
    for i in tqdm(range(start_idx, len(facility_data), batch_size)):
        batch_data = facility_data[i : i + batch_size]
        prompts = []
        objs = []
        
        for obj in batch_data:
            p = create_meta_prompt(clean_cluster_references(obj['prompt']), obj['Total_Reviews'], obj['n_summaries'])
            if p:
                prompts.append(p)
                objs.append(obj)
        
        if prompts:
            # GENERATE
            try:
                outputs = generator.generate_parallel(prompts, BATCH_SIZE_PER_INSTANCE)
                
                for obj, txt in zip(objs, outputs):
                    json_res = extract_json(txt)
                    if json_res:
                        full_res = combine_results(obj, json_res)
                        existing_results.append(full_res)
                        current_results.append(full_res)
            except Exception as e:
                print(f"Error in batch: {e}")

        # SAVE
        if len(current_results) > 0:
            save_results_safely(existing_results, OUTPUT_FILE, BACKUP_OUTPUT_FILE)
            state_mgr.save_state(i, i+len(batch_data), len(existing_results))
            current_results = [] # Clear buffer

    generator.cleanup()
    print("Done!")

if __name__ == "__main__":
    main()
