"""
Seoul Medical Reviews - Part 5: Meta Summary Generation (LOCAL GPUs - ROBUST)
===============================================================================
Parallel generation with 2x Qwen2.5-14B-Instruct instances on 4x A100 40GB
FIXED VERSION: Proper Pad Token + Verbose Logging
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
BATCH_SIZE_PER_INSTANCE = 1  # Per instance (total: 2 samples in parallel)

# SAVE FREQUENCY CONTROL
SAVE_EVERY_N_BATCHES = 1  
BACKUP_EVERY_N_BATCHES = 1  
PRINT_PROGRESS_EVERY_N_BATCHES = 1  

# Generation Parameters
MAX_NEW_TOKENS = 4000 
TEMPERATURE = 0.5 
TOP_P = 0.9
TOP_K = 50

# Tokenization Parameters
MAX_INPUT_LENGTH = 8192  # Maximum input sequence length

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
# MODEL SETUP (CRITICAL FIX: PROPER PAD TOKEN)
# ==========================================
def setup_llm_instance(model_id, target_gpus, instance_name):
    """
    Setup Qwen2.5-14B instance with FIXED pad token configuration
    CRITICAL FIX: Uses token ID 0 for padding instead of EOS token
    """
    try:
        print(f"    🚀 Loading {instance_name} on GPUs {target_gpus}...")
        
        # Load tokenizer
        tokenizer = AutoTokenizer.from_pretrained(model_id)
        
        # ============================================================
        # CRITICAL FIX: Use token ID 0 for padding (NOT EOS token!)
        # ============================================================
        tokenizer.padding_side = "left"  # For batched inference
        
        # Use first token in vocabulary (ID 0) as padding
        # This is a rarely-used token that won't confuse the model
        tokenizer.pad_token_id = 0
        tokenizer.pad_token = tokenizer.convert_ids_to_tokens(0)
        
        print(f"      ✅ FIXED: Using token ID 0 for padding: '{tokenizer.pad_token}'")
        print(f"      ✓ EOS token (NOT used for padding): '{tokenizer.eos_token}' (ID: {tokenizer.eos_token_id})")
        
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
            offload_folder=offload_dir,
            low_cpu_mem_usage=True
        )
        
        print(f"      ✅ {instance_name} loaded successfully")
        return model, tokenizer
        
    except Exception as e:
        print(f"      ❌ {instance_name} loading failed: {e}")
        import traceback
        traceback.print_exc()
        return None, None

# ==========================================
# GENERATION LOGIC (FIXED TOKENIZATION)
# ==========================================
def generate_batch(model, tokenizer, prompts, instance_name="Unknown", max_new_tokens=MAX_NEW_TOKENS, temperature=TEMPERATURE):
    """
    Generate text using Standard Qwen2.5 Instruct format with proper tokenization
    """
    if not prompts:
        return []
    
    print(f"\n  [{instance_name}] 📝 Starting batch generation for {len(prompts)} prompt(s)")
    
    try:
        # 1. Apply Chat Template
        print(f"  [{instance_name}] 🔧 Applying chat template...")
        formatted_prompts = []
        
        for idx, prompt in enumerate(prompts):
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
            
            # Log prompt length for debugging
            approx_tokens = len(text.split())
            print(f"  [{instance_name}]    Prompt {idx+1}: ~{approx_tokens} words")
        
        # 2. Tokenize with EXPLICIT parameters
        print(f"  [{instance_name}] 🔧 Tokenizing with max_length={MAX_INPUT_LENGTH}...")
        
        model_inputs = tokenizer(
            formatted_prompts, 
            return_tensors="pt", 
            padding=True,  # Pad to longest in batch
            truncation=True,  # Truncate if too long
            max_length=MAX_INPUT_LENGTH,  # EXPLICIT max length
            return_attention_mask=True  # Ensure attention mask is returned
        )
        
        # Move to device
        model_inputs = {k: v.to(model.device) for k, v in model_inputs.items()}
        
        # Log tokenization results
        input_shape = model_inputs['input_ids'].shape
        print(f"  [{instance_name}]    ✓ Input shape: {input_shape}")
        print(f"  [{instance_name}]    ✓ Max input tokens: {input_shape[1]}")
        print(f"  [{instance_name}]    ✓ Attention mask shape: {model_inputs['attention_mask'].shape}")
        print(f"  [{instance_name}]    ✓ Pad token ID: {tokenizer.pad_token_id}")
        print(f"  [{instance_name}]    ✓ EOS token ID: {tokenizer.eos_token_id}")
        
        # Check for potential issues
        if input_shape[1] > MAX_INPUT_LENGTH * 0.9:
            print(f"  [{instance_name}]    ⚠️ WARNING: Input near max length, may affect generation")
        
        # 3. Generate
        print(f"  [{instance_name}] 🚀 Generating (max_new_tokens={max_new_tokens})...")
        
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
        
        print(f"  [{instance_name}] ✅ Generation complete!")
        print(f"  [{instance_name}]    Output shape: {generated_ids.shape}")
        
        # 4. Decode
        print(f"  [{instance_name}] 🔧 Decoding outputs...")
        results = []
        
        for i, output_ids_full in enumerate(generated_ids):
            # Calculate where the new tokens start
            input_len = model_inputs['input_ids'][i].shape[0]
            output_ids = output_ids_full[input_len:]
            
            # Decode only the generated part
            text = tokenizer.decode(output_ids, skip_special_tokens=True)
            results.append(text)
            
            # Log output length
            print(f"  [{instance_name}]    Output {i+1}: {len(output_ids)} tokens, {len(text)} chars")
        
        print(f"  [{instance_name}] ✅ Batch complete!\n")
        return results
        
    except torch.cuda.OutOfMemoryError as e:
        print(f"  [{instance_name}] ❌ CUDA OOM Error: {e}")
        print(f"  [{instance_name}]    Clearing cache and returning empty results...")
        torch.cuda.empty_cache()
        return [""] * len(prompts)
        
    except Exception as e:
        print(f"  [{instance_name}] ❌ Generation error: {e}")
        import traceback
        traceback.print_exc()
        return [""] * len(prompts)

# ==========================================
# PARALLEL ENGINE (VERBOSE LOGGING)
# ==========================================
class ParallelGenerator:
    def __init__(self, model_id, instance_configs):
        self.instances = []
        self.locks = []
        self.instance_names = []
        
        print("\n" + "="*70)
        print("Initializing Parallel Generator")
        print("="*70)
        
        for name, gpus in instance_configs:
            model, tokenizer = setup_llm_instance(model_id, gpus, name)
            if model is None:
                raise RuntimeError(f"Failed to load {name}")
            
            self.instances.append({
                'model': model,
                'tokenizer': tokenizer,
                'name': name
            })
            self.locks.append(threading.Lock())
            self.instance_names.append(name)
            
        print(f"\n    ✅ All {len(self.instances)} instances ready!")
        print("="*70 + "\n")
    
    def generate_parallel(self, prompts_batch, batch_size_per_instance, **gen_kwargs):
        """Generate in parallel with verbose progress tracking"""
        
        print(f"\n{'='*70}")
        print(f"PARALLEL GENERATION: {len(prompts_batch)} prompts")
        print(f"{'='*70}")
        
        # Split batch for instances
        instance_batches = []
        
        # Simple distribution: Give chunks to each instance
        chunk_size = (len(prompts_batch) + len(self.instances) - 1) // len(self.instances)
        
        for i in range(len(self.instances)):
            start = i * chunk_size
            end = min(start + chunk_size, len(prompts_batch))
            batch = prompts_batch[start:end]
            instance_batches.append(batch)
            print(f"  {self.instance_names[i]}: {len(batch)} prompts (indices {start}-{end-1})")
        
        print(f"{'='*70}\n")
        
        results_container = [None] * len(self.instances)
        
        def run_instance(idx, p_batch):
            if not p_batch:
                results_container[idx] = []
                return
            
            instance_name = self.instances[idx]['name']
            print(f"\n🔵 [{instance_name}] Starting processing...")
            
            with self.locks[idx]:
                inst = self.instances[idx]
                out = generate_batch(
                    inst['model'], 
                    inst['tokenizer'], 
                    p_batch,
                    instance_name=instance_name,
                    **gen_kwargs
                )
                results_container[idx] = out
            
            print(f"🟢 [{instance_name}] Completed! Generated {len(out)} outputs\n")
                
        # Execute in parallel
        with ThreadPoolExecutor(max_workers=len(self.instances)) as executor:
            futures = []
            for i, batch in enumerate(instance_batches):
                futures.append(executor.submit(run_instance, i, batch))
            
            # Wait for all to complete
            for f in futures:
                f.result()
        
        # Flatten results
        final_output = []
        for res in results_container:
            if res:
                final_output.extend(res)
        
        print(f"\n{'='*70}")
        print(f"PARALLEL GENERATION COMPLETE: {len(final_output)} total outputs")
        print(f"{'='*70}\n")
        
        return final_output

    def cleanup(self):
        print("\n🧹 Cleaning up GPU memory...")
        for instance in self.instances:
            del instance['model']
            del instance['tokenizer']
        self.instances = []
        clear_gpu_memory()
        print("✅ Cleanup complete\n")

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
                with open(self.state_file, 'r') as f: 
                    state = json.load(f)
                    print(f"📂 Loaded state: {state}")
                    return state
            except: 
                pass
        if os.path.exists(self.backup_file):
            try:
                with open(self.backup_file, 'r') as f: 
                    state = json.load(f)
                    print(f"📂 Loaded backup state: {state}")
                    return state
            except: 
                pass
        return {'last_batch_idx': 0, 'facilities_processed': 0, 'success_rate': 0.0}

    def save_state(self, batch_idx, processed, saved, force_backup=False):
        self.state.update({
            'last_batch_idx': batch_idx,
            'facilities_processed': processed,
            'records_saved': saved,
            'timestamp': datetime.now().isoformat()
        })
        with open(self.state_file, 'w') as f: 
            json.dump(self.state, f, indent=2)
        
        if force_backup: 
            shutil.copy2(self.state_file, self.backup_file)
            print(f"💾 State saved and backed up")
        else:
            print(f"💾 State saved")

# ==========================================
# MAIN EXECUTION
# ==========================================
def save_results_safely(results, output_file, backup_file):
    try:
        print(f"\n💾 Saving {len(results)} results to {output_file}...")
        temp = output_file + ".tmp"
        pd.DataFrame(results).to_parquet(temp)
        shutil.move(temp, output_file)
        print(f"✅ Save successful!")
        return True
    except Exception as e:
        print(f"❌ Save failed: {e}")
        return False

def main():
    print("\n" + "="*70)
    print("Seoul Medical - Qwen2.5-14B-Instruct Parallel Generation")
    print("FIXED VERSION: Proper Pad Token (Token ID 0)")
    print("="*70)
    
    # 1. Load Data
    print(f"\n📂 Loading input data from {INPUT_PROMPTS}...")
    if not os.path.exists(INPUT_PROMPTS):
        print(f"❌ Input file not found: {INPUT_PROMPTS}")
        return
        
    with open(INPUT_PROMPTS, 'rb') as f:
        facility_data = pickle.load(f)
    print(f"✅ Loaded {len(facility_data)} facilities")

    # 2. Init Generator
    print(f"\n🚀 Initializing parallel generator...")
    try:
        generator = ParallelGenerator(
            WRITER_LLM_ID,
            [("Instance-1", INSTANCE_1_GPUS), ("Instance-2", INSTANCE_2_GPUS)]
        )
    except Exception as e:
        print(f"❌ Generator init failed: {e}")
        import traceback
        traceback.print_exc()
        return

    # 3. Processing Loop
    print(f"\n📊 Setting up processing state...")
    state_mgr = RobustGenerationState(STATE_FILE, BACKUP_STATE_FILE)
    
    # Resume logic
    existing_results = []
    if os.path.exists(OUTPUT_FILE):
        try:
            existing_results = pd.read_parquet(OUTPUT_FILE).to_dict('records')
            print(f"📂 Loaded {len(existing_results)} existing results")
        except Exception as e:
            print(f"⚠️ Could not load existing results: {e}")
    
    start_idx = len(existing_results)
    remaining = len(facility_data) - start_idx
    
    print(f"\n{'='*70}")
    print(f"PROCESSING PLAN:")
    print(f"  Total facilities: {len(facility_data)}")
    print(f"  Already processed: {start_idx}")
    print(f"  Remaining: {remaining}")
    print(f"  Batch size: {BATCH_SIZE_PER_INSTANCE * NUM_INSTANCES}")
    print(f"{'='*70}\n")
    
    if remaining == 0:
        print("✅ All facilities already processed!")
        generator.cleanup()
        return
    
    batch_size = BATCH_SIZE_PER_INSTANCE * NUM_INSTANCES
    current_results = []
    
    # Batch Processing with tqdm
    print(f"🚀 Starting batch processing...\n")
    
    for batch_num, i in enumerate(tqdm(range(start_idx, len(facility_data), batch_size), 
                                       desc="Processing batches")):
        print(f"\n{'='*70}")
        print(f"BATCH {batch_num + 1} (indices {i}-{min(i+batch_size, len(facility_data))-1})")
        print(f"{'='*70}")
        
        batch_data = facility_data[i : i + batch_size]
        prompts = []
        objs = []
        
        # Prepare prompts
        for obj in batch_data:
            p = create_meta_prompt(
                clean_cluster_references(obj['prompt']), 
                obj['Total_Reviews'], 
                obj['n_summaries']
            )
            if p:
                prompts.append(p)
                objs.append(obj)
        
        print(f"\n📝 Prepared {len(prompts)} valid prompts from {len(batch_data)} facilities")
        
        if prompts:
            # GENERATE
            try:
                outputs = generator.generate_parallel(prompts, BATCH_SIZE_PER_INSTANCE)
                
                print(f"\n🔍 Processing outputs...")
                success_count = 0
                
                for obj, txt in zip(objs, outputs):
                    json_res = extract_json(txt)
                    if json_res:
                        full_res = combine_results(obj, json_res)
                        existing_results.append(full_res)
                        current_results.append(full_res)
                        success_count += 1
                    else:
                        print(f"  ⚠️ Failed to extract JSON for {obj['Facility']}")
                
                print(f"✅ Successfully processed {success_count}/{len(objs)} facilities")
                
            except Exception as e:
                print(f"❌ Error in batch: {e}")
                import traceback
                traceback.print_exc()

        # SAVE
        if len(current_results) > 0:
            save_results_safely(existing_results, OUTPUT_FILE, BACKUP_OUTPUT_FILE)
            state_mgr.save_state(i, i+len(batch_data), len(existing_results))
            current_results = []

    print(f"\n{'='*70}")
    print(f"PROCESSING COMPLETE!")
    print(f"  Total results: {len(existing_results)}")
    print(f"{'='*70}\n")
    
    generator.cleanup()
    print("✅ Done!")

if __name__ == "__main__":
    main()
