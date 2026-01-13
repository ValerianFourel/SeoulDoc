"""
Seoul Medical Reviews - Part 5: Summary Generation (MAXIMIZED)
================================================================
Parallel generation with 2x Qwen2.5-32B instances on 4x A100 40GB
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

os.environ['PYTORCH_CUDA_ALLOC_CONF'] = 'expandable_segments:True'

# ==========================================
# CONFIGURATION - MAXIMIZED FOR 4x A100 40GB
# ==========================================
INPUT_PROMPTS = "../../../seoul-medical-facilities/seoul_medical_facility_prompts.pkl"
INPUT_METADATA = "../../../seoul-medical-facilities/seoul_medical_facility_metadata.pkl"
OUTPUT_FILE = "../../../seoul-medical-facilities/seoul_medical_rag_knowledge.parquet"
STATE_FILE = "../../../seoul-medical-facilities/generation_state.json"

# Qwen2.5-32B-Instruct (larger, more capable model)
WRITER_LLM_ID = "Qwen/Qwen2.5-32B-Instruct"

# PARALLEL CONFIGURATION
# Instance 1: GPUs 0,1 | Instance 2: GPUs 2,3
INSTANCE_1_GPUS = [0, 1]
INSTANCE_2_GPUS = [2, 3]
NUM_INSTANCES = 2

# BATCH SIZES - Maximized for 32B model on dual A100s
BATCH_SIZE_PER_INSTANCE = 12  # Per instance (total: 24 samples in parallel)
CHECKPOINT_EVERY_N_BATCHES = 5

# Memory allocation per GPU (leave 2GB headroom)
MAX_MEMORY_PER_GPU = "38GiB"

# ==========================================
# STATE MANAGEMENT
# ==========================================
class GenerationState:
    """Manages generation state for resume capability"""
    
    def __init__(self, state_file):
        self.state_file = state_file
        self.state = self.load_state()
    
    def load_state(self):
        """Load existing state or create new"""
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                state = json.load(f)
            print(f"   📂 Loaded existing state:")
            print(f"      Last batch: {state['last_batch_idx']}")
            print(f"      Processed: {state['facilities_processed']}/{state['total_facilities']}")
            print(f"      Success rate: {state.get('success_rate', 0):.1f}%")
            return state
        return {
            'last_batch_idx': 0,
            'facilities_processed': 0,
            'total_facilities': 0,
            'records_saved': 0,
            'success_rate': 0.0,
            'timestamp': datetime.now().isoformat()
        }
    
    def save_state(self, batch_idx, facilities_processed, total_facilities, records_saved):
        """Save current state"""
        self.state.update({
            'last_batch_idx': batch_idx,
            'facilities_processed': facilities_processed,
            'total_facilities': total_facilities,
            'records_saved': records_saved,
            'success_rate': (records_saved / facilities_processed * 100) if facilities_processed > 0 else 0,
            'timestamp': datetime.now().isoformat()
        })
        
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)

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

def setup_llm_instance(model_id, target_gpus, instance_name):
    """Setup LLM instance across specific GPUs"""
    try:
        print(f"   🚀 Loading {instance_name} on GPUs {target_gpus} (FP16)...")
        
        tokenizer = AutoTokenizer.from_pretrained(
            model_id, 
            trust_remote_code=True,
            padding_side='left'
        )
        
        if tokenizer.pad_token is None:
            tokenizer.pad_token = tokenizer.eos_token
        
        # Configure memory allocation - maximize for target GPUs, zero for others
        num_gpus = torch.cuda.device_count()
        max_memory_dict = {
            i: MAX_MEMORY_PER_GPU if i in target_gpus else "0GiB" 
            for i in range(num_gpus)
        }
        
        model = AutoModelForCausalLM.from_pretrained(
            model_id,
            device_map="auto",
            trust_remote_code=True,
            torch_dtype=torch.float16,
            low_cpu_mem_usage=True,
            max_memory=max_memory_dict,
            attn_implementation="flash_attention_2"  # Use FlashAttention2 if available
        )
        
        model.eval()
        
        print(f"      ✓ {instance_name} loaded:")
        for i in target_gpus:
            allocated = torch.cuda.memory_allocated(i) / 1e9
            reserved = torch.cuda.memory_reserved(i) / 1e9
            print(f"        GPU {i}: {allocated:.2f}GB allocated / {reserved:.2f}GB reserved")
        
        return model, tokenizer
        
    except Exception as e:
        print(f"      ❌ {instance_name} loading failed: {e}")
        return None, None

def generate_batch(model, tokenizer, prompts, max_new_tokens=800, temperature=0.4):
    """Generate text for batch"""
    if not prompts:
        return []
    
    inputs = tokenizer(
        prompts, 
        return_tensors="pt", 
        padding=True, 
        truncation=True,
        max_length=3584
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

# ==========================================
# PARALLEL GENERATION ENGINE
# ==========================================
class ParallelGenerator:
    """Manages parallel generation across multiple model instances"""
    
    def __init__(self, model_id, instance_configs):
        """
        instance_configs: list of (name, gpu_list) tuples
        e.g., [("Instance-1", [0,1]), ("Instance-2", [2,3])]
        """
        self.instances = []
        self.locks = []
        
        for name, gpus in instance_configs:
            model, tokenizer = setup_llm_instance(model_id, gpus, name)
            if model is None:
                raise RuntimeError(f"Failed to load {name}")
            
            self.instances.append({
                'name': name,
                'model': model,
                'tokenizer': tokenizer,
                'gpus': gpus
            })
            self.locks.append(threading.Lock())
        
        print(f"\n   ✅ All {len(self.instances)} instances loaded successfully!")
    
    def generate_parallel(self, prompts_batch, batch_size_per_instance, **gen_kwargs):
        """Generate in parallel across instances"""
        # Split prompts across instances
        instance_batches = []
        for i in range(len(self.instances)):
            start = i * batch_size_per_instance
            end = start + batch_size_per_instance
            instance_batches.append(prompts_batch[start:end])
        
        results = [None] * len(self.instances)
        
        def generate_on_instance(idx, prompts):
            if not prompts:
                return
            with self.locks[idx]:
                instance = self.instances[idx]
                outputs = generate_batch(
                    instance['model'],
                    instance['tokenizer'],
                    prompts,
                    **gen_kwargs
                )
                results[idx] = outputs
        
        # Launch parallel generation
        with ThreadPoolExecutor(max_workers=len(self.instances)) as executor:
            futures = []
            for idx, prompts in enumerate(instance_batches):
                future = executor.submit(generate_on_instance, idx, prompts)
                futures.append(future)
            
            # Wait for all to complete
            for future in futures:
                future.result()
        
        # Flatten results
        all_outputs = []
        for result in results:
            if result:
                all_outputs.extend(result)
        
        return all_outputs
    
    def cleanup(self):
        """Clean up all instances"""
        for instance in self.instances:
            del instance['model']
            del instance['tokenizer']
            clear_gpu_memory(instance['gpus'])

# ==========================================
# MAIN GENERATION LOOP
# ==========================================
def main():
    print("="*70)
    print("Seoul Medical Reviews - MAXIMIZED GENERATION")
    print(f"Model: {WRITER_LLM_ID}")
    print(f"Parallel Instances: {NUM_INSTANCES}")
    print(f"GPUs: {INSTANCE_1_GPUS} + {INSTANCE_2_GPUS}")
    print(f"Total Throughput: {BATCH_SIZE_PER_INSTANCE * NUM_INSTANCES} samples/batch")
    print("="*70)
    
    # ---------------------------------------------------------
    # LOAD PREPARED DATA
    # ---------------------------------------------------------
    print(f"\n[5/5] Loading prepared data...")
    
    with open(INPUT_PROMPTS, 'rb') as f:
        facility_prompts = pickle.load(f)
    
    with open(INPUT_METADATA, 'rb') as f:
        facility_metadata = pickle.load(f)
    
    print(f"   ✓ Loaded {len(facility_prompts):,} prompts")
    
    # Initialize state
    state = GenerationState(STATE_FILE)
    
    # Load existing results if resuming
    if os.path.exists(OUTPUT_FILE) and state.state['last_batch_idx'] > 0:
        print(f"\n   📂 Resuming from checkpoint...")
        existing_df = pd.read_parquet(OUTPUT_FILE)
        final_results = existing_df.to_dict('records')
        print(f"   ✓ Loaded {len(final_results)} existing results")
        start_idx = state.state['last_batch_idx'] * BATCH_SIZE_PER_INSTANCE * NUM_INSTANCES
    else:
        print(f"\n   🆕 Starting fresh generation...")
        final_results = []
        start_idx = 0
    
    # Calculate estimates
    total_batch_size = BATCH_SIZE_PER_INSTANCE * NUM_INSTANCES
    remaining = len(facility_prompts) - start_idx
    estimated_batches = remaining / total_batch_size
    estimated_minutes = estimated_batches * 3 / 60  # ~3 sec/batch with parallel
    
    print(f"\n   Estimation:")
    print(f"     Total facilities: {len(facility_prompts):,}")
    print(f"     Remaining: {remaining:,}")
    print(f"     Batch size: {total_batch_size} (2x {BATCH_SIZE_PER_INSTANCE})")
    print(f"     Estimated time: ~{estimated_minutes:.1f} minutes")
    print(f"     Expected checkpoints: ~{int(estimated_batches / CHECKPOINT_EVERY_N_BATCHES)}")
    
    # ---------------------------------------------------------
    # INITIALIZE PARALLEL GENERATOR
    # ---------------------------------------------------------
    print(f"\n   Initializing parallel generation engine...")
    
    generator = ParallelGenerator(
        WRITER_LLM_ID,
        [
            ("Instance-1", INSTANCE_1_GPUS),
            ("Instance-2", INSTANCE_2_GPUS)
        ]
    )
    
    print(f"\n   Starting parallel generation from batch {state.state['last_batch_idx']}...")
    print(f"   📍 Checkpoint every {CHECKPOINT_EVERY_N_BATCHES} batches\n")
    
    batch_number = state.state['last_batch_idx']
    total_batch_size = BATCH_SIZE_PER_INSTANCE * NUM_INSTANCES
    
    # ---------------------------------------------------------
    # PARALLEL GENERATION LOOP
    # ---------------------------------------------------------
    for i in tqdm(
        range(start_idx, len(facility_prompts), total_batch_size), 
        desc="Parallel Gen"
    ):
        batch_number += 1
        batch_prompts = facility_prompts[i:i+total_batch_size]
        
        # Generate in parallel across both instances
        outputs = generator.generate_parallel(
            batch_prompts,
            batch_size_per_instance=BATCH_SIZE_PER_INSTANCE,
            max_new_tokens=1000,
            temperature=0.55
        )
        
        # Extract JSON from outputs
        for output_text in outputs:
            data = extract_json(output_text)
            if data:
                final_results.append(data)
        
        # Checkpoint every N batches
        if batch_number % CHECKPOINT_EVERY_N_BATCHES == 0:
            pd.DataFrame(final_results).to_parquet(OUTPUT_FILE)
            
            facilities_processed = i + len(batch_prompts)
            
            # Calculate stats
            if final_results:
                avg_highlights = sum(len(r.get('Key_Highlights', [])) for r in final_results) / len(final_results)
            else:
                avg_highlights = 0
            
            # Save state
            state.save_state(
                batch_idx=batch_number,
                facilities_processed=facilities_processed,
                total_facilities=len(facility_prompts),
                records_saved=len(final_results)
            )
            
            # GPU memory stats
            print(f"\n  💾 Checkpoint #{batch_number // CHECKPOINT_EVERY_N_BATCHES}:")
            print(f"     Batch: {batch_number}/{int(len(facility_prompts) / total_batch_size)}")
            print(f"     Facilities: {facilities_processed:,}/{len(facility_prompts):,}")
            print(f"     Records saved: {len(final_results):,}")
            print(f"     Success rate: {state.state['success_rate']:.1f}%")
            print(f"     Avg highlights: {avg_highlights:.1f}")
            
            # Show GPU utilization
            print(f"     GPU Memory:")
            for gpu_id in range(4):
                allocated = torch.cuda.memory_allocated(gpu_id) / 1e9
                print(f"       GPU {gpu_id}: {allocated:.2f}GB")

    # ---------------------------------------------------------
    # FINAL SAVE & STATISTICS
    # ---------------------------------------------------------
    print(f"\n✅ Generation complete! Saving final results...")
    pd.DataFrame(final_results).to_parquet(OUTPUT_FILE)
    
    # Update final state
    state.save_state(
        batch_idx=batch_number,
        facilities_processed=len(facility_prompts),
        total_facilities=len(facility_prompts),
        records_saved=len(final_results)
    )
    
    # Analyze results
    if final_results:
        highlights_per_facility = [len(r.get('Key_Highlights', [])) for r in final_results]
        avg_highlights = sum(highlights_per_facility) / len(highlights_per_facility)
        min_highlights = min(highlights_per_facility)
        max_highlights = max(highlights_per_facility)
        
        from collections import Counter
        highlights_dist = Counter(highlights_per_facility)
    
    print(f"\n" + "="*70)
    print(f"📊 FINAL STATISTICS:")
    print(f"="*70)
    print(f"   Facilities processed: {len(facility_prompts):,}")
    print(f"   Successful summaries: {len(final_results):,}")
    print(f"   Success rate: {state.state['success_rate']:.1f}%")
    print(f"   Total batches: {batch_number}")
    print(f"   Checkpoints saved: {batch_number // CHECKPOINT_EVERY_N_BATCHES}")
    print(f"   Parallel speedup: ~2x (2 instances)")
    
    if final_results:
        print(f"\n   Key_Highlights Statistics:")
        print(f"     Average per facility: {avg_highlights:.1f}")
        print(f"     Range: {min_highlights} - {max_highlights} highlights")
        print(f"     Total highlights: {sum(highlights_per_facility):,}")
        
        print(f"\n   Most common highlight counts:")
        for count, freq in sorted(highlights_dist.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"     {count} highlights: {freq:,} facilities ({freq/len(final_results)*100:.1f}%)")
    
    print(f"\n   Output: {OUTPUT_FILE}")
    print(f"="*70)
    
    # Cleanup
    generator.cleanup()
    gc.collect()

if __name__ == "__main__":
    main()
