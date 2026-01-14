"""
Seoul Medical Reviews - Part 5: Meta Summary Generation (LOCAL GPUs - ROBUST)
===============================================================================
Parallel generation with 2x Qwen3-32B instances on 4x A100 40GB
FIXED VERSION: Using HuggingFace's official loading pattern for Qwen3
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

# Check transformers version
import transformers
print(f"🔍 Transformers version: {transformers.__version__}")
if tuple(map(int, transformers.__version__.split('.')[:2])) < (4, 51):
    print(f"⚠️  CRITICAL: Qwen3 requires transformers >= 4.51.0")
    print(f"   Current version: {transformers.__version__}")
    print(f"   ")
    print(f"   🔧 FIX (choose one):")
    print(f"      # If using conda:")
    print(f"      conda update transformers")
    print(f"      ")
    print(f"      # OR use pip in conda environment:")
    print(f"      pip install --upgrade transformers")
    print(f"      ")
    print(f"      # Then restart the script")
    print()
    import sys
    sys.exit(1)

os.environ['PYTORCH_CUDA_ALLOC_CONF'] = 'expandable_segments:True'

# ==========================================
# CONFIGURATION
# ==========================================
INPUT_PROMPTS = "../../../seoul-medical-facilities/seoul_medical_facility_prompts.pkl"
OUTPUT_FILE = "../../../seoul-medical-facilities/seoul_medical_rag_knowledge.parquet"
STATE_FILE = "../../../seoul-medical-facilities/generation_state_local.json"
TEST_MODE_OUTPUT = "../../../seoul-medical-facilities/test_mode_results_local.json"

# BACKUP FILES (for safety)
BACKUP_OUTPUT_FILE = "../../../seoul-medical-facilities/seoul_medical_rag_knowledge_backup.parquet"
BACKUP_STATE_FILE = "../../../seoul-medical-facilities/generation_state_local_backup.json"

# Model Configuration - Using HuggingFace's official pattern
WRITER_LLM_ID = "Qwen/Qwen3-32B"

# MODE SELECTION
TEST_MODE = False
TEST_SAMPLE_SIZE = 5
PRINT_FULL_PROMPT = True
PRINT_FULL_OUTPUT = True

# PARALLEL CONFIGURATION
INSTANCE_1_GPUS = [0, 1]
INSTANCE_2_GPUS = [2, 3]
NUM_INSTANCES = 2

# BATCH SIZES
BATCH_SIZE_PER_INSTANCE = 12  # Per instance (total: 24 samples in parallel)

# SAVE FREQUENCY CONTROL
SAVE_EVERY_N_BATCHES = 1  # Save after EVERY batch (most robust)
BACKUP_EVERY_N_BATCHES = 10  # Create backup every 10 batches
PRINT_PROGRESS_EVERY_N_BATCHES = 5  # Print detailed progress

# Generation Parameters
MAX_NEW_TOKENS = 1000
TEMPERATURE = 0.6
TOP_P = 0.9
TOP_K = 50

# Memory allocation per GPU
MAX_MEMORY_PER_GPU = "38GiB"

# Qwen3 Thinking Mode Control
ENABLE_THINKING = False  # Set to False for faster responses without CoT reasoning

# ==========================================
# PROMPT CLEANING
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
    """Combine pre-computed metadata with LLM summaries"""
    result = {
        'Facility': facility_obj['Facility'],
        'Total_Reviews': facility_obj['Total_Reviews'],
        'Key_Highlights': facility_obj['Key_Highlights']
    }
    result['Summaries'] = llm_response.get('Summaries', [])
    result['Summaries_Korean'] = llm_response.get('Summaries_Korean', [])
    return result

def extract_json(text: str) -> Optional[Dict[Any, Any]]:
    """Safe JSON extraction"""
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

def parse_qwen3_thinking(tokenizer, output_ids):
    """Parse Qwen3 thinking content (if thinking mode enabled)"""
    try:
        # Find </think> token (151668)
        index = len(output_ids) - output_ids[::-1].index(151668)
    except ValueError:
        index = 0
    
    thinking_content = tokenizer.decode(output_ids[:index], skip_special_tokens=True).strip("\n")
    content = tokenizer.decode(output_ids[index:], skip_special_tokens=True).strip("\n")
    
    return thinking_content, content

# ==========================================
# ROBUST STATE MANAGEMENT
# ==========================================
class RobustGenerationState:
    """Enhanced state management with automatic backups and recovery"""
    
    def __init__(self, state_file, backup_file):
        self.state_file = state_file
        self.backup_file = backup_file
        self.state = self.load_state()
        self.save_counter = 0
    
    def load_state(self):
        """Load existing state with fallback to backup"""
        # Try primary state file
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                print(f"   📂 Loaded existing state from primary file:")
                print(f"      Last batch: {state.get('last_batch_idx', 0)}")
                print(f"      Processed: {state.get('facilities_processed', 0)}")
                print(f"      Success rate: {state.get('success_rate', 0):.1f}%")
                return state
            except json.JSONDecodeError:
                print("   ⚠️ Primary state file corrupted. Trying backup...")
        
        # Try backup state file
        if os.path.exists(self.backup_file):
            try:
                with open(self.backup_file, 'r') as f:
                    state = json.load(f)
                print(f"   📂 Loaded state from backup file:")
                print(f"      Last batch: {state.get('last_batch_idx', 0)}")
                print(f"      Processed: {state.get('facilities_processed', 0)}")
                # Restore primary from backup
                shutil.copy2(self.backup_file, self.state_file)
                return state
            except json.JSONDecodeError:
                print("   ⚠️ Backup state file also corrupted. Starting fresh.")
        
        print("   🆕 No valid state found. Starting fresh.")
        return {
            'last_batch_idx': 0,
            'facilities_processed': 0,
            'total_facilities': 0,
            'records_saved': 0,
            'success_rate': 0.0,
            'generation_calls': 0,
            'failed_calls': 0,
            'timestamp': datetime.now().isoformat(),
            'last_save_time': datetime.now().isoformat()
        }
    
    def save_state(self, batch_idx, facilities_processed, total_facilities, 
                   records_saved, generation_calls, failed_calls, force_backup=False):
        """Save current state with automatic backups"""
        self.state.update({
            'last_batch_idx': batch_idx,
            'facilities_processed': facilities_processed,
            'total_facilities': total_facilities,
            'records_saved': records_saved,
            'success_rate': (records_saved / facilities_processed * 100) if facilities_processed > 0 else 0,
            'generation_calls': generation_calls,
            'failed_calls': failed_calls,
            'timestamp': datetime.now().isoformat(),
            'last_save_time': datetime.now().isoformat()
        })
        
        # Save primary state
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)
        
        self.save_counter += 1
        
        # Periodic backup
        if force_backup or (self.save_counter % BACKUP_EVERY_N_BATCHES == 0):
            shutil.copy2(self.state_file, self.backup_file)
            if force_backup:
                print(f"   💾 Created backup: {self.backup_file}")
    
    def verify_integrity(self):
        """Verify state file integrity"""
        try:
            with open(self.state_file, 'r') as f:
                json.load(f)
            return True
        except:
            return False

# ==========================================
# ROBUST FILE OPERATIONS
# ==========================================
def save_results_safely(results, output_file, backup_file):
    """Save results with backup and verification"""
    try:
        # Save to temporary file first
        temp_file = output_file + ".tmp"
        df = pd.DataFrame(results)
        df.to_parquet(temp_file)
        
        # Verify the file was written correctly
        try:
            test_df = pd.read_parquet(temp_file)
            if len(test_df) != len(results):
                raise ValueError("Row count mismatch")
        except Exception as e:
            print(f"   ⚠️ Verification failed: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
            return False
        
        # Move temp to actual file
        shutil.move(temp_file, output_file)
        
        return True
        
    except Exception as e:
        print(f"   ❌ Save failed: {e}")
        return False

def create_backup(output_file, backup_file):
    """Create backup of output file"""
    try:
        if os.path.exists(output_file):
            shutil.copy2(output_file, backup_file)
            return True
    except Exception as e:
        print(f"   ⚠️ Backup failed: {e}")
    return False

# ==========================================
# GPU MEMORY MANAGEMENT
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

# ==========================================
# MODEL SETUP - USING HUGGINGFACE OFFICIAL PATTERN
# ==========================================
def setup_llm_instance(model_id, target_gpus, instance_name):
    """Setup LLM instance using HuggingFace's official Qwen3 loading pattern"""
    try:
        print(f"   🚀 Loading {instance_name} on GPUs {target_gpus}...")
        print(f"      Model: {model_id}")
        
        # Load tokenizer (HuggingFace official pattern)
        tokenizer = AutoTokenizer.from_pretrained(model_id)
        
        # Prepare max_memory dict
        num_gpus = torch.cuda.device_count()
        max_memory_dict = {
            i: MAX_MEMORY_PER_GPU if i in target_gpus else "0GiB" 
            for i in range(num_gpus)
        }
        
        print(f"      Loading model weights (torch_dtype='auto')...")
        
        # Load model with EXACT HuggingFace pattern
        # KEY: Use torch_dtype="auto" as STRING (not deprecated when string)
        model = AutoModelForCausalLM.from_pretrained(
            model_id,
            torch_dtype="auto",  # This is CORRECT for Qwen3 (as string!)
            device_map="auto",
            max_memory=max_memory_dict,
            trust_remote_code=True
        )
        
        model.eval()
        
        print(f"      ✅ {instance_name} loaded successfully:")
        for i in target_gpus:
            allocated = torch.cuda.memory_allocated(i) / 1e9
            reserved = torch.cuda.memory_reserved(i) / 1e9
            print(f"         GPU {i}: {allocated:.2f}GB allocated / {reserved:.2f}GB reserved")
        
        return model, tokenizer
        
    except Exception as e:
        print(f"      ❌ {instance_name} loading failed: {e}")
        print(f"      ")
        print(f"      💡 TROUBLESHOOTING:")
        print(f"         1. Check transformers version:")
        print(f"            python -c \"import transformers; print(transformers.__version__)\"")
        print(f"            Must be >= 4.51.0")
        print(f"         ")
        print(f"         2. Upgrade transformers:")
        print(f"            # In conda environment:")
        print(f"            conda update transformers")
        print(f"            # OR:")
        print(f"            pip install --upgrade transformers")
        print(f"         ")
        print(f"         3. Check GPU memory (need ~20GB per GPU):")
        print(f"            nvidia-smi")
        print(f"         ")
        print(f"         4. Try smaller model:")
        print(f"            WRITER_LLM_ID = 'Qwen/Qwen3-14B'")
        return None, None

# ==========================================
# GENERATION FUNCTIONS - WITH QWEN3 THINKING MODE
# ==========================================
def generate_batch(model, tokenizer, prompts, max_new_tokens=MAX_NEW_TOKENS, temperature=TEMPERATURE):
    """Generate text for batch using Qwen3's official pattern"""
    if not prompts:
        return []
    
    formatted_prompts = []
    for prompt in prompts:
        messages = [
            {"role": "system", "content": "You are a helpful assistant. You must output valid JSON only. No markdown, no explanations. Reviews are pre-filtered and grouped by relevance. Generate comprehensive meta-summaries from the provided context."},
            {"role": "user", "content": prompt}
        ]
        
        # Use Qwen3's apply_chat_template with enable_thinking parameter
        text = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
            enable_thinking=ENABLE_THINKING  # Controls thinking mode
        )
        formatted_prompts.append(text)
    
    # Tokenize
    model_inputs = tokenizer(
        formatted_prompts, 
        return_tensors="pt", 
        padding=True, 
        truncation=True,
        max_length=4096
    ).to(model.device)
    
    # Generate
    with torch.no_grad():
        generated_ids = model.generate(
            **model_inputs,
            max_new_tokens=max_new_tokens,
            temperature=temperature,
            do_sample=True,
            top_p=TOP_P,
            top_k=TOP_K,
            pad_token_id=tokenizer.pad_token_id,
            eos_token_id=tokenizer.eos_token_id
        )
    
    # Decode outputs
    generated_texts = []
    for i, output_ids_full in enumerate(generated_ids):
        # Get only the generated part (excluding input)
        input_length = model_inputs.input_ids[i].shape[0]
        output_ids = output_ids_full[input_length:].tolist()
        
        if ENABLE_THINKING:
            # Parse thinking content
            thinking_content, content = parse_qwen3_thinking(tokenizer, output_ids)
            # Use only the content part (without thinking)
            generated_texts.append(content)
        else:
            # Standard decoding
            text = tokenizer.decode(output_ids, skip_special_tokens=True)
            generated_texts.append(text)
    
    return generated_texts

# ==========================================
# PARALLEL GENERATION ENGINE
# ==========================================
class ParallelGenerator:
    """Manages parallel generation across multiple model instances"""
    
    def __init__(self, model_id, instance_configs):
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
        
        with ThreadPoolExecutor(max_workers=len(self.instances)) as executor:
            futures = []
            for idx, prompts in enumerate(instance_batches):
                future = executor.submit(generate_on_instance, idx, prompts)
                futures.append(future)
            
            for future in futures:
                future.result()
        
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
# TEST MODE FUNCTION
# ==========================================
def run_test_mode(generator, facility_data):
    """Run test mode with random samples"""
    print(f"\n{'='*70}")
    print(f"🧪 TEST MODE - Processing {TEST_SAMPLE_SIZE} Random Samples")
    print(f"{'='*70}\n")
    
    total_facilities = len(facility_data)
    random_indices = random.sample(range(total_facilities), TEST_SAMPLE_SIZE)
    
    print(f"Selected indices: {random_indices}\n")
    
    results = []
    
    for i, idx in enumerate(random_indices):
        print(f"\n{'#'*70}")
        print(f"SAMPLE {i+1}/{TEST_SAMPLE_SIZE} (Index: {idx})")
        print(f"{'#'*70}\n")
        
        facility_obj = facility_data[idx]
        
        print(f"📋 PRE-COMPUTED METADATA (From Part 4):")
        print(f"{'─'*70}")
        metadata_preview = {
            'Facility': facility_obj['Facility'],
            'Total_Reviews': facility_obj['Total_Reviews'],
            'n_summaries': facility_obj['n_summaries'],
            'n_highlights': facility_obj['n_highlights'],
            'Key_Highlights_Sample': facility_obj['Key_Highlights'][:3] if len(facility_obj['Key_Highlights']) > 3 else facility_obj['Key_Highlights']
        }
        print(json.dumps(metadata_preview, indent=2, ensure_ascii=False))
        print()
        
        n_reviews = facility_obj['Total_Reviews']
        n_summaries = facility_obj['n_summaries']
        
        print(f"📊 GENERATION TARGET:")
        print(f"{'─'*70}")
        print(f"   Total Reviews: {n_reviews:,}")
        print(f"   Target Summaries: {n_summaries}")
        print()
        
        if n_summaries == 0:
            print(f"   ⚠️ Skipping - insufficient reviews (<10)\n")
            continue
        
        original_prompt = facility_obj['prompt']
        cleaned_prompt = clean_cluster_references(original_prompt)
        
        removed_chars = len(original_prompt) - len(cleaned_prompt)
        print(f"🧹 PROMPT CLEANING:")
        print(f"{'─'*70}")
        print(f"   Original length: {len(original_prompt):,} chars")
        print(f"   Cleaned length: {len(cleaned_prompt):,} chars")
        print(f"   Removed: {removed_chars:,} chars ({removed_chars/len(original_prompt)*100:.1f}%)")
        print()
        
        meta_prompt = create_meta_prompt(cleaned_prompt, n_reviews, n_summaries)
        
        if not meta_prompt:
            print(f"   ⚠️ Skipped due to insufficient reviews\n")
            continue
        
        if PRINT_FULL_PROMPT:
            print(f"📝 FULL PROMPT SENT TO MODEL:")
            print(f"{'─'*70}")
            print(f"[START OF PROMPT]")
            print(f"{'═'*70}")
            print(meta_prompt)
            print(f"{'═'*70}")
            print(f"[END OF PROMPT]")
            print(f"\nPrompt Length: {len(meta_prompt):,} characters")
            print()
        else:
            print(f"📝 PROMPT PREVIEW (first 1000 chars):")
            print(f"{'─'*70}")
            print(meta_prompt[:1000] + "...")
            print(f"\nFull Prompt Length: {len(meta_prompt):,} characters")
            print()
        
        print(f"🚀 Generating with {WRITER_LLM_ID}...")
        outputs = generator.generate_parallel(
            [meta_prompt],
            batch_size_per_instance=1,
            max_new_tokens=MAX_NEW_TOKENS,
            temperature=TEMPERATURE
        )
        
        if outputs and outputs[0]:
            output = outputs[0]
            print(f"✅ Received response\n")
            
            if PRINT_FULL_OUTPUT:
                print(f"📤 FULL RAW MODEL RESPONSE:")
                print(f"{'─'*70}")
                print(f"[START OF RESPONSE]")
                print(f"{'═'*70}")
                print(output)
                print(f"{'═'*70}")
                print(f"[END OF RESPONSE]")
                print(f"\nResponse Length: {len(output):,} characters")
                print()
            else:
                print(f"📤 RAW MODEL RESPONSE (preview):")
                print(f"{'─'*70}")
                print(output[:1000] + "..." if len(output) > 1000 else output)
                print(f"\nFull Response Length: {len(output):,} characters")
                print()
            
            json_data = extract_json(output)
            if json_data:
                combined_result = combine_results(facility_obj, json_data)
                results.append(combined_result)
                
                print(f"✨ COMBINED FINAL RESULT:")
                print(f"{'─'*70}")
                print(json.dumps(combined_result, indent=2, ensure_ascii=False))
                print()
            else:
                print(f"   ❌ Failed to extract JSON from output\n")
        else:
            print(f"   ❌ Failed to generate output\n")
    
    print(f"\n{'='*70}")
    print(f"📊 TEST MODE SUMMARY")
    print(f"{'='*70}")
    print(f"Model: {WRITER_LLM_ID}")
    print(f"Temperature: {TEMPERATURE}")
    print(f"Thinking Mode: {'Enabled' if ENABLE_THINKING else 'Disabled'}")
    print(f"Samples Processed: {TEST_SAMPLE_SIZE}")
    print(f"Successful: {len(results)}/{TEST_SAMPLE_SIZE}")
    print(f"Success Rate: {len(results)/TEST_SAMPLE_SIZE*100:.1f}%")
    
    if results:
        avg_highlights = sum(len(r.get('Key_Highlights', [])) for r in results) / len(results)
        avg_summaries_en = sum(len(r.get('Summaries', [])) for r in results) / len(results)
        avg_summaries_ko = sum(len(r.get('Summaries_Korean', [])) for r in results) / len(results)
        
        print(f"\nAverage Metrics:")
        print(f"   Key_Highlights per facility: {avg_highlights:.1f} (from Part 4)")
        print(f"   Summaries (EN) per facility: {avg_summaries_en:.1f} (from LLM)")
        print(f"   Summaries (KO) per facility: {avg_summaries_ko:.1f} (from LLM)")
    
    print(f"\n💡 Output Structure:")
    print(f"   ✓ Facility, Total_Reviews, Key_Highlights: Ground truth from Part 4")
    print(f"   ✓ Summaries, Summaries_Korean: Generated by LLM")
    print(f"{'='*70}\n")
    
    if results:
        try:
            test_output = {
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'model': WRITER_LLM_ID,
                    'temperature': TEMPERATURE,
                    'thinking_mode': ENABLE_THINKING,
                    'samples_processed': TEST_SAMPLE_SIZE,
                    'successful': len(results),
                    'success_rate': len(results)/TEST_SAMPLE_SIZE*100,
                    'random_indices': random_indices
                },
                'results': results,
                'statistics': {
                    'avg_highlights_per_facility': avg_highlights if results else 0,
                    'avg_summaries_en_per_facility': avg_summaries_en if results else 0,
                    'avg_summaries_ko_per_facility': avg_summaries_ko if results else 0
                }
            }
            
            with open(TEST_MODE_OUTPUT, 'w', encoding='utf-8') as f:
                json.dump(test_output, f, indent=2, ensure_ascii=False)
            
            print(f"💾 Test results saved to: {TEST_MODE_OUTPUT}")
            print(f"   File size: {os.path.getsize(TEST_MODE_OUTPUT) / 1024:.1f} KB")
            print()
        except Exception as e:
            print(f"⚠️ Failed to save test results to JSON: {e}")
    
    return results

# ==========================================
# PRODUCTION MODE - ROBUST VERSION
# ==========================================
def run_production_mode(generator, facility_data):
    """Run full production mode with frequent saves"""
    print(f"\n{'='*70}")
    print(f"🚀 PRODUCTION MODE - Processing All {len(facility_data):,} Facilities")
    print(f"{'='*70}\n")
    print(f"⚠️ ROBUST MODE: Saving after EVERY {SAVE_EVERY_N_BATCHES} batch(es)")
    print(f"⚠️ BACKUP MODE: Creating backups every {BACKUP_EVERY_N_BATCHES} batches")
    print()
    
    state = RobustGenerationState(STATE_FILE, BACKUP_STATE_FILE)
    
    # Resume Logic with verification
    if os.path.exists(OUTPUT_FILE) and state.state['last_batch_idx'] > 0:
        print(f"   📂 Resuming from batch {state.state['last_batch_idx']}...")
        try:
            existing_df = pd.read_parquet(OUTPUT_FILE)
            final_results = existing_df.to_dict('records')
            print(f"   ✓ Loaded {len(final_results)} existing records")
        except Exception as e:
            print(f"   ⚠️ Failed to load results: {e}")
            # Try backup
            if os.path.exists(BACKUP_OUTPUT_FILE):
                print(f"   📂 Loading from backup...")
                existing_df = pd.read_parquet(BACKUP_OUTPUT_FILE)
                final_results = existing_df.to_dict('records')
                print(f"   ✓ Loaded {len(final_results)} records from backup")
            else:
                print(f"   ⚠️ No backup available. Starting fresh.")
                final_results = []
                state.state['last_batch_idx'] = 0
        
        start_idx = state.state['last_batch_idx'] * BATCH_SIZE_PER_INSTANCE * NUM_INSTANCES
    else:
        print(f"   🆕 Starting fresh...")
        final_results = []
        start_idx = 0

    print(f"\n[2/4] Processing {len(facility_data) - start_idx} remaining facilities...")
    print(f"   Starting from facility index: {start_idx}")
    print(f"   Total batches remaining: {(len(facility_data) - start_idx) // (BATCH_SIZE_PER_INSTANCE * NUM_INSTANCES)}")
    print()
    
    generation_calls = state.state.get('generation_calls', 0)
    failed_calls = state.state.get('failed_calls', 0)
    batch_number = state.state['last_batch_idx']
    
    total_batch_size = BATCH_SIZE_PER_INSTANCE * NUM_INSTANCES
    
    # Progress tracking
    start_time = datetime.now()
    batches_since_last_print = 0
    
    for i in tqdm(range(start_idx, len(facility_data), total_batch_size), desc="Batches"):
        batch_number += 1
        batches_since_last_print += 1
        batch_data = facility_data[i:i+total_batch_size]
        
        # Prepare prompts for this batch
        batch_prompts = []
        batch_facility_objs = []
        
        for facility_obj in batch_data:
            cleaned_prompt = clean_cluster_references(facility_obj['prompt'])
            n_reviews = facility_obj['Total_Reviews']
            n_summaries = facility_obj['n_summaries']
            
            meta_prompt = create_meta_prompt(cleaned_prompt, n_reviews, n_summaries)
            
            if meta_prompt:
                batch_prompts.append(meta_prompt)
                batch_facility_objs.append(facility_obj)
            else:
                generation_calls += 1
                failed_calls += 1
        
        # Generate batch in parallel
        if batch_prompts:
            try:
                outputs = generator.generate_parallel(
                    batch_prompts,
                    batch_size_per_instance=BATCH_SIZE_PER_INSTANCE,
                    max_new_tokens=MAX_NEW_TOKENS,
                    temperature=TEMPERATURE
                )
                
                generation_calls += len(batch_prompts)
                
                # Process outputs
                for facility_obj, output in zip(batch_facility_objs, outputs):
                    if output:
                        json_data = extract_json(output)
                        if json_data:
                            combined_result = combine_results(facility_obj, json_data)
                            final_results.append(combined_result)
                        else:
                            failed_calls += 1
                    else:
                        failed_calls += 1
            
            except Exception as e:
                print(f"\n   ❌ Batch generation failed: {e}")
                failed_calls += len(batch_prompts)

        # SAVE AFTER EVERY N BATCHES (default: every batch)
        if batch_number % SAVE_EVERY_N_BATCHES == 0:
            # Save parquet
            if final_results:
                success = save_results_safely(final_results, OUTPUT_FILE, BACKUP_OUTPUT_FILE)
                if not success:
                    print(f"   ⚠️ Failed to save results at batch {batch_number}")
            
            # Save state
            force_backup = (batch_number % BACKUP_EVERY_N_BATCHES == 0)
            state.save_state(
                batch_idx=batch_number,
                facilities_processed=i + len(batch_data),
                total_facilities=len(facility_data),
                records_saved=len(final_results),
                generation_calls=generation_calls,
                failed_calls=failed_calls,
                force_backup=force_backup
            )
            
            # Create backup of parquet
            if force_backup and final_results:
                create_backup(OUTPUT_FILE, BACKUP_OUTPUT_FILE)

        # PRINT DETAILED PROGRESS
        if batches_since_last_print >= PRINT_PROGRESS_EVERY_N_BATCHES:
            batches_since_last_print = 0
            
            elapsed = (datetime.now() - start_time).total_seconds()
            batches_done = batch_number - state.state.get('initial_batch_idx', 0)
            remaining_batches = (len(facility_data) - (i + len(batch_data))) // total_batch_size
            
            if batches_done > 0:
                avg_time_per_batch = elapsed / batches_done
                eta_seconds = avg_time_per_batch * remaining_batches
                eta_minutes = eta_seconds / 60
            else:
                eta_minutes = 0
            
            print(f"\n   📊 Progress Update (Batch {batch_number}):")
            print(f"      Facilities: {i + len(batch_data):,}/{len(facility_data):,} ({(i + len(batch_data))/len(facility_data)*100:.1f}%)")
            print(f"      Records saved: {len(final_results):,}")
            print(f"      Success rate: {state.state['success_rate']:.1f}%")
            print(f"      ETA: ~{eta_minutes:.1f} minutes")
            
            # GPU memory stats
            print(f"      GPU Memory:")
            for gpu_id in range(4):
                allocated = torch.cuda.memory_allocated(gpu_id) / 1e9
                print(f"        GPU {gpu_id}: {allocated:.2f}GB")
            
            # State file verification
            if not state.verify_integrity():
                print(f"      ⚠️ State file integrity check failed!")

    # FINAL SAVE
    print(f"\n[3/4] Saving final output...")
    if final_results:
        success = save_results_safely(final_results, OUTPUT_FILE, BACKUP_OUTPUT_FILE)
        if success:
            print(f"   ✅ Saved {len(final_results)} records to {OUTPUT_FILE}")
            # Create final backup
            create_backup(OUTPUT_FILE, BACKUP_OUTPUT_FILE)
            print(f"   ✅ Created final backup: {BACKUP_OUTPUT_FILE}")
        else:
            print(f"   ⚠️ Final save failed!")
    
    # Update final state
    state.save_state(
        batch_idx=batch_number,
        facilities_processed=len(facility_data),
        total_facilities=len(facility_data),
        records_saved=len(final_results),
        generation_calls=generation_calls,
        failed_calls=failed_calls,
        force_backup=True
    )
    
    print(f"\n{'='*70}")
    print(f"📊 FINAL STATS")
    print(f"{'='*70}")
    print(f"Model: {WRITER_LLM_ID}")
    print(f"Temperature: {TEMPERATURE}")
    print(f"Thinking Mode: {'Enabled' if ENABLE_THINKING else 'Disabled'}")
    print(f"Total Generation Calls: {generation_calls}")
    print(f"Success Rate: {(len(final_results)/generation_calls*100) if generation_calls > 0 else 0:.1f}%")
    print(f"Total Time: {(datetime.now() - start_time).total_seconds() / 60:.1f} minutes")
    print(f"\n💡 Output Files:")
    print(f"   ✓ Main output: {OUTPUT_FILE}")
    print(f"   ✓ Backup: {BACKUP_OUTPUT_FILE}")
    print(f"   ✓ State: {STATE_FILE}")
    print(f"   ✓ State backup: {BACKUP_STATE_FILE}")
    print(f"{'='*70}")

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    print("="*70)
    print("Seoul Medical Meta-Reviews - QWEN3 OFFICIAL HUGGINGFACE PATTERN")
    print("Using HuggingFace's official loading pattern")
    print(f"Model: {WRITER_LLM_ID}")
    print(f"Thinking Mode: {'Enabled ✅' if ENABLE_THINKING else 'Disabled (faster) ⚡'}")
    print(f"Parallel Instances: {NUM_INSTANCES}")
    print(f"GPUs: {INSTANCE_1_GPUS} + {INSTANCE_2_GPUS}")
    print(f"Total Throughput: {BATCH_SIZE_PER_INSTANCE * NUM_INSTANCES} samples/batch")
    print(f"Save Frequency: Every {SAVE_EVERY_N_BATCHES} batch(es)")
    print(f"Backup Frequency: Every {BACKUP_EVERY_N_BATCHES} batches")
    print(f"Mode: {'🧪 TEST' if TEST_MODE else '🚀 PRODUCTION'}")
    print("="*70)
    
    # Load Data
    print(f"\n[1/4] Loading prepared data from Part 4...")
    if not os.path.exists(INPUT_PROMPTS):
        print(f"❌ File not found: {INPUT_PROMPTS}")
        return

    with open(INPUT_PROMPTS, 'rb') as f:
        facility_data = pickle.load(f)
    
    print(f"   ✓ Loaded {len(facility_data):,} facility objects")
    
    # Initialize Parallel Generator
    print(f"\n   Initializing parallel generation engine...")
    
    try:
        generator = ParallelGenerator(
            WRITER_LLM_ID,
            [
                ("Instance-1", INSTANCE_1_GPUS),
                ("Instance-2", INSTANCE_2_GPUS)
            ]
        )
    except Exception as e:
        print(f"❌ Failed to initialize generator: {e}")
        return
    
    # Run appropriate mode
    try:
        if TEST_MODE:
            run_test_mode(generator, facility_data)
        else:
            run_production_mode(generator, facility_data)
    finally:
        # Cleanup
        print(f"\n   🧹 Cleaning up...")
        generator.cleanup()
        gc.collect()
        print(f"   ✅ Cleanup complete")

if __name__ == "__main__":
    main()
