"""
Seoul Medical Reviews - Part 5: Meta Summary Generation via API (Groq Qwen)
===========================================================================
Uses pre-computed prompts with reviews already filtered by relevance threshold.
SAVES: Pre-computed metadata from Part 4 + LLM-generated summaries only
ADAPTED FOR: Groq (High Speed Inference)
"""

import json
import pandas as pd
from groq import Groq  # <--- Changed from openai to groq
from tqdm import tqdm
import os
import pickle
from datetime import datetime
import time
from typing import List, Dict, Any, Optional
import random
import re

# ==========================================
# CONFIGURATION
# ==========================================
INPUT_PROMPTS = "../../../seoul-medical-facilities/seoul_medical_facility_prompts.pkl"
OUTPUT_FILE = "../../../seoul-medical-facilities/seoul_medical_rag_knowledge.parquet"
STATE_FILE = "../../../seoul-medical-facilities/generation_state_qwen_groq.json"
TEST_MODE_OUTPUT = "../../../seoul-medical-facilities/test_mode_results_qwen_groq.json"

# API Configuration
# Ensure you have 'GROQ_API_KEY' set in your environment variables
API_KEY = os.environ.get("GROQ_API_KEY") 

# Groq usually uses 'qwen-2.5-32b' for the 32B model. 
# If a specific 'qwen3' ID exists in your tier, update it here.
MODEL_NAME = "qwen/qwen3-32b" 

# MODE SELECTION
TEST_MODE = True  # Set to False for full production run
TEST_SAMPLE_SIZE = 5

# Test mode settings
PRINT_FULL_PROMPT = True 
PRINT_FULL_OUTPUT = True 

# Performance Settings (Optimized for Groq)
BATCH_SIZE = 50
MAX_TOKENS_OUTPUT = 4096  # Groq supports large context
TEMPERATURE = 0.6
MAX_RETRIES = 5 # Increased retries for Groq rate limits
RETRY_DELAY = 2

# Rate limiting
# Groq is fast. We set this high, but rely on 429 retry logic to throttle if needed.
REQUESTS_PER_MINUTE = 300 
DELAY_BETWEEN_REQUESTS = 60 / REQUESTS_PER_MINUTE

# Checkpoint settings
CHECKPOINT_EVERY_N_BATCHES = 5

# ==========================================
# PROMPT CLEANING
# ==========================================
def clean_cluster_references(prompt: str) -> str:
    """
    Remove cluster number references from prompts.
    """
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

# ==========================================
# STATE MANAGEMENT
# ==========================================
class GenerationState:
    """Manages generation state for resume capability"""
    
    def __init__(self, state_file):
        self.state_file = state_file
        self.state = self.load_state()
    
    def load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                print(f"   📂 Loaded existing state: Batch {state.get('last_batch_idx', 0)}")
                return state
            except json.JSONDecodeError:
                print("   ⚠️ State file corrupted. Starting fresh.")
        
        return {
            'last_batch_idx': 0,
            'facilities_processed': 0,
            'total_facilities': 0,
            'records_saved': 0,
            'success_rate': 0.0,
            'api_calls': 0,
            'failed_calls': 0,
            'timestamp': datetime.now().isoformat()
        }
    
    def save_state(self, batch_idx, facilities_processed, total_facilities, 
                   records_saved, api_calls, failed_calls):
        self.state.update({
            'last_batch_idx': batch_idx,
            'facilities_processed': facilities_processed,
            'total_facilities': total_facilities,
            'records_saved': records_saved,
            'success_rate': (records_saved / facilities_processed * 100) if facilities_processed > 0 else 0,
            'api_calls': api_calls,
            'failed_calls': failed_calls,
            'timestamp': datetime.now().isoformat()
        })
        
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)

# ==========================================
# API HELPER FUNCTIONS (GROQ ADAPTED)
# ==========================================
def setup_api_client():
    """Initialize Groq Client"""
    if not API_KEY:
        raise ValueError("API key not found! Set 'GROQ_API_KEY' environment variable.")
    
    # Initialize Groq client
    client = Groq(
        api_key=API_KEY
    )
    
    print(f"   ✓ Groq Client Initialized")
    print(f"   ✓ Target Model: {MODEL_NAME}")
    print(f"   ✓ Temperature: {TEMPERATURE}")
    
    return client

def generate_with_retry(client, prompt: str, max_retries: int = MAX_RETRIES) -> Optional[str]:
    """Generate text with Groq Qwen"""
    
    # Groq requires 'json' in the system prompt for reliable JSON Mode
    system_msg = """You are a helpful assistant. You must output valid JSON only. No markdown, no explanations.
Reviews are pre-filtered and grouped by relevance. Generate comprehensive meta-summaries from the provided context."""

    for attempt in range(max_retries):
        try:
            # Groq API Call
            response = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": prompt}
                ],
                temperature=TEMPERATURE,
                max_completion_tokens=MAX_TOKENS_OUTPUT, # Groq uses max_completion_tokens
                top_p=0.9,
                response_format={"type": "json_object"}, # Groq supports JSON mode
                stream=False # We use non-streaming for batch processing to get full JSON
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            if attempt < max_retries - 1:
                # Exponential backoff
                wait_time = RETRY_DELAY * (2 ** attempt)
                error_str = str(e)
                
                if "429" in error_str:
                    print(f"\n   ⏳ Groq Rate limit (429). Waiting {wait_time}s...")
                elif "400" in error_str:
                    print(f"\n   ⚠️ Bad Request (400) - Context likely too long. Skipping.")
                    return None # Don't retry context length errors
                else:
                    print(f"\n   ⚠️ Error: {error_str[:100]}... Retrying in {wait_time}s")
                
                time.sleep(wait_time)
            else:
                print(f"\n   ❌ Failed after {max_retries} attempts: {str(e)[:100]}")
                return None
    
    return None

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

def create_meta_prompt(original_prompt: str, n_reviews: int, n_summaries: int) -> str:
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

# ==========================================
# TEST MODE FUNCTION
# ==========================================
def run_test_mode(client, facility_data):
    print(f"\n{'='*70}")
    print(f"🧪 TEST MODE (GROQ) - Processing {TEST_SAMPLE_SIZE} Random Samples")
    print(f"{'='*70}\n")
    
    total_facilities = len(facility_data)
    random_indices = random.sample(range(total_facilities), TEST_SAMPLE_SIZE)
    results = []
    
    for i, idx in enumerate(random_indices):
        print(f"\n{'#'*70}")
        print(f"SAMPLE {i+1}/{TEST_SAMPLE_SIZE}")
        
        facility_obj = facility_data[idx]
        cleaned_prompt = clean_cluster_references(facility_obj['prompt'])
        meta_prompt = create_meta_prompt(cleaned_prompt, facility_obj['Total_Reviews'], facility_obj['n_summaries'])
        
        if not meta_prompt:
            print("   ⚠️ Skipped (insufficient reviews)")
            continue
            
        if PRINT_FULL_PROMPT:
            print(f"📝 PROMPT SENT TO GROQ ({len(meta_prompt):,} chars)...")
        
        start_time = time.time()
        output = generate_with_retry(client, meta_prompt)
        elapsed = time.time() - start_time
        
        if output:
            print(f"✅ Response received in {elapsed:.2f}s")
            if PRINT_FULL_OUTPUT:
                print(f"📤 RAW OUTPUT:\n{output}\n")
            
            json_data = extract_json(output)
            if json_data:
                combined_result = combine_results(facility_obj, json_data)
                results.append(combined_result)
                print(f"✨ Parsed JSON successfully")
            else:
                print("❌ JSON Extraction failed")
        else:
            print("❌ Generation failed")
            
        time.sleep(DELAY_BETWEEN_REQUESTS)

    # Save Test Results
    if results:
        test_output = {'results': results, 'metadata': {'model': MODEL_NAME, 'timestamp': datetime.now().isoformat()}}
        with open(TEST_MODE_OUTPUT, 'w', encoding='utf-8') as f:
            json.dump(test_output, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Test results saved to: {TEST_MODE_OUTPUT}")

# ==========================================
# PRODUCTION MODE FUNCTION
# ==========================================
def run_production_mode(client, facility_data):
    print(f"\n{'='*70}")
    print(f"🚀 PRODUCTION MODE (GROQ) - Processing {len(facility_data):,} Facilities")
    print(f"{'='*70}\n")
    
    state = GenerationState(STATE_FILE)
    
    if os.path.exists(OUTPUT_FILE) and state.state['last_batch_idx'] > 0:
        print(f"   📂 Resuming from batch {state.state['last_batch_idx']}...")
        existing_df = pd.read_parquet(OUTPUT_FILE)
        final_results = existing_df.to_dict('records')
        start_idx = state.state['last_batch_idx'] * BATCH_SIZE
    else:
        print(f"   🆕 Starting fresh...")
        final_results = []
        start_idx = 0

    api_calls = state.state.get('api_calls', 0)
    failed_calls = state.state.get('failed_calls', 0)
    batch_number = state.state['last_batch_idx']
    
    for i in tqdm(range(start_idx, len(facility_data), BATCH_SIZE), desc="Batches"):
        batch_number += 1
        batch_data = facility_data[i:i+BATCH_SIZE]
        
        for facility_obj in batch_data:
            cleaned_prompt = clean_cluster_references(facility_obj['prompt'])
            meta_prompt = create_meta_prompt(cleaned_prompt, facility_obj['Total_Reviews'], facility_obj['n_summaries'])
            
            if not meta_prompt:
                api_calls += 1
                failed_calls += 1
                continue
            
            output = generate_with_retry(client, meta_prompt)
            api_calls += 1
            
            if output:
                json_data = extract_json(output)
                if json_data:
                    combined_result = combine_results(facility_obj, json_data)
                    final_results.append(combined_result)
                else:
                    failed_calls += 1
            else:
                failed_calls += 1
            
            # Simple throttle strictly for rate limit compliance
            time.sleep(DELAY_BETWEEN_REQUESTS)

        # Checkpoint
        if batch_number % CHECKPOINT_EVERY_N_BATCHES == 0 or (i + BATCH_SIZE >= len(facility_data)):
            if final_results:
                pd.DataFrame(final_results).to_parquet(OUTPUT_FILE)
            
            state.save_state(
                batch_idx=batch_number,
                facilities_processed=i + len(batch_data),
                total_facilities=len(facility_data),
                records_saved=len(final_results),
                api_calls=api_calls,
                failed_calls=failed_calls
            )
            print(f"  [Checkpoint Saved] Rows: {len(final_results)}")

    if final_results:
        pd.DataFrame(final_results).to_parquet(OUTPUT_FILE)
        print(f"   ✅ Saved {len(final_results)} records to {OUTPUT_FILE}")

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    print("="*70)
    print(f"Seoul Medical Meta-Reviews - Groq API ({MODEL_NAME})")
    print("="*70)
    
    if not os.path.exists(INPUT_PROMPTS):
        print(f"❌ File not found: {INPUT_PROMPTS}")
        return

    with open(INPUT_PROMPTS, 'rb') as f:
        facility_data = pickle.load(f)
    
    try:
        client = setup_api_client()
    except ValueError as e:
        print(f"❌ {e}")
        return
    
    if TEST_MODE:
        run_test_mode(client, facility_data)
    else:
        run_production_mode(client, facility_data)

if __name__ == "__main__":
    main()
