"""
Seoul Medical Reviews - Part 5: Meta Summary Generation via API (Qwen 3)
========================================================================
Uses pre-computed prompts with reviews already filtered by relevance threshold.
SAVES: Pre-computed metadata from Part 4 + LLM-generated summaries only
"""

import json
import pandas as pd
from openai import OpenAI
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
STATE_FILE = "../../../seoul-medical-facilities/generation_state_qwen3.json"

# API Configuration
API_BASE_URL = "https://api.deepinfra.com/v1/openai" 
API_KEY = os.environ.get("QWEN_API_KEY")
MODEL_NAME = "Qwen/Qwen3-32B"

# MODE SELECTION
TEST_MODE = True  # Set to False for full production run
TEST_SAMPLE_SIZE = 5

# Performance Settings
BATCH_SIZE = 50
MAX_TOKENS_OUTPUT = 4000  # Increased for meta-summaries
TEMPERATURE = 0.6
MAX_RETRIES = 3
RETRY_DELAY = 2

# Rate limiting
REQUESTS_PER_MINUTE = 100 
DELAY_BETWEEN_REQUESTS = 60 / REQUESTS_PER_MINUTE

# Checkpoint settings
CHECKPOINT_EVERY_N_BATCHES = 5

# ==========================================
# PROMPT CLEANING
# ==========================================
def clean_cluster_references(prompt: str) -> str:
    """
    Remove cluster number references from prompts.
    Removes patterns like:
    - 'Medical service cluster 172' / '의료 서비스 클러스터 172'
    - 'cluster 42' / '클러스터 42'
    """
    # Remove English cluster references
    # Pattern: "cluster" followed by optional space and digits
    prompt = re.sub(r'\bcluster\s*\d+\b', '', prompt, flags=re.IGNORECASE)
    
    # Remove Korean cluster references
    # Pattern: "클러스터" followed by optional space and digits
    prompt = re.sub(r'클러스터\s*\d+', '', prompt)
    
    # Clean up resulting artifacts:
    # Multiple spaces to single space
    prompt = re.sub(r'\s{2,}', ' ', prompt)
    
    # Space before slash
    prompt = re.sub(r'\s+/', ' /', prompt)
    
    # Remove lines that become empty or just "Medical service /" or similar
    lines = []
    for line in prompt.split('\n'):
        cleaned = line.strip()
        # Skip if line is now just "Medical service /" or "의료 서비스 /" or similar artifacts
        if cleaned and not re.match(r'^[A-Za-z\s가-힣]+/\s*$', cleaned):
            lines.append(line)
        elif cleaned and '/' in cleaned and len(cleaned.split('/')[0].strip()) > 3:
            # Keep lines with actual content before the slash
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
        """Load existing state or create new"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                print(f"   📂 Loaded existing state:")
                print(f"      Last batch: {state.get('last_batch_idx', 0)}")
                print(f"      Processed: {state.get('facilities_processed', 0)}")
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
        """Save current state"""
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
# API HELPER FUNCTIONS
# ==========================================
def setup_api_client():
    """Initialize Standard OpenAI Client pointing to Qwen 3 Provider"""
    if not API_KEY:
        raise ValueError("API key not found! Set 'QWEN_API_KEY' environment variable.")
    
    client = OpenAI(
        api_key=API_KEY,
        base_url=API_BASE_URL
    )
    
    print(f"   ✓ OpenAI Client Initialized")
    print(f"   ✓ Base URL: {API_BASE_URL}")
    print(f"   ✓ Target Model: {MODEL_NAME}")
    print(f"   ✓ Temperature: {TEMPERATURE}")
    
    return client

def generate_with_retry(client, prompt: str, max_retries: int = MAX_RETRIES) -> Optional[str]:
    """Generate text with Qwen 3 using OpenAI client"""
    system_msg = """You are a helpful assistant. You must output valid JSON only. No markdown, no explanations.
Reviews are pre-filtered and grouped by relevance. Generate comprehensive meta-summaries from the provided context."""

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=MAX_TOKENS_OUTPUT,
                temperature=TEMPERATURE,
                top_p=0.9,
                response_format={"type": "json_object"} 
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                if "429" in str(e):
                    print(f"\n   ⏳ Rate limit (429). Waiting {wait_time}s...")
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
    """
    Enhance original prompt with meta-review context.
    The reviews in the prompt are already filtered by relevance threshold.
    
    Args:
        original_prompt: Pre-computed prompt with filtered reviews
        n_reviews: Total number of reviews for this facility
        n_summaries: Number of summaries to generate
    """
    
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
    """
    Combine pre-computed metadata from Part 4 with LLM-generated summaries.
    
    Args:
        facility_obj: Pre-computed data from Part 4 (Facility, Total_Reviews, Key_Highlights)
        llm_response: LLM response with Summaries and Summaries_Korean
    
    Returns:
        Combined dictionary with ground truth metadata + generated summaries
    """
    # Start with pre-computed ground truth from Part 4
    result = {
        'Facility': facility_obj['Facility'],
        'Total_Reviews': facility_obj['Total_Reviews'],
        'Key_Highlights': facility_obj['Key_Highlights']
    }
    
    # Add LLM-generated summaries
    result['Summaries'] = llm_response.get('Summaries', [])
    result['Summaries_Korean'] = llm_response.get('Summaries_Korean', [])
    
    return result

# ==========================================
# TEST MODE FUNCTION WITH DETAILED OUTPUT
# ==========================================
def run_test_mode(client, facility_data):
    """Run test mode with random samples - showing full pipeline"""
    print(f"\n{'='*70}")
    print(f"🧪 TEST MODE - Processing {TEST_SAMPLE_SIZE} Random Samples")
    print(f"{'='*70}\n")
    
    # Randomly sample
    total_facilities = len(facility_data)
    random_indices = random.sample(range(total_facilities), TEST_SAMPLE_SIZE)
    
    print(f"Selected indices: {random_indices}\n")
    
    results = []
    
    for i, idx in enumerate(random_indices):
        print(f"\n{'#'*70}")
        print(f"SAMPLE {i+1}/{TEST_SAMPLE_SIZE} (Index: {idx})")
        print(f"{'#'*70}\n")
        
        facility_obj = facility_data[idx]
        
        # Show original metadata from Part 4
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
        
        # CLEAN THE PROMPT - Remove cluster references
        original_prompt = facility_obj['prompt']
        cleaned_prompt = clean_cluster_references(original_prompt)
        
        # Show cleaning stats
        removed_chars = len(original_prompt) - len(cleaned_prompt)
        print(f"🧹 PROMPT CLEANING:")
        print(f"{'─'*70}")
        print(f"   Original length: {len(original_prompt):,} chars")
        print(f"   Cleaned length: {len(cleaned_prompt):,} chars")
        print(f"   Removed: {removed_chars:,} chars ({removed_chars/len(original_prompt)*100:.1f}%)")
        print()
        
        # Show cleaned prompt preview
        print(f"📝 CLEANED PROMPT (pre-filtered reviews):")
        print(f"{'─'*70}")
        prompt_preview = cleaned_prompt[:800] if len(cleaned_prompt) > 800 else cleaned_prompt
        print(prompt_preview + "..." if len(cleaned_prompt) > 800 else prompt_preview)
        print()
        
        # Create meta-prompt with CLEANED prompt
        meta_prompt = create_meta_prompt(cleaned_prompt, n_reviews, n_summaries)
        
        if not meta_prompt:
            print(f"   ⚠️ Skipped due to insufficient reviews\n")
            continue
        
        print(f"🔧 META-ENHANCED PROMPT (sent to API):")
        print(f"{'─'*70}")
        # Show last part with meta context
        print("..." + meta_prompt[-600:])
        print()
        
        # Generate
        print(f"🚀 Sending to {MODEL_NAME}...")
        output = generate_with_retry(client, meta_prompt)
        
        if output:
            print(f"✅ Received response\n")
            
            print(f"📤 RAW API RESPONSE:")
            print(f"{'─'*70}")
            print(output[:1000] + "..." if len(output) > 1000 else output)
            print()
            
            json_data = extract_json(output)
            if json_data:
                # COMBINE: Pre-computed metadata + LLM summaries
                combined_result = combine_results(facility_obj, json_data)
                results.append(combined_result)
                
                print(f"✨ COMBINED FINAL RESULT:")
                print(f"{'─'*70}")
                print(json.dumps(combined_result, indent=2, ensure_ascii=False))
                print()
                
                # Show key stats
                print(f"📊 RESULT COMPOSITION:")
                print(f"{'─'*70}")
                print(f"   FROM PART 4 (Ground Truth):")
                print(f"      - Facility: {combined_result['Facility']}")
                print(f"      - Total_Reviews: {combined_result['Total_Reviews']}")
                print(f"      - Key_Highlights: {len(combined_result['Key_Highlights'])} items")
                print(f"\n   FROM LLM (Generated):")
                print(f"      - Summaries (EN): {len(combined_result['Summaries'])}")
                print(f"      - Summaries (KO): {len(combined_result['Summaries_Korean'])}")
                
                if combined_result.get('Key_Highlights'):
                    print(f"\n   Sample Highlights (from Part 4):")
                    for j, highlight in enumerate(combined_result['Key_Highlights'][:3], 1):
                        print(f"      {j}. EN: {highlight['topic_en']}")
                        print(f"         KO: {highlight['topic_ko']}")
                        print(f"         Relevance: {highlight['relevance']:.1%}")
                
                if combined_result.get('Summaries'):
                    print(f"\n   Sample Summary (EN, from LLM):")
                    print(f"      {combined_result['Summaries'][0][:200]}...")
                
                print()
            else:
                print(f"   ❌ Failed to extract JSON from output\n")
        else:
            print(f"   ❌ Failed to generate output\n")
        
        time.sleep(DELAY_BETWEEN_REQUESTS)
    
    # Final Summary
    print(f"\n{'='*70}")
    print(f"📊 TEST MODE SUMMARY")
    print(f"{'='*70}")
    print(f"Model: {MODEL_NAME}")
    print(f"Temperature: {TEMPERATURE}")
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
    print(f"   ✓ Reduces hallucination risk on metadata")
    print(f"{'='*70}\n")
    
    return results

# ==========================================
# PRODUCTION MODE FUNCTION
# ==========================================
def run_production_mode(client, facility_data):
    """Run full production mode with meta-review generation"""
    print(f"\n{'='*70}")
    print(f"🚀 PRODUCTION MODE - Processing All {len(facility_data):,} Facilities")
    print(f"{'='*70}\n")
    
    state = GenerationState(STATE_FILE)
    
    # Resume Logic
    if os.path.exists(OUTPUT_FILE) and state.state['last_batch_idx'] > 0:
        print(f"   📂 Resuming from batch {state.state['last_batch_idx']}...")
        existing_df = pd.read_parquet(OUTPUT_FILE)
        final_results = existing_df.to_dict('records')
        start_idx = state.state['last_batch_idx'] * BATCH_SIZE
    else:
        print(f"   🆕 Starting fresh...")
        final_results = []
        start_idx = 0

    # Processing Loop
    print(f"\n[2/4] Processing {len(facility_data) - start_idx} remaining facilities...")
    
    api_calls = state.state.get('api_calls', 0)
    failed_calls = state.state.get('failed_calls', 0)
    batch_number = state.state['last_batch_idx']
    
    for i in tqdm(range(start_idx, len(facility_data), BATCH_SIZE), desc="Batches"):
        batch_number += 1
        batch_data = facility_data[i:i+BATCH_SIZE]
        
        # Process batch
        for facility_obj in batch_data:
            # CLEAN THE PROMPT - Remove cluster references
            cleaned_prompt = clean_cluster_references(facility_obj['prompt'])
            
            # Get metadata
            n_reviews = facility_obj['Total_Reviews']
            n_summaries = facility_obj['n_summaries']
            
            # Create meta-prompt with CLEANED prompt (will return None if < 10 reviews)
            meta_prompt = create_meta_prompt(cleaned_prompt, n_reviews, n_summaries)
            
            if not meta_prompt:
                # Skip facilities with too few reviews
                api_calls += 1
                failed_calls += 1
                continue
            
            # Generate
            output = generate_with_retry(client, meta_prompt)
            api_calls += 1
            
            if output:
                json_data = extract_json(output)
                if json_data:
                    # COMBINE: Pre-computed metadata + LLM summaries
                    combined_result = combine_results(facility_obj, json_data)
                    final_results.append(combined_result)
                else:
                    failed_calls += 1
            else:
                failed_calls += 1
            
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
            
            print(f"\n   💾 Checkpoint #{batch_number // CHECKPOINT_EVERY_N_BATCHES}:")
            print(f"      Processed: {i + len(batch_data):,}/{len(facility_data):,}")
            print(f"      Saved: {len(final_results):,}")
            print(f"      Success Rate: {state.state['success_rate']:.1f}%")

    # Final Save
    print(f"\n[3/4] Saving final output...")
    if final_results:
        pd.DataFrame(final_results).to_parquet(OUTPUT_FILE)
        print(f"   ✅ Saved {len(final_results)} records to {OUTPUT_FILE}")
    
    print(f"\n{'='*70}")
    print(f"📊 FINAL STATS")
    print(f"{'='*70}")
    print(f"Model: {MODEL_NAME}")
    print(f"Temperature: {TEMPERATURE}")
    print(f"Total API Calls: {api_calls}")
    print(f"Success Rate: {(len(final_results)/api_calls*100) if api_calls > 0 else 0:.1f}%")
    print(f"\n💡 Output Structure:")
    print(f"   ✓ Metadata from Part 4: Facility, Total_Reviews, Key_Highlights")
    print(f"   ✓ Generated by LLM: Summaries, Summaries_Korean")
    print(f"{'='*70}")

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    print("="*70)
    print("Seoul Medical Meta-Reviews - Qwen 3 32B API")
    print("Uses pre-computed metadata + LLM summaries")
    print("Reduces hallucination risk on metadata")
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
    print(f"   ✓ Each contains: prompt, Facility, Total_Reviews, Key_Highlights, metadata")
    
    # Setup Client
    try:
        client = setup_api_client()
    except ValueError as e:
        print(f"❌ {e}")
        return
    
    # Run appropriate mode
    if TEST_MODE:
        run_test_mode(client, facility_data)
    else:
        run_production_mode(client, facility_data)

if __name__ == "__main__":
    main()
