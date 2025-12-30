#!/usr/bin/env python3
"""
Lightweight Coordinator with JSON File Storage
Each worker writes to its own JSON file on Mac
"""

from flask import Flask, request, jsonify
import json
import threading
from pathlib import Path
from datetime import datetime
import pandas as pd


app = Flask(__name__)

# Global state
jobs_queue = []
jobs_lock = threading.Lock()
job_index = 0

# Output directory for worker JSON files
OUTPUT_DIR = Path("./data/worker_results")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Worker file locks
worker_locks = {}


def load_jobs_from_parquet(parquet_file: str):
    """Load jobs from parquet file"""
    global jobs_queue, job_index
    
    print(f"Loading facilities from: {parquet_file}")
    df = pd.read_parquet(parquet_file)
    
    # Clean data
    df = df[df['place_id'].notna() & df['name'].notna()].copy()
    
    # Filter for medical facilities
    df = df[df['name'].str.contains('병원|의원', na=False)]
    
    print(f"✓ Loaded {len(df)} medical facilities")
    
    # Create job list
    with jobs_lock:
        jobs_queue = [
            {
                'place_id': str(row['place_id']),
                'facility_name': row['name'],
                'status': 'pending',
                'worker_id': None,
                'assigned_at': None
            }
            for _, row in df.iterrows()
        ]
    
    print(f"✓ Created {len(jobs_queue)} jobs")


def get_worker_file(worker_id: str) -> Path:
    """Get path to worker's JSON file"""
    # Sanitize worker_id for filename
    safe_id = worker_id.replace('/', '_').replace('\\', '_')
    return OUTPUT_DIR / f"{safe_id}.json"


def ensure_worker_lock(worker_id: str):
    """Ensure worker has a lock"""
    if worker_id not in worker_locks:
        worker_locks[worker_id] = threading.Lock()


def load_worker_data(worker_id: str) -> dict:
    """Load worker's existing data"""
    worker_file = get_worker_file(worker_id)
    
    if worker_file.exists():
        try:
            with open(worker_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    
    return {}


def save_worker_data(worker_id: str, data: dict):
    """Save worker's data to JSON file"""
    ensure_worker_lock(worker_id)
    
    with worker_locks[worker_id]:
        worker_file = get_worker_file(worker_id)
        
        # Write atomically via temp file
        temp_file = worker_file.with_suffix('.tmp')
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        temp_file.replace(worker_file)


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200


@app.route('/stats', methods=['GET'])
def stats():
    """Get current statistics"""
    with jobs_lock:
        pending = sum(1 for j in jobs_queue if j['status'] == 'pending')
        in_progress = sum(1 for j in jobs_queue if j['status'] == 'in_progress')
        completed = sum(1 for j in jobs_queue if j['status'] == 'completed')
        failed = sum(1 for j in jobs_queue if j['status'] == 'failed')
    
    # Count results from all worker files
    total_results = 0
    total_reviews = 0
    
    for worker_file in OUTPUT_DIR.glob('*.json'):
        try:
            with open(worker_file, 'r', encoding='utf-8') as f:
                worker_data = json.load(f)
                total_results += len(worker_data)
                for place_id, result in worker_data.items():
                    total_reviews += result.get('review_count', 0)
        except:
            pass
    
    return jsonify({
        'pending': pending,
        'in_progress': in_progress,
        'completed': completed,
        'failed': failed,
        'total_results': total_results,
        'total_reviews': total_reviews,
        'worker_files': len(list(OUTPUT_DIR.glob('*.json')))
    })


@app.route('/get_job', methods=['POST'])
def get_job():
    """Get next job for worker"""
    data = request.json
    worker_id = data.get('worker_id', 'unknown')
    
    with jobs_lock:
        # Find next pending job
        for job in jobs_queue:
            if job['status'] == 'pending':
                job['status'] = 'in_progress'
                job['worker_id'] = worker_id
                job['assigned_at'] = datetime.now().isoformat()
                
                return jsonify({
                    'job': {
                        'place_id': job['place_id'],
                        'facility_name': job['facility_name']
                    }
                })
    
    # No jobs available
    return jsonify({'job': None})


@app.route('/submit_result', methods=['POST'])
def submit_result():
    """Submit result from worker"""
    data = request.json
    
    worker_id = data.get('worker_id', 'unknown')
    place_id = data.get('place_id')
    result_data = data.get('result_data', {})
    success = data.get('success', False)
    
    # Update job status
    with jobs_lock:
        for job in jobs_queue:
            if job['place_id'] == place_id:
                job['status'] = 'completed' if success else 'failed'
                break
    
    # Load worker's data
    worker_data = load_worker_data(worker_id)
    
    # Add this result
    worker_data[place_id] = result_data
    
    # Save worker's data
    save_worker_data(worker_id, worker_data)
    
    return jsonify({'status': 'success'})


@app.route('/merge_results', methods=['GET'])
def merge_results():
    """Merge all worker JSON files into single file"""
    
    print("\n" + "="*70)
    print("MERGING WORKER RESULTS")
    print("="*70)
    
    # Collect all results
    all_results = {}
    worker_files = list(OUTPUT_DIR.glob('*.json'))
    
    print(f"\nFound {len(worker_files)} worker files")
    
    for worker_file in worker_files:
        try:
            print(f"  Reading: {worker_file.name}")
            with open(worker_file, 'r', encoding='utf-8') as f:
                worker_data = json.load(f)
                all_results.update(worker_data)
                print(f"    ✓ {len(worker_data)} facilities")
        except Exception as e:
            print(f"    ✗ Error: {e}")
    
    print(f"\nTotal unique facilities: {len(all_results)}")
    
    # Save merged results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save as JSON
    merged_json = OUTPUT_DIR.parent / f"reviews_merged_{timestamp}.json"
    with open(merged_json, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    print(f"\n✓ Saved merged JSON: {merged_json}")
    
    # Convert to DataFrame and save as parquet/CSV
    records = []
    for place_id, review_data in all_results.items():
        if review_data.get('has_reviews') and review_data.get('reviews'):
            for review in review_data['reviews']:
                record = {
                    'place_id': place_id,
                    'facility_name': review_data.get('facility_name', ''),
                    'review_index': review.get('review_index'),
                    'reviewer_name': review.get('reviewer_info', {}).get('reviewer_name'),
                    'review_text': review.get('review_text'),
                    'visit_date': review.get('visit_info', {}).get('visit_date'),
                    'visit_count': review.get('visit_info', {}).get('visit_count'),
                    'verification_method': review.get('visit_info', {}).get('verification_method'),
                    'visit_keywords': json.dumps(review.get('visit_keywords', []), ensure_ascii=False),
                    'image_urls': json.dumps(review.get('images', []), ensure_ascii=False),
                    'image_count': len(review.get('images', [])),
                    'has_owner_response': review.get('owner_response') is not None,
                    'owner_response_text': review.get('owner_response', {}).get('response_text') if review.get('owner_response') else None,
                    'reaction_count': review.get('reactions', {}).get('reaction_count'),
                    'scraped_at': review.get('scraped_at'),
                    'worker_id': review_data.get('worker_id')
                }
                records.append(record)
        else:
            # Facility with no reviews
            record = {
                'place_id': place_id,
                'facility_name': review_data.get('facility_name', ''),
                'review_index': None,
                'reviewer_name': None,
                'review_text': None,
                'visit_date': None,
                'visit_count': None,
                'verification_method': None,
                'visit_keywords': None,
                'image_urls': None,
                'image_count': 0,
                'has_owner_response': False,
                'owner_response_text': None,
                'reaction_count': None,
                'scraped_at': review_data.get('scraped_at'),
                'worker_id': review_data.get('worker_id')
            }
            records.append(record)
    
    df = pd.DataFrame(records)
    
    # Save as CSV
    csv_file = OUTPUT_DIR.parent / f"reviews_merged_{timestamp}.csv"
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print(f"✓ Saved merged CSV: {csv_file}")
    
    # Save as parquet
    try:
        parquet_file = OUTPUT_DIR.parent / f"reviews_merged_{timestamp}.parquet"
        df.to_parquet(parquet_file, index=False)
        print(f"✓ Saved merged Parquet: {parquet_file}")
    except:
        print(f"⚠ Could not save parquet (PyArrow issue)")
    
    # Summary
    total_reviews = df['review_text'].notna().sum()
    
    print(f"\n{'='*70}")
    print("MERGE SUMMARY")
    print(f"{'='*70}")
    print(f"Total facilities: {len(all_results):,}")
    print(f"Total review records: {len(df):,}")
    print(f"Total reviews: {total_reviews:,}")
    print(f"{'='*70}\n")
    
    return jsonify({
        'status': 'success',
        'total_facilities': len(all_results),
        'total_records': len(df),
        'total_reviews': int(total_reviews),
        'json_file': str(merged_json),
        'csv_file': str(csv_file)
    })


def main():
    """Main function"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python coordinator_json.py <parquet_file>")
        print("\nExample:")
        print("  python coordinator_json.py data/seoul_medical_facilities.parquet")
        sys.exit(1)
    
    parquet_file = sys.argv[1]
    
    if not Path(parquet_file).exists():
        print(f"Error: File not found: {parquet_file}")
        sys.exit(1)
    
    # Load jobs
    load_jobs_from_parquet(parquet_file)
    
    # Start server
    print(f"\n{'='*70}")
    print("COORDINATOR READY")
    print(f"{'='*70}")
    print(f"Worker results directory: {OUTPUT_DIR}")
    print(f"Endpoints:")
    print(f"  GET  /health           - Health check")
    print(f"  GET  /stats            - Current statistics")
    print(f"  POST /get_job          - Get next job")
    print(f"  POST /submit_result    - Submit result")
    print(f"  GET  /merge_results    - Merge all worker JSONs")
    print(f"{'='*70}\n")
    
    app.run(host='0.0.0.0', port=5000, threaded=True)


if __name__ == "__main__":
    main()