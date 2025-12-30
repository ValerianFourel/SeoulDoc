#!/usr/bin/env python3
"""
Lightweight Distributed Scraping Coordinator
Designed to run on same machine as workers (Mac)
Uses minimal resources - just job queue management
"""

from flask import Flask, jsonify, request
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import json
import threading
import time

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# Database setup
DB_FILE = "./data/scraping_jobs.db"
RESULTS_DIR = Path("./data/distributed_results")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Thread lock for database access
db_lock = threading.Lock()


def get_db():
    """Get database connection"""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize the job queue database"""
    conn = get_db()
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            place_id TEXT PRIMARY KEY,
            facility_name TEXT,
            status TEXT DEFAULT 'pending',
            worker_id TEXT,
            assigned_at TEXT,
            completed_at TEXT,
            error TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS results (
            place_id TEXT PRIMARY KEY,
            facility_name TEXT,
            review_count INTEGER,
            has_reviews BOOLEAN,
            result_data TEXT,
            scraped_at TEXT,
            worker_id TEXT
        )
    ''')
    
    c.execute('CREATE INDEX IF NOT EXISTS idx_status ON jobs(status)')
    
    conn.commit()
    conn.close()


def load_facilities_into_queue(parquet_file: str):
    """Load facilities from parquet file into job queue"""
    
    if not Path(parquet_file).exists():
        print(f"✗ File not found: {parquet_file}")
        return 0
    
    df = pd.read_parquet(parquet_file)
    
    if 'name' in df.columns:
        df = df[df['name'].str.contains('병원|의원', na=False)]
    
    conn = get_db()
    c = conn.cursor()
    
    c.execute("SELECT COUNT(*) FROM jobs")
    existing_count = c.fetchone()[0]
    
    if existing_count > 0:
        print(f"⚠ {existing_count} jobs already in queue")
        conn.close()
        return existing_count
    
    added = 0
    for _, row in df.iterrows():
        try:
            c.execute(
                "INSERT OR IGNORE INTO jobs (place_id, facility_name, status) VALUES (?, ?, ?)",
                (str(row['place_id']), row['name'], 'pending')
            )
            added += 1
        except:
            pass
    
    conn.commit()
    conn.close()
    
    print(f"✓ Added {added} facilities to job queue")
    return added


@app.route('/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})


@app.route('/stats', methods=['GET'])
def stats():
    """Get queue statistics"""
    with db_lock:
        conn = get_db()
        c = conn.cursor()
        
        c.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status")
        status_counts = {row[0]: row[1] for row in c.fetchall()}
        
        c.execute("SELECT COUNT(*) FROM results")
        total_results = c.fetchone()[0]
        
        c.execute("SELECT SUM(review_count) FROM results WHERE review_count IS NOT NULL")
        total_reviews = c.fetchone()[0] or 0
        
        conn.close()
    
    return jsonify({
        'pending': status_counts.get('pending', 0),
        'in_progress': status_counts.get('in_progress', 0),
        'completed': status_counts.get('completed', 0),
        'failed': status_counts.get('failed', 0),
        'total_results': total_results,
        'total_reviews': total_reviews
    })


@app.route('/get_job', methods=['POST'])
def get_job():
    """Get next job for a worker"""
    data = request.json
    worker_id = data.get('worker_id', 'unknown')
    
    with db_lock:
        conn = get_db()
        c = conn.cursor()
        
        c.execute(
            "SELECT place_id, facility_name FROM jobs WHERE status = 'pending' LIMIT 1"
        )
        
        row = c.fetchone()
        
        if row is None:
            conn.close()
            return jsonify({'job': None})
        
        place_id, facility_name = row
        
        c.execute(
            """UPDATE jobs 
               SET status = 'in_progress', 
                   worker_id = ?, 
                   assigned_at = ?
               WHERE place_id = ?""",
            (worker_id, datetime.now().isoformat(), place_id)
        )
        
        conn.commit()
        conn.close()
    
    return jsonify({
        'job': {
            'place_id': place_id,
            'facility_name': facility_name
        }
    })


@app.route('/submit_result', methods=['POST'])
def submit_result():
    """Submit completed job result"""
    data = request.json
    
    place_id = data.get('place_id')
    worker_id = data.get('worker_id')
    result_data = data.get('result_data', {})
    success = data.get('success', False)
    error = data.get('error')
    
    with db_lock:
        conn = get_db()
        c = conn.cursor()
        
        if success:
            c.execute(
                """UPDATE jobs 
                   SET status = 'completed', 
                       completed_at = ?
                   WHERE place_id = ?""",
                (datetime.now().isoformat(), place_id)
            )
            
            c.execute(
                """INSERT OR REPLACE INTO results 
                   (place_id, facility_name, review_count, has_reviews, result_data, scraped_at, worker_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    place_id,
                    result_data.get('facility_name'),
                    result_data.get('review_count', 0),
                    result_data.get('has_reviews', False),
                    json.dumps(result_data, ensure_ascii=False),
                    datetime.now().isoformat(),
                    worker_id
                )
            )
        else:
            c.execute(
                """UPDATE jobs 
                   SET status = 'failed', 
                       error = ?,
                       completed_at = ?
                   WHERE place_id = ?""",
                (error, datetime.now().isoformat(), place_id)
            )
        
        conn.commit()
        conn.close()
    
    return jsonify({'status': 'success'})


@app.route('/export_results', methods=['GET'])
def export_results():
    """Export all results to parquet file"""
    with db_lock:
        conn = get_db()
        df = pd.read_sql_query("SELECT * FROM results", conn)
        conn.close()
    
    if len(df) == 0:
        return jsonify({'error': 'No results to export'})
    
    # Parse JSON and expand reviews
    records = []
    for _, row in df.iterrows():
        result_data = json.loads(row['result_data']) if row['result_data'] else {}
        reviews = result_data.get('reviews', [])
        
        if reviews:
            for review in reviews:
                record = {
                    'place_id': row['place_id'],
                    'facility_name': row['facility_name'],
                    'worker_id': row['worker_id'],
                    'scraped_at': row['scraped_at']
                }
                record.update(review)
                records.append(record)
        else:
            records.append({
                'place_id': row['place_id'],
                'facility_name': row['facility_name'],
                'worker_id': row['worker_id'],
                'scraped_at': row['scraped_at']
            })
    
    result_df = pd.DataFrame(records)
    
    output_file = RESULTS_DIR / f"reviews_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    result_df.to_parquet(output_file, index=False)
    
    csv_file = output_file.with_suffix('.csv')
    result_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    
    return jsonify({
        'status': 'success',
        'records': len(result_df),
        'parquet_file': str(output_file),
        'csv_file': str(csv_file)
    })


def run_server(host='0.0.0.0', port=5000):
    """Run Flask server (called from thread)"""
    app.run(host=host, port=port, debug=False, threaded=True, use_reloader=False)


def start_coordinator(parquet_file: str = None, host='0.0.0.0', port=5000):
    """Start coordinator in background thread"""
    
    print("="*70)
    print("LIGHTWEIGHT COORDINATOR")
    print("="*70)
    
    init_db()
    
    if parquet_file and Path(parquet_file).exists():
        load_facilities_into_queue(parquet_file)
    
    import socket
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    
    print(f"\n✓ Coordinator starting on: http://{local_ip}:{port}")
    print(f"  (Running in background thread - minimal resources)\n")
    
    # Start server in daemon thread
    server_thread = threading.Thread(
        target=run_server,
        args=(host, port),
        daemon=True
    )
    server_thread.start()
    
    # Give it a moment to start
    time.sleep(2)
    
    return local_ip, port


if __name__ == "__main__":
    import sys
    
    parquet_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    if parquet_file:
        start_coordinator(parquet_file)
    else:
        init_db()
        run_server()
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nShutting down...")
