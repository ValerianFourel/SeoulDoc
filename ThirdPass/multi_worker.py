#!/usr/bin/env python3
"""
Multi-Worker Scraper
Spawns N parallel Selenium instances for concurrent scraping
Each worker runs in its own thread with its own browser
"""

import threading
import time
import socket
import requests
from datetime import datetime
from pathlib import Path
import sys
from urllib.parse import quote

from naver_review_scraper import NaverMapsReviewScraper


class ParallelWorker:
    """Single worker thread with its own Selenium instance"""
    
    def __init__(self, worker_id: str, coordinator_url: str, headless: bool = True):
        self.worker_id = worker_id
        self.coordinator_url = coordinator_url.rstrip('/')
        self.headless = headless
        self.scraper = None
        self.jobs_completed = 0
        self.jobs_failed = 0
        self.running = True
        self.thread = None
    
    def get_job(self):
        """Get next job from coordinator"""
        try:
            response = requests.post(
                f"{self.coordinator_url}/get_job",
                json={'worker_id': self.worker_id},
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('job')
        except:
            pass
        
        return None
    
    def submit_result(self, place_id: str, result_data: dict, success: bool, error: str = None):
        """Submit result to coordinator"""
        try:
            requests.post(
                f"{self.coordinator_url}/submit_result",
                json={
                    'worker_id': self.worker_id,
                    'place_id': place_id,
                    'result_data': result_data,
                    'success': success,
                    'error': error
                },
                timeout=5
            )
            return True
        except:
            return False
    
    def scrape_facility(self, facility_name: str, place_id: str):
        """Scrape a single facility"""
        
        if self.scraper is None:
            self.scraper = NaverMapsReviewScraper(headless=self.headless)
            self.scraper.setup_driver()
        
        try:
            encoded_name = quote(facility_name)
            search_url = f"https://map.naver.com/p/search/{encoded_name}"
            
            self.scraper.driver.get(search_url)
            time.sleep(3)
            
            review_data = self.scraper.scrape_reviews_for_facility(facility_name, place_id)
            return review_data
            
        except Exception as e:
            return {
                'has_reviews': False,
                'review_count': 0,
                'reviews': [],
                'scrape_error': str(e)
            }
    
    def process_job(self, job):
        """Process a single job"""
        place_id = job['place_id']
        facility_name = job['facility_name']
        
        print(f"[{self.worker_id}] Processing: {facility_name}")
        
        try:
            result_data = self.scrape_facility(facility_name, place_id)
            result_data['facility_name'] = facility_name
            result_data['worker_id'] = self.worker_id
            result_data['scraped_at'] = datetime.now().isoformat()
            
            success = result_data.get('has_reviews') or result_data.get('scrape_error') is None
            self.submit_result(place_id, result_data, success, result_data.get('scrape_error'))
            
            if success:
                self.jobs_completed += 1
                reviews = result_data.get('review_count', 0)
                print(f"[{self.worker_id}] ✓ Completed: {reviews} reviews")
            else:
                self.jobs_failed += 1
                print(f"[{self.worker_id}] ✗ Failed: {result_data.get('scrape_error')}")
            
        except Exception as e:
            print(f"[{self.worker_id}] ✗ Error: {e}")
            self.submit_result(place_id, {'facility_name': facility_name}, False, str(e))
            self.jobs_failed += 1
    
    def run(self):
        """Main worker loop"""
        print(f"[{self.worker_id}] Worker started")
        
        consecutive_failures = 0
        max_consecutive_failures = 3
        
        while self.running:
            job = self.get_job()
            
            if job is None:
                consecutive_failures += 1
                
                if consecutive_failures >= max_consecutive_failures:
                    print(f"[{self.worker_id}] No jobs available, stopping")
                    break
                
                time.sleep(5)
                continue
            
            consecutive_failures = 0
            self.process_job(job)
            time.sleep(1)
        
        if self.scraper:
            self.scraper.close_driver()
        
        print(f"[{self.worker_id}] Worker stopped. Completed: {self.jobs_completed}, Failed: {self.jobs_failed}")
    
    def start(self):
        """Start worker in background thread"""
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()
    
    def stop(self):
        """Stop worker"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=30)


class MultiWorkerManager:
    """Manages multiple parallel workers"""
    
    def __init__(self, coordinator_url: str, num_workers: int = 2, headless: bool = True):
        self.coordinator_url = coordinator_url
        self.num_workers = num_workers
        self.headless = headless
        self.workers = []
        self.hostname = socket.gethostname()
        self.start_time = datetime.now()
    
    def check_coordinator(self):
        """Check if coordinator is reachable"""
        try:
            response = requests.get(f"{self.coordinator_url}/health", timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def start_workers(self):
        """Start all workers"""
        
        print("\n" + "="*70)
        print(f"MULTI-WORKER SCRAPER")
        print("="*70)
        print(f"Hostname: {self.hostname}")
        print(f"Coordinator: {self.coordinator_url}")
        print(f"Number of parallel workers: {self.num_workers}")
        print(f"Headless mode: {self.headless}")
        print("="*70 + "\n")
        
        # Check coordinator
        print("🔍 Checking coordinator...")
        if not self.check_coordinator():
            print("✗ Cannot reach coordinator. Exiting.")
            return False
        
        print("✓ Coordinator is healthy\n")
        
        # Start workers
        print(f"🚀 Starting {self.num_workers} parallel workers...\n")
        
        for i in range(self.num_workers):
            worker_id = f"{self.hostname}-w{i+1}"
            worker = ParallelWorker(worker_id, self.coordinator_url, self.headless)
            worker.start()
            self.workers.append(worker)
            time.sleep(2)  # Stagger startup
        
        print(f"✓ All {self.num_workers} workers started\n")
        return True
    
    def monitor_workers(self):
        """Monitor worker status"""
        
        try:
            while True:
                # Check if any workers are still running
                alive = sum(1 for w in self.workers if w.thread and w.thread.is_alive())
                
                if alive == 0:
                    print("\n✓ All workers finished")
                    break
                
                # Print status
                total_completed = sum(w.jobs_completed for w in self.workers)
                total_failed = sum(w.jobs_failed for w in self.workers)
                
                elapsed = (datetime.now() - self.start_time).total_seconds()
                rate = total_completed / (elapsed / 60) if elapsed > 0 else 0
                
                print(f"\r[{datetime.now().strftime('%H:%M:%S')}] "
                      f"Active: {alive}/{self.num_workers} | "
                      f"Completed: {total_completed} | "
                      f"Failed: {total_failed} | "
                      f"Rate: {rate:.1f}/min", end='', flush=True)
                
                time.sleep(5)
                
        except KeyboardInterrupt:
            print("\n\n⚠ Interrupted by user")
            self.stop_all_workers()
    
    def stop_all_workers(self):
        """Stop all workers"""
        print("\n🔒 Stopping all workers...")
        
        for worker in self.workers:
            worker.stop()
        
        # Wait for all to finish
        for worker in self.workers:
            if worker.thread:
                worker.thread.join(timeout=30)
        
        print("✓ All workers stopped")
    
    def print_summary(self):
        """Print final summary"""
        total_completed = sum(w.jobs_completed for w in self.workers)
        total_failed = sum(w.jobs_failed for w in self.workers)
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        print(f"\n{'='*70}")
        print("SUMMARY")
        print(f"{'='*70}")
        print(f"Hostname: {self.hostname}")
        print(f"Workers: {self.num_workers}")
        print(f"Total completed: {total_completed}")
        print(f"Total failed: {total_failed}")
        print(f"Total time: {elapsed/60:.1f} minutes")
        
        if total_completed > 0:
            avg_time = elapsed / total_completed
            print(f"Average time per job: {avg_time:.1f} seconds")
            print(f"Rate: {total_completed/(elapsed/60):.1f} jobs/minute")
        
        print(f"\nPer-worker breakdown:")
        for worker in self.workers:
            print(f"  {worker.worker_id}: {worker.jobs_completed} completed, {worker.jobs_failed} failed")
        
        print(f"{'='*70}\n")
    
    def run(self):
        """Run the multi-worker system"""
        
        if not self.start_workers():
            return
        
        try:
            self.monitor_workers()
        finally:
            self.stop_all_workers()
            self.print_summary()


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Multi-Worker Scraper')
    parser.add_argument(
        'coordinator_url',
        help='Coordinator URL (e.g., http://192.168.1.100:5000)'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=2,
        help='Number of parallel workers (default: 2)'
    )
    parser.add_argument(
        '--visible',
        action='store_true',
        help='Show browsers (default: headless)'
    )
    
    args = parser.parse_args()
    
    manager = MultiWorkerManager(
        coordinator_url=args.coordinator_url,
        num_workers=args.workers,
        headless=not args.visible
    )
    
    manager.run()


if __name__ == "__main__":
    main()
