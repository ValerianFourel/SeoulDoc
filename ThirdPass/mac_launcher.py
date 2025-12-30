#!/usr/bin/env python3
"""
All-in-One Launcher for Mac
Starts coordinator + multiple workers on the same machine
"""

import subprocess
import time
import sys
import socket
from pathlib import Path


def get_local_ip():
    """Get local IP address"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "localhost"


def start_coordinator_and_workers(parquet_file: str, num_workers: int = 4, headless: bool = True):
    """Start coordinator in background + workers in foreground"""
    
    print("="*70)
    print("MAC LAUNCHER: COORDINATOR + WORKERS")
    print("="*70)
    
    # Check file exists
    if not Path(parquet_file).exists():
        print(f"✗ File not found: {parquet_file}")
        return
    
    ip = get_local_ip()
    coordinator_url = f"http://{ip}:5000"
    
    print(f"\nYour IP: {ip}")
    print(f"Coordinator URL: {coordinator_url}")
    print(f"Number of workers on Mac: {num_workers}")
    print(f"Headless mode: {headless}")
    
    # Start coordinator in background
    print(f"\n{'='*70}")
    print("STEP 1: STARTING COORDINATOR (BACKGROUND)")
    print(f"{'='*70}\n")
    
    coordinator_cmd = [
        sys.executable,
        'coordinator_lightweight.py',
        parquet_file
    ]
    
    coordinator_process = subprocess.Popen(
        coordinator_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    
    # Wait for coordinator to start
    print("⏳ Waiting for coordinator to start...")
    time.sleep(5)
    
    # Check if coordinator is running
    import requests
    try:
        response = requests.get(f"{coordinator_url}/health", timeout=5)
        if response.status_code == 200:
            print("✓ Coordinator is running and healthy\n")
        else:
            print("⚠ Coordinator responded but with error")
    except:
        print("⚠ Cannot reach coordinator (it may still be starting...)")
    
    # Start workers
    print(f"{'='*70}")
    print(f"STEP 2: STARTING {num_workers} WORKERS ON MAC")
    print(f"{'='*70}\n")
    
    worker_cmd = [
        sys.executable,
        'multi_worker.py',
        coordinator_url,
        '--workers', str(num_workers)
    ]
    
    if not headless:
        worker_cmd.append('--visible')
    
    print(f"Command: {' '.join(worker_cmd)}\n")
    
    # Start workers (this blocks - foreground process)
    try:
        subprocess.run(worker_cmd)
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user")
    finally:
        # Cleanup coordinator
        print("\n🔒 Stopping coordinator...")
        coordinator_process.terminate()
        try:
            coordinator_process.wait(timeout=5)
        except:
            coordinator_process.kill()
        
        print("✓ Coordinator stopped")
        
        # Print where to find results
        print(f"\n{'='*70}")
        print("EXPORT RESULTS")
        print(f"{'='*70}")
        print(f"\nTo export results, run:")
        print(f"  curl http://localhost:5000/export_results")
        print(f"\nOr visit in browser:")
        print(f"  http://localhost:5000/export_results")
        print(f"\nResults will be saved to:")
        print(f"  ./data/distributed_results/\n")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='All-in-One Launcher for Mac',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start with 4 workers (recommended for Mac with 16GB+ RAM)
  python mac_launcher.py data/facilities.parquet --workers 4
  
  # Start with 6 workers (for Mac with 32GB+ RAM)
  python mac_launcher.py data/facilities.parquet --workers 6
  
  # Show browsers (for debugging)
  python mac_launcher.py data/facilities.parquet --workers 2 --visible
        """
    )
    
    parser.add_argument(
        'parquet_file',
        help='Path to facilities parquet file'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of parallel workers on Mac (default: 4)'
    )
    parser.add_argument(
        '--visible',
        action='store_true',
        help='Show browser windows (default: headless)'
    )
    
    args = parser.parse_args()
    
    # Recommendations based on workers
    if args.workers > 6:
        print("⚠ Warning: >6 workers may be too many. Recommended: 4-6 workers")
        proceed = input("Continue anyway? (y/n): ")
        if proceed.lower() != 'y':
            return
    
    start_coordinator_and_workers(
        parquet_file=args.parquet_file,
        num_workers=args.workers,
        headless=not args.visible
    )


if __name__ == "__main__":
    main()
