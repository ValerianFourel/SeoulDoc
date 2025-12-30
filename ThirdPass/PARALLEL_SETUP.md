# ⚡ PARALLEL SCRAPING SETUP - NEW ARCHITECTURE

## 🎯 Architecture: Mac Does Most of the Work

```
┌─────────────────────────────────────┐
│  Mac (16-32GB RAM)                  │
│  • Coordinator (background)         │  
│  • 4-6 parallel Selenium workers    │  ← MOST WORK HAPPENS HERE
└───────────┬─────────────────────────┘
            │ WiFi
    ┌───────┴─────────┐
    │                 │
┌───▼────┐      ┌─────▼───┐
│Ubuntu 1│      │Ubuntu 2 │
│2-3     │      │2-3      │
│workers │      │workers  │
└────────┘      └─────────┘

Total: 8-12 parallel Selenium instances!
```

---

## 🚀 ONE-COMMAND SETUP (Mac)

```bash
# Start coordinator + 4 workers on Mac
python mac_launcher.py data/facilities.parquet --workers 4

# Or if you have 32GB+ RAM:
python mac_launcher.py data/facilities.parquet --workers 6
```

**That's it for Mac!** Coordinator runs in background, workers run in foreground.

---

## 💻 Ubuntu Setup (Simple)

### Ubuntu 1:
```bash
# Copy files from Mac
scp mac_ip:~/multi_worker.py .
scp mac_ip:~/naver_review_scraper.py .
scp -r mac_ip:~/utils .

# Start 2-3 parallel workers
python multi_worker.py http://192.168.1.100:5000 --workers 3
```

### Ubuntu 2:
```bash
# Same as Ubuntu 1
python multi_worker.py http://192.168.1.100:5000 --workers 3
```

---

## 📊 Expected Performance

**Total parallel workers: 4 (Mac) + 3 (Ubuntu1) + 3 (Ubuntu2) = 10 workers**

- **Speed:** ~50 facilities/minute (10 workers × 5 facilities/min)
- **1,000 facilities:** ~20 minutes
- **10,000 facilities:** ~3.5 hours

**10X FASTER than single machine!**

---

## 🎮 Full Command Examples

### Mac with 16GB RAM:
```bash
python mac_launcher.py data/facilities.parquet --workers 4
```

### Mac with 32GB RAM:
```bash
python mac_launcher.py data/facilities.parquet --workers 6
```

### Ubuntu with 8GB RAM:
```bash
python multi_worker.py http://192.168.1.100:5000 --workers 2
```

### Ubuntu with 16GB RAM:
```bash
python multi_worker.py http://192.168.1.100:5000 --workers 3
```

---

## 🔧 Memory Usage Guide

**Per Selenium worker:**
- Headless Chrome: ~400-500MB RAM
- CPU: ~15-20%

**Recommended worker counts:**

| Machine RAM | Workers | Total RAM Used |
|-------------|---------|----------------|
| 8GB         | 2       | ~1GB           |
| 16GB        | 4       | ~2GB           |
| 32GB        | 6       | ~3GB           |
| 64GB        | 8-10    | ~4-5GB         |

---

## 📈 Monitor Progress

### From Mac:
```bash
# Check stats
curl http://localhost:5000/stats

# Watch in real-time
watch -n 5 'curl -s http://localhost:5000/stats | python -m json.tool'
```

### From Ubuntu:
```bash
# Check Mac's stats (replace with Mac's IP)
curl http://192.168.1.100:5000/stats
```

---

## 💾 Export Results

### When scraping is done:
```bash
# Export all results
curl http://localhost:5000/export_results

# Results saved to:
# ./data/distributed_results/reviews_YYYYMMDD_HHMMSS.parquet
# ./data/distributed_results/reviews_YYYYMMDD_HHMMSS.csv
```

---

## 🎯 What's Different?

### OLD Architecture (Coordinator-Only Mac):
- Mac: Coordinator only ❌
- Ubuntu 1: 1-2 workers
- Ubuntu 2: 1-2 workers
- **Total: 2-4 workers**

### NEW Architecture (Mac Works Too):
- Mac: Coordinator + 4-6 workers ✅
- Ubuntu 1: 2-3 workers
- Ubuntu 2: 2-3 workers
- **Total: 8-12 workers**

---

## 🏃 Quick Start Summary

### 1️⃣ On Mac (ONE COMMAND):
```bash
python mac_launcher.py data/facilities.parquet --workers 4
```

### 2️⃣ On Ubuntu 1:
```bash
python multi_worker.py http://MAC_IP:5000 --workers 3
```

### 3️⃣ On Ubuntu 2:
```bash
python multi_worker.py http://MAC_IP:5000 --workers 3
```

### 4️⃣ Watch it fly! 🚀
10 parallel workers scraping simultaneously!

---

## 🎬 Example Session

**Terminal 1 (Mac):**
```bash
$ python mac_launcher.py data/facilities.parquet --workers 4

==================================================
MAC LAUNCHER: COORDINATOR + WORKERS
==================================================
Your IP: 192.168.1.100
Number of workers on Mac: 4

STEP 1: STARTING COORDINATOR (BACKGROUND)
✓ Coordinator is running and healthy

STEP 2: STARTING 4 WORKERS ON MAC
[mac-w1] Worker started
[mac-w2] Worker started
[mac-w3] Worker started
[mac-w4] Worker started

[14:25:30] Active: 4/4 | Completed: 48 | Failed: 0 | Rate: 9.6/min
```

**Terminal 2 (Ubuntu 1):**
```bash
$ python multi_worker.py http://192.168.1.100:5000 --workers 3

MULTI-WORKER SCRAPER
Number of parallel workers: 3

[ubuntu1-w1] Worker started
[ubuntu1-w2] Worker started
[ubuntu1-w3] Worker started

[14:25:30] Active: 3/3 | Completed: 36 | Failed: 0 | Rate: 7.2/min
```

**Terminal 3 (Ubuntu 2):**
```bash
$ python multi_worker.py http://192.168.1.100:5000 --workers 3

[ubuntu2-w1] Worker started
[ubuntu2-w2] Worker started
[ubuntu2-w3] Worker started

[14:25:30] Active: 3/3 | Completed: 36 | Failed: 0 | Rate: 7.2/min
```

**Combined:** 48+36+36 = 120 facilities in ~5 minutes! ⚡

---

## 🔥 Pro Tips

### 1. Test First with Few Workers:
```bash
# Test with just 1 worker first
python multi_worker.py http://localhost:5000 --workers 1 --visible
```

### 2. Stagger Ubuntu Starts:
Start Ubuntu workers 30 seconds apart to avoid overwhelming Naver.

### 3. Monitor Resource Usage:
```bash
# On Mac/Ubuntu
htop  # Watch CPU and RAM
```

### 4. Run Ubuntu in Background:
```bash
nohup python multi_worker.py http://192.168.1.100:5000 --workers 3 > worker.log 2>&1 &
```

### 5. Use Screen for Persistence:
```bash
# On Ubuntu
screen -S scraper
python multi_worker.py http://192.168.1.100:5000 --workers 3
# Ctrl+A, D to detach
```

---

## 🚨 Troubleshooting

### Workers can't get jobs:
```bash
# Check coordinator is running
curl http://192.168.1.100:5000/health

# Check job queue
curl http://192.168.1.100:5000/stats
```

### Too many workers crashing:
Reduce worker count:
```bash
# Instead of --workers 6, use --workers 4
python multi_worker.py http://localhost:5000 --workers 4
```

### Mac getting slow:
```bash
# Check resource usage
top

# Reduce Mac workers
python mac_launcher.py data/facilities.parquet --workers 3
```

---

## 📦 File Checklist

**On Mac:**
- ✅ coordinator_lightweight.py
- ✅ multi_worker.py
- ✅ mac_launcher.py
- ✅ naver_review_scraper.py
- ✅ utils/frame_switch.py
- ✅ data/facilities.parquet

**On Each Ubuntu:**
- ✅ multi_worker.py
- ✅ naver_review_scraper.py
- ✅ utils/frame_switch.py

---

## ⚡ Speed Comparison

| Setup | Workers | Time for 1,000 | Time for 10,000 |
|-------|---------|----------------|-----------------|
| Single Machine | 1 | ~3.5 hours | ~35 hours |
| Old (Ubuntu only) | 4 | ~1 hour | ~10 hours |
| **New (Mac + Ubuntu)** | **10** | **~20 min** | **~3.5 hours** |

**50X faster than single machine!**

---

## 🎯 Recommended Configurations

### Conservative (8GB RAM each):
- Mac: 2 workers
- Ubuntu 1: 2 workers  
- Ubuntu 2: 2 workers
- **Total: 6 workers**

### Balanced (16GB RAM each):
- Mac: 4 workers
- Ubuntu 1: 3 workers
- Ubuntu 2: 3 workers
- **Total: 10 workers** ← RECOMMENDED

### Aggressive (32GB+ RAM):
- Mac: 6 workers
- Ubuntu 1: 4 workers
- Ubuntu 2: 4 workers
- **Total: 14 workers**

---

**Ready to scrape at lightning speed! ⚡🚀**
