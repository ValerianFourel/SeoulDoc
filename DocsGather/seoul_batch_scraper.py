#!/usr/bin/env python3
"""
Seoul Medical Facilities Batch Scraper V3
Features:
- Fresh browser for each dong (driver recreated)
- Parallel processing with distributed start points
- Dong considered complete when first keyword CSV has ≥40 entries
Structure: district/dong/keyword.json
"""

import json
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple
import pandas as pd
from multiprocessing import Pool
import traceback
import filelock
import csv

from naver_medical_scraper_v6 import NaverMedicalScraperV6

# Seoul administrative dongs data
seoul_administrative_dongs = {
    "Gangnam-gu": [
        "개포1동", "개포2동", "개포3동", "개포4동", "논현1동", "논현2동",
        "대치1동", "대치2동", "대치4동", "도곡1동", "도곡2동", "삼성1동",
        "삼성2동", "세곡동", "수서동", "신사동", "압구정동", "역삼1동",
        "역삼2동", "일원1동", "일원본동", "청담동"
    ],
    "Gangdong-gu": [
        "강일동", "고덕1동", "고덕2동", "길동", "둔촌1동", "둔촌2동",
        "명일1동", "명일2동", "상일1동", "상일2동", "성내1동", "성내2동",
        "성내3동", "암사1동", "암사2동", "암사3동", "천호1동", "천호2동", "천호3동"
    ],
    "Gangbuk-gu": [
        "미아동", "번1동", "번2동", "번3동", "삼각산동", "삼양동",
        "송중동", "송천동", "수유1동", "수유2동", "수유3동", "우이동", "인수동"
    ],
    "Gangseo-gu": [
        "가양1동", "가양2동", "가양3동", "공항동", "등촌1동", "등촌2동",
        "등촌3동", "발산1동", "방화1동", "방화2동", "방화3동", "염창동",
        "우장산동", "화곡1동", "화곡2동", "화곡3동", "화곡4동", "화곡6동",
        "화곡8동", "화곡본동"
    ],
    "Gwanak-gu": [
        "낙성대동", "난곡동", "난향동", "남현동", "대학동", "미성동",
        "보라매동", "삼성동", "서림동", "서원동", "성현동", "신림동",
        "신사동", "신원동", "은천동", "인헌동", "조원동", "중앙동",
        "청룡동", "청림동", "행운동"
    ],
    "Gwangjin-gu": [
        "광장동", "구의1동", "구의2동", "구의3동", "군자동", "능동",
        "자양1동", "자양2동", "자양3동", "자양4동", "중곡1동", "중곡2동",
        "중곡3동", "중곡4동", "화양동"
    ],
    "Guro-gu": [
        "가리봉동", "개봉1동", "개봉2동", "개봉3동", "고척1동", "고척2동",
        "구로1동", "구로2동", "구로3동", "구로4동", "구로5동", "수궁동",
        "신도림동", "오류1동", "오류2동", "항동"
    ],
    "Geumcheon-gu": [
        "가산동", "독산1동", "독산2동", "독산3동", "독산4동",
        "시흥1동", "시흥2동", "시흥3동", "시흥4동", "시흥5동"
    ],
    "Nowon-gu": [
        "공릉1동", "공릉2동", "상계1동", "상계2동", "상계3.4동", "상계5동",
        "상계6.7동", "상계8동", "상계9동", "상계10동", "월계1동", "월계2동",
        "월계3동", "중계본동", "중계1동", "중계2.3동", "중계4동", "하계1동", "하계2동"
    ],
    "Dobong-gu": [
        "도봉1동", "도봉2동", "방학1동", "방학2동", "방학3동",
        "쌍문1동", "쌍문2동", "쌍문3동", "쌍문4동",
        "창1동", "창2동", "창3동", "창4동", "창5동"
    ],
    "Dongdaemun-gu": [
        "답십리1동", "답십리2동", "용신동", "이문1동", "이문2동",
        "장안1동", "장안2동", "전농1동", "전농2동", "제기동",
        "청량리동", "회기동", "휘경1동", "휘경2동"
    ],
    "Dongjak-gu": [
        "노량진1동", "노량진2동", "대방동", "사당1동", "사당2동",
        "사당3동", "사당4동", "사당5동", "상도1동", "상도2동",
        "상도3동", "상도4동", "신대방1동", "신대방2동", "흑석동"
    ],
    "Mapo-gu": [
        "공덕동", "대흥동", "도화동", "망원1동", "망원2동", "상암동",
        "서강동", "서교동", "성산1동", "성산2동", "신수동", "아현동",
        "연남동", "염리동", "용강동", "합정동"
    ],
    "Seodaemun-gu": [
        "남가좌1동", "남가좌2동", "북가좌1동", "북가좌2동", "북아현동",
        "신촌동", "연희동", "천연동", "충현동", "홍은1동", "홍은2동",
        "홍제1동", "홍제2동", "홍제3동"
    ],
    "Seocho-gu": [
        "내곡동", "반포본동", "반포1동", "반포2동", "반포3동", "반포4동",
        "방배본동", "방배1동", "방배2동", "방배3동", "방배4동",
        "서초1동", "서초2동", "서초3동", "서초4동", "양재1동", "양재2동", "잠원동"
    ],
    "Seongdong-gu": [
        "금호1가동", "금호2.3가동", "금호4가동", "마장동", "사근동",
        "성수1가1동", "성수1가2동", "성수2가1동", "성수2가3동", "송정동",
        "옥수동", "왕십리도선동", "왕십리2동", "용답동", "응봉동",
        "행당1동", "행당2동"
    ],
    "Seongbuk-gu": [
        "길음1동", "길음2동", "돈암1동", "돈암2동", "동선동", "보문동",
        "삼선동", "석관동", "성북동", "안암동", "월곡1동", "월곡2동",
        "장위1동", "장위2동", "장위3동", "정릉1동", "정릉2동",
        "정릉3동", "정릉4동", "종암동"
    ],
    "Songpa-gu": [
        "가락본동", "가락1동", "가락2동", "거여1동", "거여2동",
        "마천1동", "마천2동", "문정1동", "문정2동", "방이1동", "방이2동",
        "삼전동", "석촌동", "송파1동", "송파2동", "오금동", "오륜동",
        "위례동", "잠실본동", "잠실2동", "잠실3동", "잠실4동",
        "잠실6동", "잠실7동", "장지동", "풍납1동", "풍납2동"
    ],
    "Yangcheon-gu": [
        "목1동", "목2동", "목3동", "목4동", "목5동",
        "신월1동", "신월2동", "신월3동", "신월4동", "신월5동", "신월6동", "신월7동",
        "신정1동", "신정2동", "신정3동", "신정4동", "신정6동", "신정7동"
    ],
    "Yeongdeungpo-gu": [
        "당산1동", "당산2동", "대림1동", "대림2동", "대림3동", "도림동",
        "문래동", "신길1동", "신길3동", "신길4동", "신길5동", "신길6동", "신길7동",
        "양평1동", "양평2동", "여의동", "영등포본동", "영등포동"
    ],
    "Yongsan-gu": [
        "남영동", "보광동", "서빙고동", "용문동", "용산2가동",
        "원효로1동", "원효로2동", "이촌1동", "이촌2동",
        "이태원1동", "이태원2동", "청파동", "한강로동", "한남동",
        "효창동", "후암동"
    ],
    "Eunpyeong-gu": [
        "갈현1동", "갈현2동", "구산동", "녹번동", "대조동",
        "불광1동", "불광2동", "수색동", "신사1동", "신사2동",
        "역촌동", "응암1동", "응암2동", "응암3동", "증산동", "진관동"
    ],
    "Jongno-gu": [
        "가회동", "교남동", "무악동", "부암동", "사직동", "삼청동",
        "숭인1동", "숭인2동", "이화동", "종로1.2.3.4가동", "종로5.6가동",
        "창신1동", "창신2동", "창신3동", "청운효자동", "평창동", "혜화동"
    ],
    "Jung-gu": [
        "광희동", "다산동", "동화동", "명동", "소공동", "신당동", "신당5동",
        "약수동", "을지로동", "장충동", "중림동", "청구동", "필동",
        "황학동", "회현동"
    ],
    "Jungnang-gu": [
        "망우본동", "망우3동", "면목본동", "면목2동", "면목3.8동",
        "면목4동", "면목5동", "면목7동", "묵1동", "묵2동",
        "상봉1동", "상봉2동", "신내1동", "신내2동", "중화1동", "중화2동"
    ]
}


def count_csv_rows(csv_path: Path) -> int:
    """Count rows in CSV file (excluding header)"""
    try:
        if not csv_path.exists():
            return 0
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            return sum(1 for row in reader)
    except:
        return 0


def is_dong_complete(output_dir: Path, gu: str, dong: str, first_keyword: str = '병원', min_entries: int = 40) -> bool:
    """
    Check if dong is complete by checking first keyword CSV has >= min_entries
    
    Args:
        output_dir: Base output directory
        gu: District name
        dong: Dong name
        first_keyword: First keyword to check (default: 병원)
        min_entries: Minimum entries required (default: 40)
    
    Returns:
        True if CSV exists with >= min_entries rows
    """
    csv_path = output_dir / gu / dong / f"{first_keyword}.csv"
    row_count = count_csv_rows(csv_path)
    return row_count >= min_entries


def scrape_single_dong(task: Dict, output_dir: str, headless: bool, max_pages: int, min_entries: int = 40) -> Dict:
    """
    Scrape a single dong with all keywords
    Creates fresh browser for this dong only
    Dong is complete when first keyword CSV has >= min_entries
    
    Args:
        task: Dict with 'gu', 'dong', 'keywords'
        output_dir: Base output directory
        headless: Run headless
        max_pages: Max pages per keyword
        min_entries: Minimum entries to consider complete
    
    Returns:
        Dict with results summary
    """
    gu = task['gu']
    dong = task['dong']
    keywords = task['keywords']
    
    output_path = Path(output_dir)
    results_summary = {
        'gu': gu,
        'dong': dong,
        'completed_keywords': [],
        'failed_keywords': [],
        'total_facilities': 0,
        'start_time': datetime.now().isoformat(),
        'end_time': None,
        'error': None
    }
    
    print(f"\n{'='*70}")
    print(f"🔄 Starting NEW browser for: {gu} > {dong}")
    print(f"   Keywords: {', '.join(keywords)}")
    print(f"{'='*70}")
    
    # Create fresh browser for this dong
    scraper = None
    try:
        scraper = NaverMedicalScraperV6(headless=headless)
        
        for keyword in keywords:
            print(f"\n{'─'*70}")
            print(f"📍 {gu} > {dong} > {keyword}")
            print(f"{'─'*70}")
            
            try:
                # Scrape this keyword
                results = scraper.scrape_location(
                    query=keyword,
                    location=dong,
                    max_pages=max_pages
                )
                
                # Save results
                district_dir = output_path / gu
                dong_dir = district_dir / dong
                dong_dir.mkdir(parents=True, exist_ok=True)
                
                json_path = dong_dir / f"{keyword}.json"
                csv_path = dong_dir / f"{keyword}.csv"
                
                if results:
                    # Save JSON
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(results, f, ensure_ascii=False, indent=2)
                    
                    # Save CSV
                    try:
                        scraper.save_to_csv(results, str(csv_path))
                        
                        # Check if we have enough entries for first keyword
                        if keyword == keywords[0]:
                            row_count = count_csv_rows(csv_path)
                            print(f"    📊 First keyword CSV: {row_count} entries")
                            if row_count >= min_entries:
                                print(f"    ✅ Reached minimum {min_entries} entries - dong considered complete!")
                    except Exception as csv_err:
                        print(f"    ⚠️  CSV save warning: {csv_err}")
                    
                    print(f"\n✅ Saved {len(results)} results for {keyword}")
                    results_summary['total_facilities'] += len(results)
                else:
                    # Save empty file
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump([], f)
                    print(f"\n⚠️  No results for {keyword}")
                
                results_summary['completed_keywords'].append(keyword)
                
            except Exception as kw_error:
                print(f"\n❌ Error for {keyword}: {kw_error}")
                print(traceback.format_exc())
                results_summary['failed_keywords'].append(keyword)
                
                # Save empty file to mark as attempted
                try:
                    dong_dir.mkdir(parents=True, exist_ok=True)
                    json_path = dong_dir / f"{keyword}.json"
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump([], f)
                except:
                    pass
        
        results_summary['end_time'] = datetime.now().isoformat()
        print(f"\n✅ Completed {gu} > {dong}")
        print(f"   Total facilities: {results_summary['total_facilities']}")
        
    except Exception as e:
        results_summary['error'] = str(e)
        results_summary['end_time'] = datetime.now().isoformat()
        print(f"\n❌ Fatal error for {gu} > {dong}: {e}")
        print(traceback.format_exc())
    
    finally:
        # ALWAYS close browser for this dong
        if scraper:
            try:
                scraper.close()
                print(f"🔚 Closed browser for {gu} > {dong}")
            except:
                pass
    
    return results_summary


class SeoulMedicalBatchScraperV3:
    """
    Batch scraper with parallel processing and fresh browsers per dong
    """
    
    def __init__(self, output_dir: str = 'seoul_medical_data', min_entries: int = 40):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.progress_file = self.output_dir / 'progress.json'
        self.progress_lock_file = self.output_dir / 'progress.json.lock'
        
        self.keywords = ['병원', '의원', '클리닉']
        self.min_entries = min_entries
        
        self.total_dongs = sum(len(dongs) for dongs in seoul_administrative_dongs.values())
        self.total_tasks = self.total_dongs * len(self.keywords)
        
        print(f"\n{'='*60}")
        print(f"Seoul Medical Facilities Batch Scraper V3")
        print(f"{'='*60}")
        print(f"Districts: {len(seoul_administrative_dongs)}")
        print(f"Dongs: {self.total_dongs}")
        print(f"Keywords: {', '.join(self.keywords)}")
        print(f"Total tasks: {self.total_tasks}")
        print(f"Completion criteria: First keyword CSV ≥ {self.min_entries} entries")
        print(f"✨ Features: Fresh browser per dong, parallel processing")
        print(f"{'='*60}\n")
    
    def _load_progress(self) -> Dict:
        """Load progress with file locking"""
        lock = filelock.FileLock(str(self.progress_lock_file))
        
        try:
            with lock.acquire(timeout=10):
                if self.progress_file.exists():
                    with open(self.progress_file, 'r', encoding='utf-8') as f:
                        return json.load(f)
        except:
            pass
        
        return {
            'completed_dongs': [],
            'statistics': {
                'total_dongs_completed': 0,
                'total_facilities': 0,
                'by_keyword': {}
            },
            'start_time': datetime.now().isoformat()
        }
    
    def _save_progress(self, dong_summary: Dict):
        """Save progress with file locking"""
        lock = filelock.FileLock(str(self.progress_lock_file))
        
        try:
            with lock.acquire(timeout=10):
                # Load current progress
                progress_data = self._load_progress()
                
                # Add this dong
                dong_key = f"{dong_summary['gu']}_{dong_summary['dong']}"
                if dong_key not in progress_data['completed_dongs']:
                    progress_data['completed_dongs'].append(dong_key)
                
                # Update statistics
                progress_data['statistics']['total_dongs_completed'] = len(progress_data['completed_dongs'])
                progress_data['statistics']['total_facilities'] = \
                    progress_data['statistics'].get('total_facilities', 0) + dong_summary['total_facilities']
                
                for keyword in dong_summary['completed_keywords']:
                    if keyword not in progress_data['statistics']['by_keyword']:
                        progress_data['statistics']['by_keyword'][keyword] = 0
                    progress_data['statistics']['by_keyword'][keyword] += \
                        dong_summary['total_facilities'] // max(len(dong_summary['completed_keywords']), 1)
                
                progress_data['last_updated'] = datetime.now().isoformat()
                progress_data['completion_percentage'] = \
                    (len(progress_data['completed_dongs']) / self.total_dongs * 100)
                
                # Save
                with open(self.progress_file, 'w', encoding='utf-8') as f:
                    json.dump(progress_data, f, ensure_ascii=False, indent=2)
        
        except Exception as e:
            print(f"⚠️  Progress save error: {e}")
    
    def _is_dong_completed(self, gu: str, dong: str) -> bool:
        """
        Check if dong is completed by checking:
        1. Progress file (tracked completion)
        2. First keyword CSV exists with >= min_entries rows
        """
        # Check progress file first
        progress_data = self._load_progress()
        dong_key = f"{gu}_{dong}"
        if dong_key in progress_data.get('completed_dongs', []):
            return True
        
        # Check if CSV exists with enough entries
        return is_dong_complete(self.output_dir, gu, dong, self.keywords[0], self.min_entries)
    
    def show_progress(self):
        """Display progress"""
        progress_data = self._load_progress()
        
        completed = len(progress_data.get('completed_dongs', []))
        percentage = completed / self.total_dongs * 100
        
        print(f"\n{'='*60}")
        print(f"PROGRESS STATUS")
        print(f"{'='*60}")
        print(f"Completed dongs: {completed}/{self.total_dongs} ({percentage:.1f}%)")
        print(f"Completion criteria: First keyword CSV ≥ {self.min_entries} entries")
        
        stats = progress_data.get('statistics', {})
        if stats:
            print(f"\nTotal facilities: {stats.get('total_facilities', 0):,}")
            
            by_keyword = stats.get('by_keyword', {})
            if by_keyword:
                print(f"\nBy keyword:")
                for keyword, count in by_keyword.items():
                    print(f"  {keyword}: {count:,}")
        
        print(f"{'='*60}\n")
    
    def scrape_all_seoul(self, headless: bool = True, max_pages: int = 10, 
                        first_page_only: bool = False, workers: int = 1):
        """
        Scrape all Seoul with parallel processing
        Workers start at different points for better distribution
        
        Args:
            headless: Run headless
            max_pages: Max pages per location
            first_page_only: Only first page
            workers: Number of parallel workers (1 = sequential)
        """
        if first_page_only:
            max_pages = 1
            print(f"\n⚡ FIRST PAGE ONLY MODE")
        
        if workers > 1:
            print(f"\n🚀 PARALLEL MODE: {workers} workers")
            print(f"   Workers will start at different points for even distribution")
        
        # Collect pending dongs
        pending_dongs = []
        
        for gu, dongs in seoul_administrative_dongs.items():
            for dong in dongs:
                if not self._is_dong_completed(gu, dong):
                    pending_dongs.append({
                        'gu': gu,
                        'dong': dong,
                        'keywords': self.keywords
                    })
        
        print(f"\n{'='*60}")
        print(f"TASK SUMMARY")
        print(f"{'='*60}")
        print(f"Total dongs: {self.total_dongs}")
        print(f"Completed: {self.total_dongs - len(pending_dongs)}")
        print(f"Pending: {len(pending_dongs)}")
        print(f"Max pages: {max_pages}")
        print(f"Workers: {workers}")
        print(f"{'='*60}\n")
        
        if not pending_dongs:
            print("🎉 All dongs completed!")
            return
        
        # Distribute work across workers
        if workers > 1:
            # Split pending_dongs into chunks for each worker
            worker_chunks = [[] for _ in range(workers)]
            for idx, dong in enumerate(pending_dongs):
                worker_chunks[idx % workers].append(dong)
            
            print(f"📊 Work distribution:")
            for i, chunk in enumerate(worker_chunks):
                if chunk:
                    first_dong = f"{chunk[0]['gu']}/{chunk[0]['dong']}"
                    print(f"   Worker {i+1}: {len(chunk)} dongs (starting: {first_dong})")
            print()
        
        # Process dongs
        if workers == 1:
            # Sequential
            for idx, task in enumerate(pending_dongs, 1):
                print(f"\n{'#'*70}")
                print(f"Dong {idx}/{len(pending_dongs)}")
                print(f"{'#'*70}")
                
                summary = scrape_single_dong(task, str(self.output_dir), headless, max_pages, self.min_entries)
                self._save_progress(summary)
                
                completed = self.total_dongs - len(pending_dongs) + idx
                pct = completed / self.total_dongs * 100
                print(f"\n📊 Overall: {completed}/{self.total_dongs} ({pct:.1f}%)")
        
        else:
            # Parallel with distributed start points
            print(f"🚀 Starting {workers} parallel workers at different points...\n")
            
            from functools import partial
            scrape_func = partial(
                scrape_single_dong,
                output_dir=str(self.output_dir),
                headless=headless,
                max_pages=max_pages,
                min_entries=self.min_entries
            )
            
            with Pool(processes=workers) as pool:
                for idx, summary in enumerate(pool.imap_unordered(scrape_func, pending_dongs), 1):
                    self._save_progress(summary)
                    
                    completed = self.total_dongs - len(pending_dongs) + idx
                    pct = completed / self.total_dongs * 100
                    print(f"\n📊 [{summary['gu']}/{summary['dong']}] Overall: {completed}/{self.total_dongs} ({pct:.1f}%)")
        
        print(f"\n{'='*60}")
        print(f"✅ ALL DONGS COMPLETED!")
        print(f"{'='*60}\n")
    
    def get_statistics(self):
        """Show statistics"""
        self.show_progress()
        
        total_json = len(list(self.output_dir.rglob('*.json'))) - 1
        total_csv = len(list(self.output_dir.rglob('*.csv')))
        
        print(f"\nFiles: {total_json} JSON, {total_csv} CSV")
        
        # Check completion status
        print(f"\nCompletion check (first keyword CSV ≥ {self.min_entries} entries):")
        complete_count = 0
        for gu, dongs in seoul_administrative_dongs.items():
            for dong in dongs:
                if is_dong_complete(self.output_dir, gu, dong, self.keywords[0], self.min_entries):
                    complete_count += 1
        
        print(f"  Dongs with ≥{self.min_entries} entries: {complete_count}/{self.total_dongs}")
    
    def merge_results(self):
        """Merge all results"""
        print(f"\n📁 Merging results...")
        
        all_json_files = [f for f in self.output_dir.rglob('*.json') 
                          if f.name not in ['progress.json']]
        
        if not all_json_files:
            print("No files to merge.")
            return
        
        all_data = []
        for json_file in all_json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if data:
                        relative_path = json_file.relative_to(self.output_dir)
                        for item in data:
                            item['file_district'] = relative_path.parts[0]
                            item['file_dong'] = relative_path.parts[1]
                            item['file_keyword'] = relative_path.stem
                        all_data.extend(data)
            except:
                pass
        
        if not all_data:
            print("No data to merge.")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        merged_json = self.output_dir / f'_merged_all_{timestamp}.json'
        with open(merged_json, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        
        merged_csv = self.output_dir / f'_merged_all_{timestamp}.csv'
        df = pd.DataFrame(all_data)
        df.to_csv(merged_csv, index=False, encoding='utf-8-sig')
        
        print(f"\n✅ Merged!")
        print(f"   Rows: {len(all_data):,}")
        print(f"   Unique: {df['place_id'].nunique():,}")


def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Seoul Medical Batch Scraper V3 - Fresh browsers + Parallel + Smart completion',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sequential (1 dong at a time)
  python seoul_batch_scraper.py --headless
  
  # Parallel (4 dongs at once, start at different points)
  python seoul_batch_scraper.py --headless --workers 4
  
  # Fast parallel mode
  python seoul_batch_scraper.py --headless --first-page-only --workers 4
  
  # Custom minimum entries
  python seoul_batch_scraper.py --headless --min-entries 30
  
  # Progress
  python seoul_batch_scraper.py --progress
  
  # Merge
  python seoul_batch_scraper.py --merge
        """
    )
    
    parser.add_argument('--output-dir', default='seoul_medical_data')
    parser.add_argument('--headless', action='store_true')
    parser.add_argument('--max-pages', type=int, default=10)
    parser.add_argument('--first-page-only', action='store_true')
    parser.add_argument('--workers', type=int, default=1,
                       help='Number of parallel workers (1-8)')
    parser.add_argument('--min-entries', type=int, default=40,
                       help='Minimum CSV entries to consider dong complete (default: 40)')
    parser.add_argument('--progress', action='store_true')
    parser.add_argument('--stats', action='store_true')
    parser.add_argument('--merge', action='store_true')
    parser.add_argument('--test', action='store_true')
    
    args = parser.parse_args()
    
    scraper = SeoulMedicalBatchScraperV3(
        output_dir=args.output_dir,
        min_entries=args.min_entries
    )
    
    if args.progress or args.stats:
        scraper.show_progress()
        if args.stats:
            scraper.get_statistics()
        return
    
    if args.merge:
        scraper.merge_results()
        return
    
    if args.test:
        print("\n🧪 TEST MODE")
        task = {
            'gu': 'Gangnam-gu',
            'dong': '개포1동',
            'keywords': ['병원']
        }
        summary = scrape_single_dong(task, args.output_dir, False, 2, args.min_entries)
        print(f"\n✅ Test complete: {summary}")
        return
    
    # Run scraping
    scraper.scrape_all_seoul(
        headless=args.headless,
        max_pages=args.max_pages,
        first_page_only=args.first_page_only,
        workers=min(args.workers, 8)  # Max 8 workers
    )


if __name__ == "__main__":
    main()