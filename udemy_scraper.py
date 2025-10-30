# -*- coding: utf-8 -*-
"""
Script cào Udemy, được tham số hóa để chạy trên GitHub Actions.
Nhận lệnh (category, trang bắt đầu, trang kết thúc) từ argparse.
Tự động lưu Parquet và upload lên Azure Blob Storage.
"""

import json, time, random, pprint, os, datetime
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import pandas as pd
import argparse # Thư viện nhận lệnh
from azure.storage.blob import BlobServiceClient # Thư viện Azure

# =========================
# SETTINGS
# =========================
PAGELOAD_TIMEOUT = 60
RETRY = 2
SCROLL_STEPS = 12 # Dùng làm mốc cho _human_scroll

# =========================
# HÀM HỖ TRỢ (JITTER, DRIVER, SCROLL, v.v.)
# =========================

def _jitter(a=0.4, b=0.9):
    time.sleep(random.uniform(a, b))

def _jitter_long(a=5.0, b=10.0):
    print(f"[human] ...Đang 'đọc' trang, chờ {a}-{b} giây...")
    time.sleep(random.uniform(a, b))

def _init_driver(headless=False):
    opts = uc.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-dev-shm-usage")
    opts.set_capability("pageLoadStrategy", "eager")

    current_dir = os.getcwd()
    profile_path = os.path.join(current_dir, "udemy_profile")
    print(f"[driver] Sử dụng profile tại: {profile_path}")
    opts.add_argument(f"--user-data-dir={profile_path}")

    # === SỬA LỖI PHIÊN BẢN CHROME ===
    # Đọc phiên bản Chrome đã được GHA phát hiện
    detected_version_str = os.environ.get("INSTALLED_CHROME_VERSION")
    version_main = None
    if detected_version_str and detected_version_str.isdigit():
        version_main = int(detected_version_str)
        print(f"[driver] Đã phát hiện GHA Chrome v{version_main}. Sẽ ép 'uc' dùng phiên bản này.")
    # === KẾT THÚC SỬA LỖI ===

    driver = uc.Chrome(
        options=opts,
        version_main=version_main # <-- DÒNG QUAN TRỌNG NHẤT
    )

    driver.set_page_load_timeout(PAGELOAD_TIMEOUT)
    return driver

def _human_scroll(driver, base_steps=SCROLL_STEPS):
    print("[scroll] Bắt đầu cuộn trang tự nhiên...")
    steps = random.randint(base_steps - 2, base_steps + 3)
    for i in range(steps):
        scroll_amount = random.randint(400, 800)
        driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
        print(f"  [scroll] ...lần {i+1}/{steps} (cuộn {scroll_amount}px)")
        time.sleep(random.uniform(0.3, 0.8))
    
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
    _jitter()

def _extract_course_links_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = set()
    for a in soup.select('a[href*="/course/"]'):
        href = a.get("href") or ""
        if "/course/" in href:
            if href.startswith("/"):
                href = "https://www.udemy.com" + href
            href = href.split("?")[0].rstrip("/") + "/"
            if href.startswith("https://www.udemy.com/course/"):
                links.add(href)
    return sorted(links)

def _safe_get(driver, url):
    for i in range(RETRY + 1):
        try:
            driver.get(url)
            return True
        except (TimeoutException, WebDriverException):
            if i == RETRY:
                print(f"  ↳ ERROR: Không thể mở URL sau {RETRY} lần thử.")
                return False
            print("  ↳ WARN: Lỗi, đang chờ và thử lại...")
            _jitter(0.8, 1.6)
    return False

# =========================
# HÀM LƯU TRỮ VÀ UPLOAD
# =========================

def _save_to_parquet(data: List[Dict], filename: str):
    if not data:
        print("\n[save] Không có dữ liệu để lưu file Parquet.")
        return
        
    print(f"\n[save] Bắt đầu chuẩn hóa (normalize) {len(data)} dòng dữ liệu...")
    try:
        # Tạo thư mục nếu nó là một đường dẫn (vd: "data/file.parquet")
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        df = pd.json_normalize(data)

        # Chuyển đổi cột list/dict sang chuỗi JSON để Parquet lưu trữ
        for col in df.columns:
            if df[col].apply(type).eq(list).any():
                print(f"[save] ...Chuyển đổi cột list '{col}' sang chuỗi JSON")
                df[col] = df[col].apply(json.dumps)
            if df[col].apply(type).eq(dict).any():
                print(f"[save] ...Chuyển đổi cột dict '{col}' sang chuỗi JSON")
                df[col] = df[col].apply(json.dumps)

        df.to_parquet(filename, compression='gzip', index=False)
        print(f"[save] Đã lưu {len(df)} kết quả vào file Parquet: {filename}")
        
    except ImportError:
        print("\n[save] ❌ Lỗi: Cần cài 'pandas' và 'pyarrow'.")
    except Exception as e:
        print(f"\n[save] ❌ Lỗi khi lưu file Parquet: {e}")

def _upload_to_blob(connection_string: str, container_name: str, file_path: str, blob_name: str):
    try:
        print(f"\n[azure] Đang kết nối tới Azure Blob Storage...")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        print(f"[azure] Đang tải file: {file_path} lên container '{container_name}' với tên: {blob_name}")
        
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            
        print(f"[azure] Tải lên THÀNH CÔNG.")
    except ImportError:
        print("\n[azure] ❌ Lỗi: Cần cài 'azure-storage-blob'.")
    except Exception as e:
        print(f"\n[azure] ❌ Lỗi khi tải file lên Azure: {e}")


# =========================
# 1) GET COURSE URLS (ONE PAGE)
# =========================
def get_course_urls_per_page(listing_url: str, headless: bool = False) -> List[str]:
    driver = _init_driver(headless=headless)
    print(f"[driver] Init Chrome (headless={headless})")
    try:
        ok = _safe_get(driver, listing_url)
        if not ok:
            print("[category] ERROR: cannot open listing URL")
            return []

        try:
            WebDriverWait(driver, PAGELOAD_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except TimeoutException:
            pass

        try:
            driver.add_cookie({"name": "ud_locale", "value": "en_US", "path": "/", "domain": ".udemy.com"})
            driver.add_cookie({"name": "seen", "value": "1", "path": "/", "domain": ".udemy.com"})
            driver.refresh()
            _jitter()
        except Exception:
            pass

        _human_scroll(driver, base_steps=SCROLL_STEPS)
        
        print("[wait] Chờ cho các thẻ link course xuất hiện...")
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'a[href*="/course/"]'))
            )
            print("[wait] ...Đã thấy link. Lấy source.")
        except TimeoutException:
            # === THÊM DEBUG VÀO ĐÂY ===
            print("[debug] ❌ LỖI: Không tìm thấy link. Đang chụp ảnh màn hình...")
            try:
                # Tạo thư mục 'artifacts' nếu chưa có
                os.makedirs("artifacts", exist_ok=True)
                debug_time = datetime.datetime.now().strftime("%H%M%S")
                screenshot_file = f"artifacts/debug_screenshot_{debug_time}.png"
                html_file = f"artifacts/debug_page_{debug_time}.html"

                driver.save_screenshot(screenshot_file)
                with open(html_file, "w", encoding='utf-8') as f:
                    f.write(driver.page_source)
                print(f"[debug] Đã lưu {screenshot_file} và {html_file}")
            except Exception as e:
                print(f"[debug] Lỗi khi lưu file debug: {e}")
            # === KẾT THÚC DEBUG ===
            
            print("[wait] ❌ WARN: Hết thời gian chờ, không thấy link course nào.")
            pass # Vẫn tiếp tục

        html = driver.page_source
        links = _extract_course_links_from_html(html)
        print(f"[category] Found {len(links)} course links")
        if links[:5]:
            print("[category] Sample:", " | ".join(links[:5]))
        return links
    finally:
        try:
            driver.quit()
            print("[driver] Closed listing driver")
        except Exception:
            pass

# =========================
# 2) PARSER (Bỏ num_lecture)
# =========================
def parse_course_details(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    body_tag = soup.find('body')
    if not (body_tag and 'data-module-args' in body_tag.attrs):
        print("❌ Lỗi: Không tìm thấy 'data-module-args' trong thẻ body.")
        return None

    try:
        data = json.loads(body_tag['data-module-args'])
    except json.JSONDecodeError:
        print("❌ Lỗi: Không thể phân tích JSON từ 'data-module-args'.")
        return None

    ssp = data.get('serverSideProps', {})
    cpp = data.get('componentProps', {})
    course_data = ssp.get('course', {}) or cpp.get('course', {})
    reviews_data = ssp.get('reviewsRatings', {}) or cpp.get('reviews', {})
    title = data.get('title') or course_data.get('title')

    instructors_list = course_data.get('instructors', {}).get('instructors_info', []) or cpp.get('instructors', [])
    all_instructors_details = []
    for instructor in instructors_list or []:
        is_ssp_structure = 'total_num_students' in instructor
        all_instructors_details.append({
            'instructor_id': instructor.get('id'),
            'name': instructor.get('display_name'),
            'job_title': instructor.get('job_title'),
            'num_students': instructor.get('total_num_students') if is_ssp_structure else instructor.get('num_students'),
            'avg_rating_score': instructor.get('avg_rating_recent') if is_ssp_structure else instructor.get('rating'),
            'num_of_courses': instructor.get('total_num_taught_courses') if is_ssp_structure else instructor.get('num_published_courses')
        })

    rating_distribution_list = reviews_data.get('ratingDistribution', [])
    rating_distribution = {f"{item['rating']}_star_count": item['count'] for item in rating_distribution_list}

    original_price, discount_price, num_sections = None, None, 0
    
    try:
        meta_price_tag = soup.find("meta", {"property": "udemy_com:price"})
        if meta_price_tag:
            price_content = meta_price_tag.get("content", "")
            cleaned_price = price_content.replace("₫", "").replace(",", "")
            if cleaned_price.isdigit():
                original_price = float(cleaned_price)
    except Exception:
        print("⚠️ Cảnh báo: Không thể lấy giá gốc (original_price) từ thẻ meta.")

    script_tag = soup.find("script", {"type": "application/ld+json"})
    if script_tag:
        try:
            json_ld_data = json.loads(script_tag.string)
            course_ld_info = {}
            graph_data = json_ld_data.get('@graph', [json_ld_data])
            for item in graph_data:
                if item and item.get('@type') == 'Course':
                    course_ld_info = item
                    break
            
            if course_ld_info.get('offers'):
                discount_price = course_ld_info.get('offers', [{}])[0].get('price')

            syllabus = course_ld_info.get('syllabusSections', [])
            num_sections = len(syllabus)

        except (json.JSONDecodeError, IndexError, TypeError):
            print("⚠️ Cảnh báo: Không thể lấy dữ liệu giá hoặc syllabus từ JSON-LD.")

    extracted_data = {
        'course_in4': {
            'course_id': data.get('course_id'),
            'title': title,
            'headline': course_data.get('headline'),
            'language': course_data.get('localeSimpleEnglishTitle'),
            'level': course_data.get('instructionalLevel') or course_data.get('instructional_level_simple'),
            'subtitle': ", ".join(course_data.get('captionedLanguages', [])),
            'course_duration_seconds': course_data.get('contentLengthVideo') or course_data.get('content_length_video'),
            'num_sections': num_sections,
            'publishes_date': course_data.get('publishedDate') or course_data.get('published_time'),
            'lasted_updated_date': course_data.get('lastUpdateDate') or course_data.get('last_update_date'),
            'original_price': original_price,
            'discount_price': discount_price,
        },
        'course_performance': {
            'num_students': course_data.get('numStudents') or course_data.get('num_students'),
            'num_reviews': course_data.get('numReviews') or course_data.get('num_reviews'),
            'avg_rating_score': course_data.get('rating'),
            'rating_distribution': rating_distribution,
        },
        'instruction_in4': {
            'all_instructors': all_instructors_details
        }
    }
    return extracted_data


# =========================
# 3) PARSE ALL COURSES (Thêm category, scraped_datetime)
# =========================
def parse_all_courses(course_urls: List[str], category_name: str, headless: bool = False) -> List[Dict]:
    results = []
    now_iso = datetime.datetime.now().isoformat()
    
    driver = _init_driver(headless=headless) 
    print(f"[driver] Init Chrome for detail (headless={headless})")
    try:
        for i, url in enumerate(course_urls, 1):
            print(f"[course] [{i}/{len(course_urls)}] ({category_name}) {url}")
            ok = _safe_get(driver, url)
            if not ok:
                print("  ↳ ERROR: cannot open course URL")
                continue

            try:
                WebDriverWait(driver, PAGELOAD_TIMEOUT).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                pass
            
            _jitter_long() 

            try:
                driver.execute_script("window.scrollTo(0, 250);"); _jitter()
                driver.execute_script("window.scrollTo(0, 0);"); _jitter()
            except Exception:
                pass

            html = driver.page_source
            parsed = parse_course_details(html)
            if not parsed:
                print("  ↳ WARN: Parser trả về None → skip")
                continue
            
            parsed["_url"] = url
            parsed["_category"] = category_name
            parsed["_scraped_datetime"] = now_iso
            
            results.append(parsed)
    finally:
        try:
            driver.quit()
            print("[driver] Closed detail driver")
        except Exception:
            pass
    return results

# =========================
# 4) KHỐI MAIN (ĐỌC LỆNH TỪ ARGPARSE)
# =========================
if __name__ == "__main__":
    
    # --- Thiết lập trình đọc lệnh ---
    parser = argparse.ArgumentParser(description="Udemy Scraper")
    parser.add_argument("--job-type", required=True, help="Thư mục gốc trên Blob (vd: 'full_dashboard' hoặc 'test_run')")
    parser.add_argument("--category-name", required=True, help="Tên category (vd: 'Software Testing')")
    parser.add_argument("--category-url", required=True, help="URL gốc của category")
    parser.add_argument("--start-page", type=int, default=1, help="Trang bắt đầu cào (default: 1)")
    parser.add_argument("--end-page", type=int, required=True, help="Trang KẾT THÚC cào (vd: 6)")
    
    args = parser.parse_args()

    # --- Lấy các biến từ môi trường (cho Github Actions) ---
    IS_HEADLESS = os.environ.get("HEADLESS", "false").lower() == "true"
    AZURE_CONN_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    AZURE_CONTAINER_NAME = "udemy-it" # Giả sử tên container của bạn là 'raw-data'

    print(f"--- BẮT ĐẦU JOB: {args.job_type} ---")
    print(f"--- CATEGORY: {args.category_name} ---")
    print(f"--- TRANG CẦN CÀO: {args.start_page} ĐẾN {args.end_page} ---")
    print(f"--- HEADLESS: {IS_HEADLESS} ---")

    all_urls_for_this_category = []
    
    # Vòng lặp: Lặp qua các trang trong phạm vi được chỉ định
    for page_num in range(args.start_page, args.end_page + 1):
        current_listing_url = f"{args.category_url}?p={page_num}"
        
        print(f"\n" + "="*70)
        print(f"--- [ĐANG XỬ LÝ TRANG {page_num}/{args.end_page}] ---")
        print(f"Đang lấy link từ: {current_listing_url}")
        
        urls_from_page = get_course_urls_per_page(current_listing_url, headless=IS_HEADLESS)
        
        if not urls_from_page:
            print(f"WARN: Không tìm thấy link nào ở trang {page_num}, có thể đã hết trang. Dừng category này.")
            break 
        
        all_urls_for_this_category.extend(urls_from_page)
        
        if page_num < args.end_page:
            print(f"[human] Đã xong trang {page_num}. Chờ 10-20 giây trước khi qua trang mới...")
            _jitter_long(10, 20)
            
    # --- GOM KẾT QUẢ VÀ PARSE ---
    unique_urls = sorted(list(set(all_urls_for_this_category)))
    
    print(f"\n" + "="*70)
    print(f"=== TỔNG KẾT CATEGORY: {args.category_name} ===")
    print(f"Đã thu thập {len(unique_urls)} links duy nhất.")
    print("Bắt đầu phân tích (parse) chi tiết...")
    print("="*70)
    
    parsed_rows = parse_all_courses(unique_urls, args.category_name, headless=IS_HEADLESS)

    # --- KẾT THÚC QUÁ TRÌNH CÀO ---
    print(f"\n\n" + "#"*70)
    print(f"### HOÀN TẤT CÀO CATEGORY: {args.category_name} ###")
    print(f"Tổng cộng cào được {len(parsed_rows)} khóa học.")
    print("#"*70)

    # === LƯU FILE VÀ UPLOAD ===
    if parsed_rows:
        file_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # Đặt tên file theo category và phân trang, vd: Software_Testing_p1_p6.parquet
        file_name = f"{args.category_name.replace(' ', '_')}_p{args.start_page}_p{args.end_page}_{file_timestamp}.parquet"
        
        local_file_path = f"{file_name}" 
        # Tên file trên Blob (có cả thư mục)
        blob_file_path = f"{args.job_type}/{args.category_name.replace(' ', '_')}/{file_name}"

        # 1. Lưu file Parquet xuống máy ảo GHA
        _save_to_parquet(parsed_rows, local_file_path)
        
        # 2. Upload file đó lên Azure Blob
        if AZURE_CONN_STRING:
            _upload_to_blob(AZURE_CONN_STRING, AZURE_CONTAINER_NAME, local_file_path, blob_file_path)
        else:
            print("\n[azure] ⚠️ WARN: Không tìm thấy AZURE_STORAGE_CONNECTION_STRING. Bỏ qua upload.")
            
    else:
        print("\n[save] Không có dữ liệu để lưu file.")


    print("\n=== HOÀN THÀNH JOB ===")

