#!/usr/bin/env python3
# yabanci_hamveri.py (CSV pivot versiyon)

import os, time, logging, csv
from datetime import datetime, UTC
from typing import List, Dict, Any, Optional

import requests, pandas as pd, certifi
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# -------------------------
# CONFIG
# -------------------------
CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", "")
HEADLESS = os.getenv("SELENIUM_HEADLESS", "true").lower() in ("1","true","yes")
USER_AGENT = "Mozilla/5.0"
AJAX_URL = "https://www.isyatirim.com.tr/_layouts/15/IsYatirim.Website/StockInfo/CompanyInfoAjax.aspx/GetYabanciOranlarXHR"
BASE_PAGE = "https://www.isyatirim.com.tr/tr-tr/analiz/hisse/Sayfalar/yabanci-oranlari.aspx"
MAX_ROWS = 5   # sadece 5 günlük veri
DATES_FILE = "data/dates.csv"
OUTPUT_FILE = "yabanci_oranlari.csv"
# -------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# --- Selenium ile cookie alma ---
def get_cookies_with_selenium(chrome_driver_path: Optional[str], headless: bool, url: str) -> List[Dict[str, Any]]:
    opts = Options()
    if headless:
        try:
            opts.add_argument("--headless=new")
        except:
            opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument(f"--user-agent={USER_AGENT}")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(60)

    for attempt in range(3):
        try:
            driver.get(url)
            time.sleep(2)
            cookies = driver.get_cookies()
            driver.quit()
            logger.info(f"✅ Cookie alındı (deneme {attempt+1}, {len(cookies)} adet)")
            return cookies
        except TimeoutException:
            logger.warning(f"⏱️ Timeout (deneme {attempt+1}), tekrar deneniyor...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Selenium hata (deneme {attempt+1}): {e}")
            time.sleep(5)
    driver.quit()
    raise RuntimeError("❌ Cookie alınamadı, Selenium başarısız.")

def cookie_header_from_list(cookies: List[Dict[str, Any]]) -> str:
    return "; ".join([f"{c['name']}={c['value']}" for c in cookies])

# --- Güvenli POST ---
def safe_post(session: requests.Session, url: str, json_payload: dict, headers: dict, max_retries: int = 3, backoff: float = 1.0):
    for attempt in range(1, max_retries+1):
        try:
            r = session.post(url, json=json_payload, headers=headers, timeout=30, verify=certifi.where())
            return r
        except Exception as e:
            logger.error(f"POST hatası (deneme {attempt}): {e}")
            time.sleep(backoff*attempt)
    return None

# --- Tarih parse ---
def date_str_to_dt(dstr: str) -> datetime:
    s = str(dstr).strip().split()[0]
    s = s.replace("/", ".").replace("-", ".")
    for fmt in ("%d.%m.%Y","%Y.%m.%d","%d.%m.%y"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    raise ValueError(f"Unrecognized date format: {dstr}")

# --- Tarih listesi CSV'den ---
def load_dates_from_csv(path: str = DATES_FILE) -> List[datetime]:
    dates = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if row and row[0].strip():
                try:
                    dt = date_str_to_dt(row[0].strip())
                    if dt.date() <= datetime.now(UTC).date():
                        dates.append(dt)
                except Exception as e:
                    logger.warning(f"Tarih parse hatası: {row[0]} ({e})")
    return sorted(dates, reverse=True)

# --- Veri çekme ---
def fetch_for_target_range(session: requests.Session, start_date: str, end_date: str, endeks: str = "09") -> List[Dict[str, Any]]:
    payload = {
        "baslangicTarih": start_date,
        "bitisTarihi": end_date,
        "sektor": None,
        "endeks": endeks,
        "hisse": None
    }
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.isyatirim.com.tr",
        "Referer": BASE_PAGE,
        "User-Agent": USER_AGENT,
        "Cookie": session.headers.get("Cookie","")
    }
    r = safe_post(session, AJAX_URL, payload, headers)
    if r and r.ok:
        try:
            return r.json().get("d", [])
        except Exception as e:
            logger.error(f"JSON parse hatası: {e}")
    else:
        logger.warning(f"Yanıt alınamadı: {start_date} - {end_date}")
    return []

# --- Ana akış ---
def main():
    # Tarih listesi artık CSV'den geliyor
    collected_dates = load_dates_from_csv(DATES_FILE)

    # Cookie al
    session = requests.Session()
    cookies = get_cookies_with_selenium(CHROMEDRIVER_PATH, HEADLESS, BASE_PAGE)
    session.headers.update({"Cookie": cookie_header_from_list(cookies), "User-Agent": USER_AGENT})

    # Veri çek
    all_data = []
    row_count = 0
    for i, dt in enumerate(collected_dates):
        end_date = dt.strftime("%d-%m-%Y")
        for offset in (3, 10, 20):
            if i+offset < len(collected_dates):
                start_date = collected_dates[i+offset].strftime("%d-%m-%Y")
                records = fetch_for_target_range(session, start_date, end_date)
                if records:
                    for rec in records:
                        rec["Tarih"] = end_date
                    all_data.extend(records)
                    row_count += 1
                    break
        if row_count >= MAX_ROWS:
            logger.info("5 günlük veri limiti aşıldı, durduruluyor.")
            break

    # Pivot tabloya dönüştür ve CSV'ye yaz
    if all_data:
        df = pd.DataFrame(all_data)
        df = df[["Tarih","HISSE_KODU","YAB_ORAN_END"]]
        df = df.rename(columns={
            "HISSE_KODU": "Kod",
            "YAB_ORAN_END": "Yabancı Oran"
        })

        df_pivot = df.pivot_table(
            index="Tarih",
            columns="Kod",
            values="Yabancı Oran",
            aggfunc="first"
        ).sort_index()

        df_pivot.to_csv(OUTPUT_FILE, encoding="utf-8")
        logger.info(f"✅ {df_pivot.shape[0]} tarih ve {df_pivot.shape[1]} hisse için tablo {OUTPUT_FILE} dosyasına yazıldı.")
    else:
        logger.warning("❌ Hiç veri bulunamadı.")

# 🔑 Script doğrudan çalıştırıldığında main() çağrılır
if __name__ == "__main__":
    main()
