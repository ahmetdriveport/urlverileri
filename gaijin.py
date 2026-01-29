#!/usr/bin/env python3
import os, time, logging, csv
from datetime import datetime, UTC
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
MAX_ROWS = 5
DATES_FILE = os.path.join(os.path.dirname(__file__), "data", "dates.csv")
OUTPUT_FILE = "gaijin.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# -------------------------
# Selenium ile cookie alma
# -------------------------
def get_cookies_with_selenium(path, headless, url):
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

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(60)

    for attempt in range(3):
        try:
            driver.get(url)
            time.sleep(2)
            cookies = driver.get_cookies()
            driver.quit()
            return cookies
        except TimeoutException:
            logger.warning(f"Timeout (deneme {attempt+1}), tekrar deneniyor...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Selenium hata: {e}")
            time.sleep(5)
    driver.quit()
    raise RuntimeError("❌ Cookie alınamadı")

def cookie_header_from_list(cookies):
    return "; ".join([f"{x['name']}={x['value']}" for x in cookies])

def safe_post(session, url, payload, headers, n=3, backoff=1.0):
    for attempt in range(n):
        try:
            return session.post(url, json=payload, headers=headers, timeout=30, verify=certifi.where())
        except Exception as e:
            logger.error(f"POST hata: {e}")
            time.sleep(backoff * (attempt + 1))
    return None

def date_str_to_dt(s):
    s = s.strip().split()[0].replace("/", ".").replace("-", ".")
    for fmt in ("%d.%m.%Y", "%Y.%m.%d", "%d.%m.%y"):
        try:
            return datetime.strptime(s, fmt)
        except:
            continue
    raise ValueError(f"bad date {s}")

# 🔑 Tarih ve hisse kodlarını birlikte oku
def load_dates_and_hisses(path=DATES_FILE):
    dates = []
    hisses = set()
    with open(path, "r", encoding="utf-8") as f:
        for row in csv.reader(f):
            if row and row[0].strip():
                try:
                    dt = date_str_to_dt(row[0].strip())
                    if dt.date() <= datetime.now(UTC).date():
                        dates.append(dt)
                        if len(row) > 1 and row[1].strip():
                            hisses.add(row[1].strip())
                except:
                    pass
    return sorted(dates, reverse=True), sorted(hisses)

def fetch_for_target_range(session, start, end, endeks="09"):
    payload = {"baslangicTarih": start, "bitisTarihi": end, "sektor": None, "endeks": endeks, "hisse": None}
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.isyatirim.com.tr",
        "Referer": BASE_PAGE,
        "User-Agent": USER_AGENT,
        "Cookie": session.headers.get("Cookie", "")
    }
    r = safe_post(session, AJAX_URL, payload, headers)
    if r and r.ok:
        try:
            return r.json().get("d", [])
        except:
            pass
    return []

def main():
    dates, hisses = load_dates_and_hisses(DATES_FILE)
    session = requests.Session()
    cookies = get_cookies_with_selenium(CHROMEDRIVER_PATH, HEADLESS, BASE_PAGE)
    session.headers.update({"Cookie": cookie_header_from_list(cookies), "User-Agent": USER_AGENT})

    all_data = []
    cnt = 0
    for i, dt in enumerate(dates):
        end = dt.strftime("%d-%m-%Y")
        for off in (3, 10, 20):
            if i + off < len(dates):
                start = dates[i + off].strftime("%d-%m-%Y")
                recs = fetch_for_target_range(session, start, end)
                if recs:
                    for r in recs:
                        r["Tarih"] = end
                    all_data += recs
                    cnt += 1
                    break
        if cnt >= MAX_ROWS:
            break

    if all_data:
        df = pd.DataFrame(all_data)
        cols = [c for c in ["Tarih", "HISSE_KODU", "YAB_ORAN_END"] if c in df.columns]
        df = df[cols].rename(columns={"HISSE_KODU": "Kod", "YAB_ORAN_END": "Yabancı Oran"})
        if "Kod" in df.columns and "Yabancı Oran" in df.columns:
            dfp = df.pivot_table(index="Tarih", columns="Kod", values="Yabancı Oran", aggfunc="first").sort_index()
            # 🔑 Hisse filtreleme
            if hisses:
                dfp = dfp[[c for c in dfp.columns if c in hisses]]
            dfp.to_csv(OUTPUT_FILE, encoding="utf-8")
            logger.info(f"{dfp.shape} tablo {OUTPUT_FILE} yazıldı (filtre: {len(hisses)} hisse)")
        else:
            logger.warning("Beklenen kolonlar eksik, pivot yapılamadı.")
    else:
        logger.warning("❌ Veri yok")

def print_artifact_link():
    repo = os.getenv("GITHUB_REPOSITORY")
    run_id = os.getenv("GITHUB_RUN_ID")
    if repo and run_id:
        print(f"Artifact link: https://github.com/{repo}/actions/runs/{run_id}")
    else:
        print("Artifact link bilgisi bulunamadı.")

if __name__ == "__main__":
    main()
    print_artifact_link()
