#!/usr/bin/env python3
import os, time, logging, requests, pandas as pd, numpy as np, certifi
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH","")
HEADLESS = os.getenv("SELENIUM_HEADLESS","true").lower() in ("1","true","yes")
USER_AGENT = "Mozilla/5.0"
AJAX_URL = "https://www.isyatirim.com.tr/_layouts/15/IsYatirim.Website/StockInfo/CompanyInfoAjax.aspx/GetYabanciOranlarXHR"
BASE_PAGE = "https://www.isyatirim.com.tr/tr-tr/analiz/hisse/Sayfalar/yabanci-oranlari.aspx"
DATES_FILE = os.path.join(os.path.dirname(__file__),"data","dates.csv")
OUTPUT_FILE = "gaijin.csv"
MAX_ROWS = 5   # sadece 5 tarih aralığı işlenecek

logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def get_cookies_with_selenium(path,headless,url):
    opts = Options()
    if headless:
        try: opts.add_argument("--headless=new")
        except: opts.add_argument("--headless")
    opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu"); opts.add_argument(f"--user-agent={USER_AGENT}")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=opts)
    driver.set_page_load_timeout(60)
    for attempt in range(3):
        try:
            driver.get(url); time.sleep(2)
            cookies = driver.get_cookies()
            driver.quit()
            return cookies
        except TimeoutException:
            logger.warning(f"Timeout {attempt+1}"); time.sleep(5)
        except Exception as e:
            logger.error(f"Selenium hata: {e}"); time.sleep(5)
    driver.quit()
    raise RuntimeError("Cookie alınamadı")

def cookie_header_from_list(cookies):
    return "; ".join([f"{x['name']}={x['value']}" for x in cookies])

def safe_post(session,url,payload,headers,n=3,backoff=1.0):
    for attempt in range(n):
        try:
            return session.post(url,json=payload,headers=headers,timeout=30,verify=certifi.where())
        except Exception as e:
            logger.error(f"POST hata: {e}")
            time.sleep(backoff*(attempt+1))
    return None

def load_dates(path=DATES_FILE):
    df = pd.read_csv(path)
    df["Tarih"] = pd.to_datetime(df["Tarih"], dayfirst=True, errors="coerce")
    dates = df["Tarih"].dropna().sort_values(ascending=False).tolist()
    return dates

def fetch_for_target_range(session,start,end,endeks="09"):
    payload = {"baslangicTarih":start,"bitisTarihi":end,"sektor":None,"endeks":endeks,"hisse":None}
    headers = {"Content-Type":"application/json; charset=UTF-8","X-Requested-With":"XMLHttpRequest",
               "Origin":"https://www.isyatirim.com.tr","Referer":BASE_PAGE,"User-Agent":USER_AGENT,
               "Cookie":session.headers.get("Cookie","")}
    r = safe_post(session,AJAX_URL,payload,headers)
    if r and r.ok:
        try: return r.json().get("d",[])
        except: pass
    return []

def temizle_fiyat(s):
    if pd.isna(s): return None
    try:
        s = str(s).strip().replace(",", ".")
        return float(s)
    except:
        return None

def pivotla(df,kolon,do_ffill=True):
    df["Kod"] = df["Kod"].astype(str).str.strip().str.upper()
    df["Tarih"] = pd.to_datetime(df["Tarih"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Tarih","Kod",kolon])
    df[kolon] = df[kolon].map(temizle_fiyat)

    pivot_df = pd.pivot_table(
        df,
        index="Tarih",
        columns="Kod",
        values=kolon,
        aggfunc="first"
    )

    pivot_df = pivot_df.sort_index(ascending=False).sort_index(axis=1)
    if do_ffill:
        pivot_df = pivot_df.ffill()
    pivot_df.index = pivot_df.index.strftime("%d.%m.%Y")

    # iki basamaklı ondalık
    pivot_df = pivot_df.round(2)

    return pivot_df

def main():
    dates = load_dates(DATES_FILE)
    session = requests.Session()
    cookies = get_cookies_with_selenium(CHROMEDRIVER_PATH,HEADLESS,BASE_PAGE)
    session.headers.update({"Cookie":cookie_header_from_list(cookies),"User-Agent":USER_AGENT})
    all_data=[]; cnt=0

    for i,dt in enumerate(dates):
        end = dt.strftime("%d-%m-%Y")
        if i+1 < len(dates):
            start = dates[i+1].strftime("%d-%m-%Y")
            recs = fetch_for_target_range(session,start,end)
            if recs:
                for r in recs: r["Tarih"] = end
                all_data += recs
                cnt += 1
        if cnt >= MAX_ROWS:   # sadece 5 gün
            break

    if all_data:
        df = pd.DataFrame(all_data).rename(columns={
            "HISSE_KODU":"Kod",
            "YAB_ORAN_END":"Yabancı Oran"
        })
        if {"Kod","Yabancı Oran"} <= set(df.columns):
            dfp = pivotla(df,"Yabancı Oran",do_ffill=True)
            dfp.to_csv(
                OUTPUT_FILE,
                sep=";",              
                encoding="utf-8-sig", 
                float_format="%.2f"   # örn: 13.72
            )
            logger.info(f"{dfp.shape} tablo {OUTPUT_FILE} yazıldı")
        else:
            logger.warning("Eksik kolonlar, pivot yapılamadı.")
    else:
        logger.warning("Veri yok")

if __name__=="__main__":
    main()
