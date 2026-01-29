#!/usr/bin/env python3
import os, time, logging, requests, pandas as pd, certifi
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
MAX_ROWS = 5

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
    for attempt in range(5):  # retry güçlendirildi
        try:
            driver.get(url); time.sleep(2)
            cookies = driver.get_cookies()
            driver.quit()
            return cookies
        except TimeoutException:
            logger.warning(f"Timeout {attempt+1}"); time.sleep(2*(attempt+1))
        except Exception as e:
            logger.error(f"Selenium hata: {e}"); time.sleep(2*(attempt+1))
    driver.quit()
    raise RuntimeError("Cookie alınamadı")

def cookie_header_from_list(cookies):
    return ", ".join([f"{x['name']}={x['value']}" for x in cookies])

def safe_post(session,url,payload,headers,n=5,backoff=2.0):
    for attempt in range(n):
        try:
            r = session.post(url,json=payload,headers=headers,timeout=30,verify=certifi.where())
            if r.ok:
                return r
        except Exception as e:
            logger.error(f"POST hata: {e}, deneme {attempt+1}/{n}")
            time.sleep(backoff * (attempt+1))
    return None

def load_dates_and_hisseler(path=DATES_FILE):
    df = pd.read_csv(path)
    df["Tarih"] = pd.to_datetime(df["Tarih"], dayfirst=True, errors="coerce")
    dates = df["Tarih"].dropna().sort_values(ascending=False).tolist()
    hisseler = df.iloc[:,1].dropna().astype(str).str.strip().str.upper().unique().tolist()
    return dates, hisseler

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

def main():
    dates, hisseler = load_dates_and_hisseler(DATES_FILE)
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
        if cnt >= MAX_ROWS:
            break

    if all_data:
        df = pd.DataFrame(all_data)[["Tarih", "HISSE_KODU", "YAB_ORAN_END"]]

        # 🔧 Tekrarları önle
        df = df.drop_duplicates(subset=["Tarih", "HISSE_KODU", "YAB_ORAN_END"])

        # 🔧 Hisse kodlarını normalize et
        df["HISSE_KODU"] = df["HISSE_KODU"].astype(str).str.strip().str.upper()

        # 🔧 dates.csv’deki hisselerle filtrele
        df = df[df["HISSE_KODU"].isin(hisseler)]

        # 🔧 Sayısal değerleri 2 basamağa yuvarla
        df["YAB_ORAN_END"] = pd.to_numeric(df["YAB_ORAN_END"], errors="coerce").round(2)

        # 🔧 Pivotlama: yatay tabloya çevir
        df["Tarih"] = pd.to_datetime(df["Tarih"], dayfirst=True, errors="coerce")
        pivot_df = pd.pivot_table(
            df,
            index="Tarih",
            columns="HISSE_KODU",
            values="YAB_ORAN_END",
            aggfunc="first"
        )
        pivot_df = pivot_df.sort_index(ascending=False).sort_index(axis=1)
        pivot_df.index = pivot_df.index.strftime("%d.%m.%Y")

        pivot_df.to_csv(
            OUTPUT_FILE,
            sep=",",
            encoding="utf-8-sig"
        )
        logger.info(f"{pivot_df.shape} tablo {OUTPUT_FILE} yazıldı (pivotlanmış)")
    else:
        logger.warning("Veri yok")

if __name__=="__main__":
    main()
