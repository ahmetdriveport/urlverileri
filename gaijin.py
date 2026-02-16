#!/usr/bin/env python3
import os, time, logging, requests, pandas as pd, json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

secret=json.loads(os.environ.get("GAIJIN"))
AJAX_URL=secret["ajax_url"]
BASE_PAGE=secret["base_page"]
USER_AGENT=secret.get("user_agent","Mozilla/5.0")

CHROMEDRIVER_PATH=os.getenv("CHROMEDRIVER_PATH","")
HEADLESS=os.getenv("SELENIUM_HEADLESS","true").lower() in ("1","true","yes")
DATES_FILE=os.path.join(os.path.dirname(__file__),"data","dates.csv")
PIVOT_FILE="pivot_gaijin.xlsx"
MAX_ROWS=500

# Sertifika dosyası yolu
CERT_PATH=os.path.join(os.path.dirname(__file__),"data","ti.crt")

logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
logger=logging.getLogger(__name__)

def get_cookies_with_selenium(path,headless,url):
    opts=Options()
    if headless:
        try: opts.add_argument("--headless=new")
        except: opts.add_argument("--headless")
    opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu"); opts.add_argument(f"--user-agent={USER_AGENT}")
    driver=webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=opts)
    driver.set_page_load_timeout(60)
    for attempt in range(5):
        try:
            driver.get(url); time.sleep(2)
            cookies=driver.get_cookies(); driver.quit(); return cookies
        except TimeoutException:
            logger.warning(f"Timeout {attempt+1}"); time.sleep(2*(attempt+1))
        except Exception as e:
            logger.error(f"Selenium hata: {e}"); time.sleep(2*(attempt+1))
    driver.quit(); raise RuntimeError("Cookie alınamadı")

def cookie_header_from_list(cookies):
    return ", ".join([f"{x['name']}={x['value']}" for x in cookies])

def safe_post(session,url,payload,headers,n=5,backoff=2.0):
    for attempt in range(n):
        try:
            r=session.post(url,json=payload,headers=headers,timeout=30,verify=CERT_PATH)
            if r.ok: return r
        except Exception as e:
            logger.error(f"POST hata: {e}, deneme {attempt+1}/{n}")
            time.sleep(backoff*(attempt+1))
    return None

def load_dates_and_hisseler(path=DATES_FILE):
    df=pd.read_csv(path)
    df["Tarih"]=pd.to_datetime(df["Tarih"],dayfirst=True,errors="coerce")
    dates=df["Tarih"].dropna().sort_values(ascending=False).tolist()
    hisseler=(df.iloc[:,1].dropna().astype(str).str.strip().str.upper().unique().tolist())
    return dates,hisseler

def fetch_for_target_range(session,start,end,endeks="09"):
    payload={"baslangicTarih":start,"bitisTarihi":end,"sektor":None,"endeks":endeks,"hisse":None}
    headers={"Content-Type":"application/json; charset=UTF-8","X-Requested-With":"XMLHttpRequest",
             "Origin":"https://www.isyatirim.com.tr","Referer":BASE_PAGE,"User-Agent":USER_AGENT,
             "Cookie":session.headers.get("Cookie","")}
    r=safe_post(session,AJAX_URL,payload,headers)
    if r and r.ok:
        try: return r.json().get("d",[])
        except: pass
    return []

def main():
    dates,hisseler=load_dates_and_hisseler(DATES_FILE)
    session=requests.Session()
    cookies=get_cookies_with_selenium(CHROMEDRIVER_PATH,HEADLESS,BASE_PAGE)
    session.headers.update({"Cookie":cookie_header_from_list(cookies),"User-Agent":USER_AGENT})
    all_data=[]; cnt=0
    for i,dt in enumerate(dates):
        end=dt.strftime("%d-%m-%Y")
        if i+1<len(dates):
            start=dates[i+1].strftime("%d-%m-%Y")
            recs=fetch_for_target_range(session,start,end)
            if recs:
                for r in recs: r["Tarih"]=end
                all_data+=recs; cnt+=1
        if cnt>=MAX_ROWS: break
    if all_data:
        df=pd.DataFrame(all_data)[["Tarih","HISSE_KODU","YAB_ORAN_END"]]
        df["Tarih"]=pd.to_datetime(df["Tarih"],dayfirst=True,errors="coerce")
        df["HISSE_KODU"]=df["HISSE_KODU"].astype(str).str.strip().str.upper()
        df["YAB_ORAN_END"]=pd.to_numeric(df["YAB_ORAN_END"],errors="coerce").round(2)
        df=df[df["HISSE_KODU"].isin(hisseler)]

        # Duplicate kombinasyonları bul ve iptal et
        dupes = df.duplicated(subset=["Tarih","HISSE_KODU"], keep=False)
        dropped = df[dupes]
        if not dropped.empty:
            logger.warning(f"{len(dropped)} satır duplicate olduğu için iptal edildi")
        df = df[~dupes]

        # Pivot işlemi
        pivot_df=df.pivot(index="Tarih",columns="HISSE_KODU",values="YAB_ORAN_END")
        pivot_df=pivot_df.reindex(columns=hisseler).sort_index(ascending=False).sort_index(axis=1)
        pivot_df.index=pivot_df.index.strftime("%d.%m.%Y")
        pivot_df.to_excel(PIVOT_FILE,engine="openpyxl")
        logger.info(f"{pivot_df.shape} boyutlu pivot {PIVOT_FILE} yazıldı")
    else: 
        logger.warning("Veri yok")

if __name__=="__main__": main()
