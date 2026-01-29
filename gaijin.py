#!/usr/bin/env python3
import os,time,logging,csv
from datetime import datetime,UTC
import requests,pandas as pd,certifi
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

CHROMEDRIVER_PATH=os.getenv("CHROMEDRIVER_PATH","")
HEADLESS=os.getenv("SELENIUM_HEADLESS","true").lower() in ("1","true","yes")
USER_AGENT="Mozilla/5.0"
AJAX_URL="https://www.isyatirim.com.tr/_layouts/15/IsYatirim.Website/StockInfo/CompanyInfoAjax.aspx/GetYabanciOranlarXHR"
BASE_PAGE="https://www.isyatirim.com.tr/tr-tr/analiz/hisse/Sayfalar/yabanci-oranlari.aspx"
MAX_ROWS=5
DATES_FILE="data/dates.csv"
OUTPUT_FILE="gaijin.csv"

logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
logger=logging.getLogger(__name__)

def get_cookies_with_selenium(path,headless,url):
    o=Options()
    if headless:
        try:o.add_argument("--headless=new")
        except:o.add_argument("--headless")
    o.add_argument("--no-sandbox");o.add_argument("--disable-dev-shm-usage");o.add_argument("--disable-gpu");o.add_argument(f"--user-agent={USER_AGENT}")
    d=webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=o);d.set_page_load_timeout(60)
    for _ in range(3):
        try:d.get(url);time.sleep(2);c=d.get_cookies();d.quit();return c
        except TimeoutException:time.sleep(5)
        except Exception as e:logger.error(e);time.sleep(5)
    d.quit();raise RuntimeError("cookie alınamadı")

def cookie_header_from_list(c):return "; ".join([f"{x['name']}={x['value']}" for x in c])

def safe_post(s,u,p,h,n=3,b=1.0):
    for a in range(n):
        try:return s.post(u,json=p,headers=h,timeout=30,verify=certifi.where())
        except Exception as e:logger.error(e);time.sleep(b*(a+1))
    return None

def date_str_to_dt(s):
    s=s.strip().split()[0].replace("/",".").replace("-",".")
    for f in ("%d.%m.%Y","%Y.%m.%d","%d.%m.%y"):
        try:return datetime.strptime(s,f)
        except:continue
    raise ValueError(f"bad date {s}")

def load_dates_from_csv(path=DATES_FILE):
    d=[]
    with open(path,"r",encoding="utf-8") as f:
        for r in csv.reader(f):
            if r and r[0].strip():
                try:
                    dt=date_str_to_dt(r[0].strip())
                    if dt.date()<=datetime.now(UTC).date():d.append(dt)
                except:pass
    return sorted(d,reverse=True)

def fetch_for_target_range(s,start,end,endeks="09"):
    p={"baslangicTarih":start,"bitisTarihi":end,"sektor":None,"endeks":endeks,"hisse":None}
    h={"Content-Type":"application/json; charset=UTF-8","X-Requested-With":"XMLHttpRequest","Origin":"https://www.isyatirim.com.tr","Referer":BASE_PAGE,"User-Agent":USER_AGENT,"Cookie":s.headers.get("Cookie","")}
    r=safe_post(s,AJAX_URL,p,h)
    if r and r.ok:
        try:return r.json().get("d",[])
        except:pass
    return []

def main():
    dates=load_dates_from_csv(DATES_FILE)
    s=requests.Session()
    c=get_cookies_with_selenium(CHROMEDRIVER_PATH,HEADLESS,BASE_PAGE)
    s.headers.update({"Cookie":cookie_header_from_list(c),"User-Agent":USER_AGENT})
    all=[];cnt=0
    for i,dt in enumerate(dates):
        end=dt.strftime("%d-%m-%Y")
        for off in (3,10,20):
            if i+off<len(dates):
                start=dates[i+off].strftime("%d-%m-%Y")
                recs=fetch_for_target_range(s,start,end)
                if recs:
                    for r in recs:r["Tarih"]=end
                    all+=recs;cnt+=1;break
        if cnt>=MAX_ROWS:break
    if all:
        df=pd.DataFrame(all)[["Tarih","HISSE_KODU","YAB_ORAN_END"]].rename(columns={"HISSE_KODU":"Kod","YAB_ORAN_END":"Yabancı Oran"})
        dfp=df.pivot_table(index="Tarih",columns="Kod",values="Yabancı Oran",aggfunc="first").sort_index()
        dfp.to_csv(OUTPUT_FILE,encoding="utf-8")
        logger.info(f"{dfp.shape} tablo {OUTPUT_FILE} yazıldı")
    else:logger.warning("veri yok")

if __name__=="__main__":main()
