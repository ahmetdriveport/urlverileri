import pandas as pd, requests, os, json, time
from io import BytesIO
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

urls=json.loads(os.environ.get("SEKTORPAZAR"))
url_indices,url_markets,url_fd=urls["url1"],urls["url2"],urls["url3"]

# Baştaki 5 sütunu besleyen yerel dosyalar ve orijinal mantık aynen korundu
df_dates=pd.read_csv("data/dates.csv")
hisse_list=df_dates["Hisse"].dropna().str.strip().unique()
today=datetime.now().date()
tarih_list=pd.to_datetime(df_dates.iloc[:,0],dayfirst=True,errors="coerce").dropna().dt.date
secili_tarih=max([d for d in tarih_list if d<=today])
tarih_str=secili_tarih.strftime("%d.%m.%Y")

df_raw=pd.read_excel(BytesIO(requests.get(url_indices,timeout=30).content),header=None)
endeks_dict={}
cur=None
for i in range(len(df_raw)-1):
    vals=[str(df_raw.iat[i,j]).strip() if pd.notna(df_raw.iat[i,j]) else "" for j in range(min(5,df_raw.shape[1]))]
    nxt=str(df_raw.iat[i+1,0]).strip() if pd.notna(df_raw.iat[i+1,0]) else ""
    baslik=next((v for v in vals if v.upper().startswith("BIST")),None)
    if baslik and nxt=="1": 
        cur=baslik
        endeks_dict[cur]=[]
        continue
    if vals and vals[0].isdigit() and cur: 
        endeks_dict[cur].append(vals[1] if len(vals)>1 else "")
df_endeks=pd.DataFrame({k:pd.Series(v) for k,v in endeks_dict.items()})

df_m=pd.read_excel(BytesIO(requests.get(url_markets,timeout=30).content),header=None)
pazar_dict={}
cur=None
for i in range(len(df_m)):
    a=str(df_m.iat[i,0]).strip() if pd.notna(df_m.iat[i,0]) else ""
    b=str(df_m.iat[i,1]).strip() if pd.notna(df_m.iat[i,1]) else ""
    if any(x in a.upper() for x in["PAZAR","PİYASA"]): 
        cur=a
        pazar_dict[cur]=[]
        continue
    if a.isdigit() and cur: 
        pazar_dict[cur].append(b)
df_pazar=pd.DataFrame({k:pd.Series(v) for k,v in pazar_dict.items()})

def parse_turkish_number(val_str):
    """Metin olarak gelen Türkçe formatlı rakamları temiz sayıya (float) çevirir."""
    if not val_str or val_str.strip() == "" or val_str.strip() == "-":
        return 0.0
    try:
        cleaned = val_str.strip().replace(".", "").replace(",", ".")
        return float(cleaned)
    except:
        return 0.0

# --- SELENIUM ARKA PLAN TARAYICI AYARLARI ---
opts = Options()
opts.add_argument("--headless=new")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--disable-gpu")
opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
fd_dict = {}

try:
    driver.get(url_fd)
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.TAG_NAME, "table"))
    )
    time.sleep(3)
    
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table_element = soup.find("table")
    rows = [[td.get_text(strip=True) for td in tr.find_all(["td", "th"])] for tr in table_element.find_all("tr")]

    if rows:
        header_row = [str(h).strip() for h in rows[0]]
        
        idx_kod = next((i for i, h in enumerate(header_row) if "Borsa Kodu" in h), 1)
        idx_islem_goren = next((i for i, h in enumerate(header_row) if "Borsada İşlem Gören Pay Adedi" in h), 4)
        idx_c_maddesi = next((i for i, h in enumerate(header_row) if "I. C Maddesi" in h), 5)
        idx_de_maddesi = next((i for i, h in enumerate(header_row) if "I. D ve I. E" in h), 6)
        idx_lot = next((i for i, h in enumerate(header_row) if "Fiili Dolaşımdaki Pay Adedi" in h), 7)
        idx_oran = next((i for i, h in enumerate(header_row) if "Fiili Dolaşımdaki Pay Oranı" in h), 8)
        
        for r in rows[1:]:
            if len(r) > max(idx_kod, idx_islem_goren, idx_c_maddesi, idx_de_maddesi, idx_lot, idx_oran):
                kod = r[idx_kod].strip().upper()
                if kod:
                    islem_goren_lot = parse_turkish_number(r[idx_islem_goren])
                    c_maddesi_lot = parse_turkish_number(r[idx_c_maddesi])
                    de_maddesi_lot = parse_turkish_number(r[idx_de_maddesi])
                    dolasim_lotu = parse_turkish_number(r[idx_lot])
                    dolasim_orani = parse_turkish_number(r[idx_oran])
                    
                    dolasim_disi_lot = c_maddesi_lot + de_maddesi_lot
                    
                    fd_dict[kod] = {
                        "islem_goren": islem_goren_lot,
                        "oran": dolasim_orani,
                        "lot": dolasim_lotu,
                        "disi_lot": dolasim_disi_lot
                    }
finally:
    driver.quit()
# --------------------------------------------

res = []
for kod in hisse_list:
    hisse_key = kod.upper()
    if hisse_key in set(df_endeks["BIST 30"].dropna().astype(str).str.strip()): endeks="XU030"
    elif hisse_key in set(df_endeks["BIST 50"].dropna().astype(str).str.strip()): endeks="XU050"
    elif hisse_key in set(df_endeks["BIST 100"].dropna().astype(str).str.strip()): endeks="XU100"
    else: endeks=""
    sektor=""
    for _,r in df_sektor.iterrows():
        if r["Endeks Kolonu"].strip() in df_endeks.columns and hisse_key in set(df_endeks[r["Endeks Kolonu"].strip()].dropna().astype(str).str.strip()):
            sektor=r["Sektor Kodu"].strip(); break
    pazar=""
    for _,r in df_pazar_map.iterrows():
        if r["Pattern"].strip() in df_pazar.columns and hisse_key in set(df_pazar[r["Pattern"].strip()].dropna().astype(str).str.strip()):
            pazar=r["Pazar Kodu"].strip(); break
            
    if hisse_key in fd_dict:
        # İstenen sütunlar tam sayıya (int) yuvarlanıyor
        islem_goren = int(round(fd_dict[hisse_key]["islem_goren"]))
        oran = fd_dict[hisse_key]["oran"]
        lot = int(round(fd_dict[hisse_key]["lot"]))
        darsi = int(round(fd_dict[hisse_key]["disi_lot"]))
    else:
        islem_goren = ""
        oran = ""
        lot = ""
        darsi = ""
        
    res.append([tarih_str, kod, pazar, endeks, sektor, islem_goren, oran, lot, darsi])

artifact=pd.DataFrame(res, columns=["Tarih", "Hisse", "Pazar", "Endeks", "Sektör", "İşlem Gören Lot Sayısı", "Dolaşım Oranı", "Dolaşım Lotu", "Dolaşım Dışı Lot"])

# --- EXCEL BİNLİK AYIRACI BİÇİMLENDİRME İŞLEMİ ---
with pd.ExcelWriter("sektorpazar.xlsx", engine="openpyxl") as writer:
    artifact.to_excel(writer, index=False, sheet_name="SektorPazar")
    
    workbook = writer.book
    worksheet = writer.sheets["SektorPazar"]
    
    # Ondalıksız tam sayı binlik ayracı maskesi
    thousands_format = '#,##0' 
    
    # F, H ve I kolonlarını döngüyle tarayıp sayısal hücrelere format basıyoruz
    for row in range(2, len(artifact) + 2):
        cell_f = worksheet[f"F{row}"]
        cell_h = worksheet[f"H{row}"]
        cell_i = worksheet[f"I{row}"]
        
        if cell_f.value != "": cell_f.number_format = thousands_format
        if cell_h.value != "": cell_h.number_format = thousands_format
        if cell_i.value != "": cell_i.number_format = thousands_format

print("✅ sektorpazar.xlsx baştaki 5 sütun korunarak tam sayı formatıyla başarıyla oluşturuldu.")
