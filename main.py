import requests,pandas as pd,json,os
from bs4 import BeautifulSoup
from collections import defaultdict
from tarih_ayar import secili_tarihleri_bul

def temizle_sayi(s):
    s=str(s).strip()
    if not s: return None
    if "," in s and "." in s: s=s.replace(".","").replace(",",".")
    elif "," in s: s=s.replace(",",".")
    elif "." in s and len(s.split(".")[-1])>2: s=s.replace(".","")
    try: return float(s)
    except: return None

def temizle_hacim(s):
    s=str(s).strip()
    if not s: return None
    s=s.replace(".","").replace(",","")
    try: return int(s)
    except: return None

BASE_URL=json.loads(os.environ["MAIN"])["DATA_SOURCE_URL"]

def fiyat_hacim_tek_gun(g,a,y):
    try:
        r=requests.get(f"{BASE_URL}?gun={g}&ay={a}&yil={y}&tip=Hisse",headers={'User-Agent':'Mozilla/5.0'},timeout=10)
        r.raise_for_status()
    except: return []
    soup=BeautifulSoup(r.text,'html.parser'); t=soup.find('table')
    if not t: return []
    ts=f"{g:02d}.{a:02d}.{y}"
    return [{"Tarih":ts,"Hisse":c[0],"Kapanış":temizle_sayi(c[1]),"Yüksek":temizle_sayi(c[4]),"Düşük":temizle_sayi(c[5]),"Hacim(Lot)":temizle_hacim(c[7])}
            for c in ([td.get_text(strip=True) for td in tr.find_all('td')] for tr in t.find_all('tr')[1:])
            if len(c)>=9 and temizle_sayi(c[1])]

try:
    df_csv=pd.read_csv("data/dates.csv",encoding="utf-8")
    secili=secili_tarihleri_bul(df_csv["Tarih"].dropna().tolist())
    takip=df_csv.iloc[:,1].dropna().unique().tolist()
    tum=[v for t in secili for v in fiyat_hacim_tek_gun(*map(int,t.split(".")))]
    vg=defaultdict(list); [vg[v["Tarih"]].append(v) for v in tum]
    ilk={}; [ilk.setdefault(v["Hisse"],pd.to_datetime(v["Tarih"],dayfirst=True)) for v in tum]
    hisseler=sorted({v["Hisse"] for v in tum}); onceki={}; final=[]
    for t in secili:
        td=pd.to_datetime(t,dayfirst=True); gv={v["Hisse"]:v for v in vg.get(t,[])}; sat=[]
        for h in hisseler:
            if h in gv: sat.append(gv[h])
            elif h in onceki and td>=ilk[h]: k=onceki[h].copy(); k["Tarih"]=t; sat.append(k)
        onceki.update(gv); final.extend(sat)
    df=pd.DataFrame(final)
    def pivotla(df,kolon,takip,tarih,haftalik=False):
        df["Tarih"]=pd.to_datetime(df["Tarih"],dayfirst=True,errors="coerce")
        df=df[df["Tarih"].isin(pd.to_datetime(tarih,dayfirst=True))].dropna(subset=["Tarih","Hisse",kolon])
        df[kolon]=df[kolon].apply(temizle_sayi)
        if haftalik:
            df["Hafta"]=df["Tarih"].dt.to_period("W"); df["Gun"]=df["Tarih"].dt.dayofweek; tercih=[4,3,2,1,0]; haftalik=[]
            for h,g in df.groupby("Hafta"):
                for gun in tercih:
                    s=g[g["Gun"]==gun]
                    if not s.empty: haftalik.append(s); break
            if not haftalik: return pd.DataFrame()
            df=pd.concat(haftalik)
        p=df.pivot_table(index="Tarih",columns="Hisse",values=kolon,aggfunc="first").ffill()
        p=p[[h for h in p.columns if h in takip]]
        return p.sort_index(ascending=False).sort_index(axis=1)
    tablolar={c:pivotla(df,c,takip,secili) for c in ["Kapanış","Yüksek","Düşük","Hacim(Lot)"]}
    haftalik=pivotla(df,"Kapanış",takip,secili,haftalik=True)
    with pd.ExcelWriter("fiyat.xlsx",engine="openpyxl") as w:
        for c,p in tablolar.items():
            p.index=p.index.strftime("%d.%m.%Y"); p.to_excel(w,sheet_name=c)
        if not haftalik.empty:
            haftalik.index=haftalik.index.strftime("%d.%m.%Y"); haftalik.to_excel(w,sheet_name="Haftalik_Kapanis")
    print("✅ fiyat.xlsx oluşturuldu")
except Exception:
    print("❌ fiyat.xlsx oluşturulamadı")
