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

BASE_URL=json.loads(os.environ["MAININDIS"])["DATA_SOURCE_URL"]

def cnn_kapanis_tek_gun(g,a,y):
    try:
        r=requests.get(f"{BASE_URL}&gun={g}&ay={a}&yil={y}",headers={'User-Agent':'Mozilla/5.0'},timeout=10)
        r.raise_for_status()
    except: return []
    soup=BeautifulSoup(r.text,'html.parser'); t=soup.find('table')
    if not t: return []
    ts=f"{g:02d}.{a:02d}.{y}"
    return [{"Tarih":ts,"Endeks":c[0],"Kapanış":temizle_sayi(c[1])}
            for c in ([td.get_text(strip=True) for td in tr.find_all('td')] for tr in t.find_all('tr')[1:])
            if len(c)>=2 and temizle_sayi(c[1])]

try:
    df_csv=pd.read_csv("data/dates.csv",encoding="utf-8")
    secili=list(reversed(secili_tarihleri_bul(df_csv["Tarih"].dropna().tolist(),150)))
    takip=df_csv.iloc[:,2].dropna().unique().tolist()
    tum=[v for t in secili for v in cnn_kapanis_tek_gun(*map(int,t.split(".")))]
    vg=defaultdict(list); [vg[v["Tarih"]].append(v) for v in tum]
    ilk={}; [ilk.setdefault(v["Endeks"],pd.to_datetime(v["Tarih"],dayfirst=True)) for v in tum]
    endeksler=sorted({v["Endeks"] for v in tum}); onceki={}; final=[]
    for t in secili:
        td=pd.to_datetime(t,dayfirst=True); gv={v["Endeks"]:v for v in vg.get(t,[])}; sat=[]
        for e in endeksler:
            if e in gv: sat.append(gv[e])
            elif e in onceki and td>=ilk[e]: k=onceki[e].copy(); k["Tarih"]=t; sat.append(k)
        onceki.update(gv); final.extend(sat)
    df=pd.DataFrame(final)

    def pivotla(df,kolon,takip,tarih,haftalik=False):
        df["Tarih"]=pd.to_datetime(df["Tarih"],dayfirst=True,errors="coerce")
        df=df[df["Tarih"].isin(pd.to_datetime(tarih,dayfirst=True))].dropna(subset=["Tarih","Endeks",kolon])
        df[kolon]=df[kolon].apply(temizle_sayi)
        if haftalik:
            df["Hafta"]=df["Tarih"].dt.to_period("W"); df["Gun"]=df["Tarih"].dt.dayofweek; tercih=[4,3,2,1,0]; haftalik=[]
            for h,g in df.groupby("Hafta"):
                for gun in tercih:
                    s=g[g["Gun"]==gun]
                    if not s.empty: haftalik.append(s); break
            if not haftalik: return pd.DataFrame()
            df=pd.concat(haftalik)
        p=df.pivot_table(index="Tarih",columns="Endeks",values=kolon,aggfunc="first").ffill()
        p=p[[e for e in p.columns if e in takip]]
        return p.sort_index(ascending=False).sort_index(axis=1)

    kapanis=pivotla(df,"Kapanış",takip,secili)
    haftalik=pivotla(df,"Kapanış",takip,secili,haftalik=True)

    with pd.ExcelWriter("endeks.xlsx",engine="openpyxl") as w:
        kapanis.index=kapanis.index.strftime("%d.%m.%Y"); kapanis.to_excel(w,sheet_name="Kapanis")
        if not haftalik.empty:
            haftalik.index=haftalik.index.strftime("%d.%m.%Y"); haftalik.to_excel(w,sheet_name="Haftalik_Kapanis")
    print("✅ endeks.xlsx oluşturuldu")
except Exception as e: print("❌ Hata:",e)
