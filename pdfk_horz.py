import pandas as pd, numpy as np, os
from tarih_ayar import secili_tarihleri_bul

df_dates=pd.read_csv("data/dates.csv",encoding="utf-8")
codes=df_dates.iloc[:,1].dropna().astype(str).str.strip().str.upper().tolist()
tarih_list=df_dates.iloc[:,0].dropna().astype(str).str.strip().tolist()
master_dates=secili_tarihleri_bul(tarih_list)

if not os.path.exists("pdfk_vert.xlsx"): raise FileNotFoundError("❌ pdfk_vert.xlsx bulunamadı. Önce vert script çalışmalı.")
df_src=pd.read_excel("pdfk_vert.xlsx",engine="openpyxl")
df_src.columns=df_src.columns.str.strip()
df_src["Hisse_Kodu"]=df_src["Hisse_Kodu"].astype(str).str.strip().str.upper()
df_src["Tarih"]=pd.to_datetime(df_src["Tarih"].astype(str),format="%d.%m.%Y",errors="coerce")

latest_values,pivot_tables={},{}

def create_pivot(df,col,dtype="int"):
    p=pd.pivot_table(df,index="Tarih",columns="Hisse_Kodu",values=col,aggfunc="first")
    p=p.reindex(pd.to_datetime(master_dates,dayfirst=True))
    for c in codes:
        if c not in p.columns: p[c]=np.nan
    p=p[codes].ffill().replace([np.inf,-np.inf],pd.NA)
    if dtype=="int": p=p.round().astype("Int64")
    elif dtype=="float2": p=p.round(2).astype(float)
    elif dtype=="float5": p=p.round(5).astype(float)
    last=p.tail(1)
    latest_values[col]={"Tarih":last.index[0].strftime("%d.%m.%Y"),"Veriler":last.iloc[0].to_dict()}
    out=p.reset_index().rename(columns={"index":"Tarih"})
    out["Tarih"]=pd.to_datetime(out["Tarih"],errors="coerce")
    out=out.sort_values("Tarih",ascending=False)
    out["Tarih"]=out["Tarih"].dt.strftime("%d.%m.%Y")
    pivot_tables[col]=out

# Vertical’den gelen Pd/Fk kaldırıldı, sadece temel kolonlar pivotlanıyor
for col,dtype in [
    ("Ozkaynak","int"),("Sermaye","int"),("Aktifler","int"),("Netborc","int"),("Yillik_Kar","int"),
    ("Ozkarlilik","float2"),("Aktifkarlilik","float2")
]: create_pivot(df_src,col,dtype)

# 🔹 Pd/Fk hesaplamaları horizontal aşamada pivotlardan yapılacak
sermaye_last = latest_values["Sermaye"]["Veriler"]  # her hisse için en güncel sermaye
ozkaynak_pivot = pivot_tables["Ozkaynak"].set_index("Tarih")
yillikkar_pivot = pivot_tables["Yillik_Kar"].set_index("Tarih")

# Pd_Carpan pivotu
pd_carpan = ozkaynak_pivot.copy()
for h in codes:
    sermaye_val = sermaye_last.get(h, np.nan)
    pd_carpan[h] = np.where(
        (ozkaynak_pivot[h].notna()) & (ozkaynak_pivot[h] != 0),
        (sermaye_val / ozkaynak_pivot[h]).round(5),
        np.nan
    )
pd_carpan = pd_carpan.reset_index()
pd_carpan["Tarih"] = pd.to_datetime(pd_carpan["Tarih"], errors="coerce").dt.strftime("%d.%m.%Y")
pivot_tables["Pd_Carpan"] = pd_carpan

# Fk_Carpan pivotu
fk_carpan = yillikkar_pivot.copy()
for h in codes:
    sermaye_val = sermaye_last.get(h, np.nan)
    fk_carpan[h] = np.where(
        (yillikkar_pivot[h].notna()) & (yillikkar_pivot[h] != 0),
        (sermaye_val / yillikkar_pivot[h]).round(5),
        np.nan
    )
fk_carpan = fk_carpan.reset_index()
fk_carpan["Tarih"] = pd.to_datetime(fk_carpan["Tarih"], errors="coerce").dt.strftime("%d.%m.%Y")
pivot_tables["Fk_Carpan"] = fk_carpan

def safe(x): 
    return "" if pd.isna(x) or (isinstance(x,float) and np.isinf(x)) else (
        round(float(x),5) if isinstance(x,float) else int(x) if isinstance(x,int) else str(x)
    )

def latest_vertical():
    rows=[]; last_date=latest_values["Sermaye"]["Tarih"]
    last_date_dt=pd.to_datetime(last_date,format="%d.%m.%Y",errors="coerce")
    for h in codes:
        subset=df_src[(df_src["Hisse_Kodu"]==h)&(df_src["Tarih"]==last_date_dt)]
        msci_val=subset["Msci"].iloc[0] if not subset.empty else ""
        rows.append([last_date,h,
            safe(latest_values["Pd_Carpan"]["Veriler"].get(h,"")),
            safe(latest_values["Fk_Carpan"]["Veriler"].get(h,"")),
            msci_val,
            safe(latest_values["Sermaye"]["Veriler"].get(h,"")),
            safe(latest_values["Ozkaynak"]["Veriler"].get(h,"")),
            safe(latest_values["Aktifler"]["Veriler"].get(h,"")),
            safe(latest_values["Netborc"]["Veriler"].get(h,"")),
            safe(latest_values["Yillik_Kar"]["Veriler"].get(h,"")),
            safe(latest_values["Aktifkarlilik"]["Veriler"].get(h,"")),
            safe(latest_values["Ozkarlilik"]["Veriler"].get(h,""))])
    return pd.DataFrame(rows,columns=[
        "Tarih","Hisse_Kodu","Pd_Carpan","Fk_Carpan","Msci",
        "Sermaye","Ozkaynak","Aktifler","Netborc","Yillik_Kar",
        "Aktifkarlilik","Ozkarlilik"])

artifact="pdfk_horz.xlsx"
with pd.ExcelWriter(artifact,engine="openpyxl") as w:
    [df.to_excel(w,sheet_name=name[:30],index=False) for name,df in pivot_tables.items()]
    latest_vertical().to_excel(w,sheet_name="Son_Tarihli_Oranlar",index=False)

print("✅ Artifact oluşturuldu:",artifact)
