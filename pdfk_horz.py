import pandas as pd, numpy as np, os

df_dates = pd.read_csv("data/dates.csv", encoding="utf-8")
codes = df_dates.iloc[:,1].dropna().astype(str).str.strip().str.upper().tolist()

if not os.path.exists("pdfk_vert.xlsx"):
    raise FileNotFoundError("❌ pdfk_vert.xlsx bulunamadı. Önce vert script çalışmalı.")

df_src = pd.read_excel("pdfk_vert.xlsx", engine="openpyxl")
df_src.columns = df_src.columns.str.strip()
df_src["Hisse_Kodu"] = df_src["Hisse_Kodu"].astype(str).str.strip().str.upper()
df_src["Tarih"] = pd.to_datetime(df_src["Tarih"].astype(str), format="%d.%m.%Y", errors="coerce")

latest_values, pivot_tables = {}, {}

def create_pivot(df, col, dtype="int"):
    p = pd.pivot_table(df, index="Tarih", columns="Hisse_Kodu", values=col, aggfunc="first").sort_index(ascending=False)
    for c in codes:
        if c not in p.columns: 
            p[c] = np.nan
    p = p[codes].ffill().replace([np.inf,-np.inf], pd.NA)
    if dtype=="int": p = p.round().astype("Int64")
    elif dtype=="float2": p = p.round(2).astype(float)
    elif dtype=="float5": p = p.round(5).astype(float)
    last = p.head(1)
    latest_values[col] = {"Tarih": last.index[0].strftime("%d.%m.%Y"), "Veriler": last.iloc[0].to_dict()}
    out = p.reset_index()
    out["Tarih"] = out["Tarih"].dt.strftime("%d.%m.%Y")
    pivot_tables[col] = out

for col,dtype in [
    ("Ozkaynak","int"),("Sermaye","int"),("Aktifler","int"),("Netborc","int"),("Yillik_Kar","int"),
    ("Ozkarlilik","float2"),("Aktifkarlilik","float2"),("Pd_Carpan","float5"),("Fk_Carpan","float5")
]: 
    create_pivot(df_src,col,dtype)

def safe(x): 
    return "" if pd.isna(x) or (isinstance(x,float) and np.isinf(x)) else (
        round(float(x),5) if isinstance(x,float) else int(x) if isinstance(x,int) else str(x)
    )

def latest_vertical():
    rows=[]
    last_date = latest_values["Pd_Carpan"]["Tarih"]
    last_date_dt = pd.to_datetime(last_date, format="%d.%m.%Y", errors="coerce")

    for h in codes:
        subset = df_src[(df_src["Hisse_Kodu"] == h) & (df_src["Tarih"] == last_date_dt)]
        msci_val = subset["Msci"].iloc[0] if not subset.empty else ""

        rows.append([
            last_date,  # Tarih
            h,          # Hisse_Kodu
            msci_val,   # Msci
            safe(latest_values["Sermaye"]["Veriler"].get(h,"")),
            safe(latest_values["Ozkaynak"]["Veriler"].get(h,"")),
            safe(latest_values["Aktifler"]["Veriler"].get(h,"")),
            safe(latest_values["Netborc"]["Veriler"].get(h,"")),
            safe(latest_values["Yillik_Kar"]["Veriler"].get(h,"")),
            safe(latest_values["Aktifkarlilik"]["Veriler"].get(h,"")),
            safe(latest_values["Ozkarlilik"]["Veriler"].get(h,"")),
            safe(latest_values["Pd_Carpan"]["Veriler"].get(h,"")),
            safe(latest_values["Fk_Carpan"]["Veriler"].get(h,""))
        ])
    return pd.DataFrame(rows, columns=[
        "Tarih","Hisse_Kodu","Msci",
        "Sermaye","Ozkaynak","Aktifler","Netborc","Yillik_Kar",
        "Aktifkarlilik","Ozkarlilik","Pd_Carpan","Fk_Carpan"
    ])

artifact="pdfk_horz.xlsx"
with pd.ExcelWriter(artifact,engine="openpyxl") as w:
    [df.to_excel(w,sheet_name=name[:30],index=False) for name,df in pivot_tables.items()]
    latest_vertical().to_excel(w,sheet_name="Son_Tarihli_Oranlar",index=False)

print("✅ Artifact oluşturuldu:",artifact)
