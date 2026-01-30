import pandas as pd
import numpy as np
import os

# dates.csv'den referans hisse kodlarını al
df_dates = pd.read_csv("data/dates.csv", encoding="utf-8")
codes = df_dates.iloc[:,1].dropna().astype(str).str.strip().str.upper().tolist()
codes_set = set(codes)

# --- Önce vert artifact var mı kontrol et ---
if not os.path.exists("pdfk_vert.xlsx"):
    raise FileNotFoundError("❌ pdfk_vert.xlsx bulunamadı. Önce vert script çalışmalı.")

# --- Vert artifact'i oku ---
df_src = pd.read_excel("pdfk_vert.xlsx", engine="openpyxl")
df_src.columns = df_src.columns.str.strip()
df_src["Hisse Kodu"] = df_src["Hisse Kodu"].str.strip().str.upper()
df_src["Tarih"] = pd.to_datetime(df_src["Tarih"].astype(str), format="%d.%m.%Y", errors="coerce")

latest_values = {}
pivot_tables = {}

def create_pivot(df, value_col, dtype="int", all_codes=None):
    pivot = pd.pivot_table(
        df,
        index=["Tarih"],
        columns=["Hisse Kodu"],
        values=value_col,
        aggfunc="first"
    )
    pivot.index = pd.to_datetime(pivot.index.astype(str), errors="coerce")
    pivot.index.name = "Tarih"
    pivot = pivot.sort_index(ascending=False)

    # Eksik sütunları ekle ve sıralamayı dates.csv’ye göre yap
    if all_codes is not None:
        for code in all_codes:
            if code not in pivot.columns:
                pivot[code] = np.nan
        pivot = pivot[all_codes]  # sütun sırası dates.csv’ye göre

    # ffill mantığı
    pivot = pivot.ffill()

    def clean_numeric(x):
        if (x is not None) and (not pd.isna(x)) and str(x) not in ["", "None"]:
            return float(str(x).replace(",", "."))
        return np.nan
    pivot = pivot.apply(lambda col: col.map(clean_numeric))

    if dtype == "int":
        pivot = pivot.round().astype("Int64")
    elif dtype == "float2":
        pivot = pivot.round(2).astype(float)
    elif dtype == "float5":
        pivot = pivot.round(5).astype(float)

    pivot = pivot.replace([np.inf, -np.inf], pd.NA)

    # latest_values güncelle
    last_row = pivot.head(1)
    last_date = last_row.index[0].strftime("%d.%m.%Y")
    latest_values[value_col] = {
        "Tarih": last_date,
        "Veriler": last_row.iloc[0].to_dict()
    }

    df_out = pivot.reset_index()
    df_out.columns = ["Tarih"] + list(df_out.columns[1:])
    df_out["Tarih"] = df_out["Tarih"].dt.strftime("%d.%m.%Y")

    pivot_tables[value_col] = df_out

# --- Pivot tablolar ---
create_pivot(df_src, "Özkaynaklar", dtype="int", all_codes=codes)
create_pivot(df_src, "Ödenmiş Sermaye", dtype="int", all_codes=codes)
create_pivot(df_src, "Toplam Aktifler", dtype="int", all_codes=codes)
create_pivot(df_src, "Net Borç", dtype="int", all_codes=codes)
create_pivot(df_src, "Yıllık Net Kar", dtype="int", all_codes=codes)
create_pivot(df_src, "Öz Karlılık (%)", dtype="float2", all_codes=codes)
create_pivot(df_src, "Aktif Karlılık (%)", dtype="float2", all_codes=codes)
create_pivot(df_src, "PD Çarpan", dtype="float5", all_codes=codes)
create_pivot(df_src, "F/K Çarpan", dtype="float5", all_codes=codes)

# --- Dikey tablo (Son Tarihli Oranlar) ---
def safe_json_value(x):
    if x is None or pd.isna(x):
        return ""
    if isinstance(x, (np.floating, float)):
        if np.isinf(x):
            return ""
        return round(float(x), 5)
    if isinstance(x, (np.integer, int)):
        return int(x)
    return str(x)

def create_latest_vertical(latest_values, all_codes):
    rows = []
    for hisse in all_codes:
        row = [
            hisse,
            safe_json_value(latest_values.get("PD Çarpan", {}).get("Veriler", {}).get(hisse, "")),
            safe_json_value(latest_values.get("F/K Çarpan", {}).get("Veriler", {}).get(hisse, "")),
            safe_json_value(latest_values.get("Aktif Karlılık (%)", {}).get("Veriler", {}).get(hisse, "")),
            safe_json_value(latest_values.get("Öz Karlılık (%)", {}).get("Veriler", {}).get(hisse, "")),
            safe_json_value(latest_values.get("Özkaynaklar", {}).get("Veriler", {}).get(hisse, "")),
            safe_json_value(latest_values.get("Toplam Aktifler", {}).get("Veriler", {}).get(hisse, "")),
            safe_json_value(latest_values.get("Net Borç", {}).get("Veriler", {}).get(hisse, "")),
            safe_json_value(latest_values.get("Yıllık Net Kar", {}).get("Veriler", {}).get(hisse, "")),
            safe_json_value(latest_values.get("Ödenmiş Sermaye", {}).get("Veriler", {}).get(hisse, ""))
        ]
        rows.append(row)

    headers = [
        "Hisse Kodu",
        "PD Çarpan",
        "F/K Çarpan",
        "Aktif Karlılık (%)",
        "Öz Karlılık (%)",
        "Özkaynaklar",
        "Toplam Aktifler",
        "Net Borç",
        "Yıllık Net Kar",
        "Ödenmiş Sermaye"
    ]
    return pd.DataFrame(rows, columns=headers)

df_vert = create_latest_vertical(latest_values, codes)

# --- Tek artifact dosyaya yaz ---
artifact_path = "pdfk_horz.xlsx"
with pd.ExcelWriter(artifact_path, engine="openpyxl") as writer:
    for name, df in pivot_tables.items():
        safe_name = (
            name.replace("/", "-")
                .replace("%", "pct")
                .replace(" ", "_")
        )
        df.to_excel(writer, sheet_name=safe_name[:30], index=False)
    df_vert.to_excel(writer, sheet_name="Son_Tarihli_Oranlar", index=False)

print("✅ Tek artifact oluşturuldu:", artifact_path)
