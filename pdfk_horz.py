import pandas as pd
import numpy as np

# --- Kaynak: ziraat_hamveri (artık Excel artifact'tan okunacak) ---
df_src = pd.read_excel("vert_pdfk.xlsx", engine="openpyxl")
df_src.columns = df_src.columns.str.strip()
df_src["Tarih"] = pd.to_datetime(df_src["Tarih"].astype(str), format="%d.%m.%Y", errors="coerce")

# --- Son değerleri biriktirmek için dict ---
latest_values = {}

# --- Pivot oluşturma fonksiyonu ---
def create_pivot(df, value_col, dtype="int"):
    pivot = pd.pivot_table(
        df,
        index=["Tarih"],
        columns=["Hisse Kodu"],
        values=value_col,
        aggfunc="first"
    )
    pivot.index = pd.to_datetime(pivot.index.astype(str), errors="coerce")
    pivot.index.name = "Tarih"
    pivot = pivot.sort_index(ascending=False).ffill()

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

    # --- Son satırı latest_values dict'e ekle (en güncel tarih en üstte)
    last_row = pivot.head(1)
    last_date = last_row.index[0].strftime("%d.%m.%Y")
    latest_values[value_col] = {
        "Tarih": last_date,
        "Veriler": last_row.iloc[0].to_dict()
    }

    df_out = pivot.reset_index()
    df_out.columns = ["Tarih"] + list(df_out.columns[1:])
    df_out["Tarih"] = df_out["Tarih"].dt.strftime("%d.%m.%Y")

    # Artifact olarak Excel'e yaz
    artifact_path = f"pivot_{value_col}.xlsx"
    df_out.to_excel(artifact_path, index=False, engine="openpyxl")
    print(f"✅ {value_col} pivot artifact oluşturuldu: {artifact_path}")

# --- Ham tablolar ---
create_pivot(df_src, "Özkaynaklar", dtype="int")
create_pivot(df_src, "Ödenmiş Sermaye", dtype="int")
create_pivot(df_src, "Toplam Aktifler", dtype="int")
create_pivot(df_src, "Net Borç", dtype="int")
create_pivot(df_src, "Yıllık Net Kar", dtype="int")

# --- Karlılık tabloları ---
create_pivot(df_src, "Öz Karlılık (%)", dtype="float2")
create_pivot(df_src, "Aktif Karlılık (%)", dtype="float2")

# --- İşlenmiş tablolar ---
create_pivot(df_src, "PD Çarpan", dtype="float5")
create_pivot(df_src, "F/K Çarpan", dtype="float5")

print("🔎 Tüm pivot tablolar artifact olarak üretildi.")

# --- Dikey tablo oluşturma ---
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

def create_latest_vertical(latest_values):
    last_date = list(latest_values.values())[0]["Tarih"]

    rows = []
    for hisse in df_src["Hisse Kodu"].unique():
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

    df_vert = pd.DataFrame(rows, columns=headers)
    artifact_path = "vertical_latest.xlsx"
    df_vert.to_excel(artifact_path, index=False, engine="openpyxl")
    print(f"✅ Dikey tablo artifact oluşturuldu: {artifact_path}")

# --- Çalıştır ---
create_latest_vertical(latest_values)
