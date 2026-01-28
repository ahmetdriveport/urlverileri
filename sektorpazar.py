import pandas as pd
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from itertools import zip_longest
import numpy as np

# --- Hisse listesi ---
df_dates = pd.read_csv("data/dates.csv")
hisse_list = df_dates["Hisse"].dropna().str.strip().unique()

# --- Endeksler (KAP URL) ---
url_indices = "https://kap.org.tr/tr/api/company/indices/excel"
resp = requests.get(url_indices, timeout=30)
resp.raise_for_status()
df_raw = pd.read_excel(BytesIO(resp.content), header=None)

endeks_dict = {}
current_endeks = None
for i in range(len(df_raw) - 1):
    check_cols = min(5, df_raw.shape[1])
    row_vals = [
        str(df_raw.iat[i, j]).strip() if pd.notna(df_raw.iat[i, j]) else ""
        for j in range(check_cols)
    ]
    next_a = str(df_raw.iat[i + 1, 0]).strip() if pd.notna(df_raw.iat[i + 1, 0]) else ""
    baslik = next((v for v in row_vals if v.upper().startswith("BIST")), None)
    if baslik and next_a == "1":
        current_endeks = baslik
        endeks_dict[current_endeks] = []
        continue
    val_a = row_vals[0] if row_vals else ""
    val_b = row_vals[1] if len(row_vals) > 1 else ""
    if val_a.isdigit() and current_endeks:
        endeks_dict[current_endeks].append(val_b)

df_endeks = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in endeks_dict.items()]))

# --- Pazarlar (KAP URL) ---
url_markets = "https://kap.org.tr/tr/api/company/markets/excel"
resp_m = requests.get(url_markets, timeout=30)
resp_m.raise_for_status()
df_markets = pd.read_excel(BytesIO(resp_m.content), header=None)

pazar_dict = {}
current_pazar = None
for i in range(len(df_markets)):
    val_a = str(df_markets.iat[i, 0]).strip() if pd.notna(df_markets.iat[i, 0]) else ""
    val_b = str(df_markets.iat[i, 1]).strip() if pd.notna(df_markets.iat[i, 1]) else ""
    if any(keyword in val_a.upper() for keyword in ["PAZAR", "PİYASA"]):
        current_pazar = val_a
        pazar_dict[current_pazar] = []
        continue
    if val_a.isdigit() and current_pazar:
        pazar_dict[current_pazar].append(val_b)

df_pazar = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in pazar_dict.items()]))

# --- Fiili dolaşım (KAP URL) ---
url_fd = "https://www.kap.org.tr/tr/tumKalemler/kpy41_acc5_fiili_dolasimdaki_pay"
resp_fd = requests.get(url_fd, timeout=30)
resp_fd.raise_for_status()
soup = BeautifulSoup(resp_fd.text, "html.parser")
table = soup.find("table")

rows_fd = []
for tr in table.find_all("tr"):
    tds = [td.get_text(strip=True) for td in tr.find_all("td")]
    if tds:
        rows_fd.append(tds)

data_rows = []
for row in rows_fd:
    if len(row) == 4:
        kod, tutar, oran = row[1].strip(), row[2].strip(), row[3].strip()
    elif len(row) == 3:
        kod, tutar, oran = row[0].strip(), row[1].strip(), row[2].strip()
    else:
        continue
    if kod:
        data_rows.append([kod, tutar, oran])

df_fd = pd.DataFrame(data_rows, columns=["Borsa Kodu","Fiili Dolaşımdaki Pay Tutarı(TL)","Fiili Dolaşımdaki Pay Oranı(%)"])

# --- Sektör ve Pazar mapping CSV'leri ---
df_sektor = pd.read_csv("data/sektor.csv")
df_pazar_map = pd.read_csv("data/pazar.csv")

# --- Artifact üretimi ---
results = []
for kod in hisse_list:
    # Endeks üyeliği
    if kod in set(df_endeks["BIST 30"].dropna().astype(str).str.strip()):
        endeks = "XU030"
    elif kod in set(df_endeks["BIST 50"].dropna().astype(str).str.strip()):
        endeks = "XU050"
    elif kod in set(df_endeks["BIST 100"].dropna().astype(str).str.strip()):
        endeks = "XU100"
    else:
        endeks = ""

    # Sektör
    sektor = ""
    for _, row in df_sektor.iterrows():
        sec_code = row["Sektor Kodu"].strip()
        colname = row["Endeks Kolonu"].strip()
        if colname in df_endeks.columns and kod in set(df_endeks[colname].dropna().astype(str).str.strip()):
            sektor = sec_code
            break

    # Pazar
    pazar = ""
    for _, row in df_pazar_map.iterrows():
        pattern = row["Pattern"].strip()
        pazar_code = row["Pazar Kodu"].strip()
        if pattern in df_pazar.columns and kod in set(df_pazar[pattern].dropna().astype(str).str.strip()):
            pazar = pazar_code
            break

    # Fiili dolaşım
    fd_row = df_fd[df_fd["Borsa Kodu"] == kod]
    dolasim_oran = fd_row.iloc[0]["Fiili Dolaşımdaki Pay Oranı(%)"] if not fd_row.empty else ""
    dolasim_lot  = fd_row.iloc[0]["Fiili Dolaşımdaki Pay Tutarı(TL)"] if not fd_row.empty else ""

    results.append([kod, pazar, endeks, sektor, dolasim_oran, dolasim_lot])

artifact = pd.DataFrame(results, columns=["Hisse","Pazar","Endeks","Sektör","Dolaşım Oranı","Dolaşım Lotu"])
artifact.to_csv("artifact.csv", index=False)
print("✅ artifact.csv oluşturuldu.")
