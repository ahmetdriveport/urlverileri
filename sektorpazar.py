import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from io import BytesIO
from bs4 import BeautifulSoup

load_dotenv()
secret_json = os.getenv("SEKTORPAZAR")
secret = json.loads(secret_json)

url1 = secret["url1"]  # Endeksler
url2 = secret["url2"]  # Pazarlar
url3 = secret["url3"]  # Fiili dolaşım

df_dates = pd.read_csv("data/dates.csv")
df_sektor = pd.read_csv("data/sektor.csv")
df_pazar = pd.read_csv("data/pazar.csv")

# --- Endeksler ---
resp1 = requests.get(url1, timeout=30)
resp1.raise_for_status()
df_endeks = pd.read_excel(BytesIO(resp1.content), header=None)

# --- Pazarlar ---
resp2 = requests.get(url2, timeout=30)
resp2.raise_for_status()
df_markets = pd.read_excel(BytesIO(resp2.content), header=None)

# --- Fiili dolaşım ---
resp3 = requests.get(url3, timeout=30)
resp3.raise_for_status()
soup = BeautifulSoup(resp3.text, "html.parser")
table = soup.find("table")

rows_fd = []
for tr in table.find_all("tr"):
    tds = [td.get_text(strip=True) for td in tr.find_all("td")]
    if tds:
        rows_fd.append(tds)
df_fd = pd.DataFrame(rows_fd)

# --- Artifact üretimi ---
results = []
for kod in df_dates["Hisse"].unique():
    endeks = ""
    if kod in set(df_dates[df_dates["Endeks"]=="BIST30"]["Hisse"]):
        endeks = "XU030"
    elif kod in set(df_dates[df_dates["Endeks"]=="BIST50"]["Hisse"]):
        endeks = "XU050"
    elif kod in set(df_dates[df_dates["Endeks"]=="BIST100"]["Hisse"]):
        endeks = "XU100"

    sektor = ""
    for _, row in df_sektor.iterrows():
        if kod in df_dates[df_dates["Endeks"]==row["Endeks Kolonu"]]["Hisse"].values:
            sektor = row["Sektor Kodu"]
            break

    pazar = ""
    for _, row in df_pazar.iterrows():
        if row["Pattern"] in df_markets.values:
            pazar = row["Pazar Kodu"]
            break

    dolasim_oran, dolasim_lot = "", ""
    fd_row = df_fd[df_fd.iloc[:,0] == kod]
    if not fd_row.empty:
        dolasim_oran = fd_row.iloc[0,-1]
        dolasim_lot  = fd_row.iloc[0,-2]

    results.append([kod, pazar, endeks, sektor, dolasim_oran, dolasim_lot])

artifact = pd.DataFrame(results, columns=[
    "Hisse","Pazar","Endeks","Sektör","Dolaşım Oranı","Dolaşım Lotu"
])
artifact.to_csv("artifact.csv", index=False)
print("✅ artifact.csv oluşturuldu.")
