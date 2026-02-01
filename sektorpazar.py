import pandas as pd, requests, os, json
from io import BytesIO
from bs4 import BeautifulSoup

urls=json.loads(os.environ.get("SEKTORPAZAR"))
url_indices,url_markets,url_fd=urls["url1"],urls["url2"],urls["url3"]

df_dates=pd.read_csv("data/dates.csv")
hisse_list=df_dates["Hisse"].dropna().str.strip().unique()

df_raw=pd.read_excel(BytesIO(requests.get(url_indices,timeout=30).content),header=None)
endeks_dict={}; cur=None
for i in range(len(df_raw)-1):
    vals=[str(df_raw.iat[i,j]).strip() if pd.notna(df_raw.iat[i,j]) else "" for j in range(min(5,df_raw.shape[1]))]
    nxt=str(df_raw.iat[i+1,0]).strip() if pd.notna(df_raw.iat[i+1,0]) else ""
    baslik=next((v for v in vals if v.upper().startswith("BIST")),None)
    if baslik and nxt=="1": cur=baslik; endeks_dict[cur]=[]; continue
    if vals and vals[0].isdigit() and cur: endeks_dict[cur].append(vals[1] if len(vals)>1 else "")
df_endeks=pd.DataFrame({k:pd.Series(v) for k,v in endeks_dict.items()})

df_m=pd.read_excel(BytesIO(requests.get(url_markets,timeout=30).content),header=None)
pazar_dict={}; cur=None
for i in range(len(df_m)):
    a=str(df_m.iat[i,0]).strip() if pd.notna(df_m.iat[i,0]) else ""
    b=str(df_m.iat[i,1]).strip() if pd.notna(df_m.iat[i,1]) else ""
    if any(x in a.upper() for x in["PAZAR","PİYASA"]): cur=a; pazar_dict[cur]=[]; continue
    if a.isdigit() and cur: pazar_dict[cur].append(b)
df_pazar=pd.DataFrame({k:pd.Series(v) for k,v in pazar_dict.items()})

soup=BeautifulSoup(requests.get(url_fd,timeout=30).text,"html.parser")
rows=[[td.get_text(strip=True) for td in tr.find_all("td")] for tr in soup.find("table").find_all("tr")]
data=[]
for r in rows:
    if len(r)==4: kod,tutar,oran=r[1].strip(),r[2].strip(),r[3].strip()
    elif len(r)==3: kod,tutar,oran=r[0].strip(),r[1].strip(),r[2].strip()
    else: continue
    if kod: data.append([kod,tutar,oran])
df_fd=pd.DataFrame(data,columns=["Borsa Kodu","Fiili Dolaşımdaki Pay Tutarı(TL)","Fiili Dolaşımdaki Pay Oranı(%)"])

df_sektor=pd.read_csv("data/sektor.csv")
df_pazar_map=pd.read_csv("data/pazar.csv")

res=[]
for kod in hisse_list:
    if kod in set(df_endeks["BIST 30"].dropna().astype(str).str.strip()): endeks="XU030"
    elif kod in set(df_endeks["BIST 50"].dropna().astype(str).str.strip()): endeks="XU050"
    elif kod in set(df_endeks["BIST 100"].dropna().astype(str).str.strip()): endeks="XU100"
    else: endeks=""
    sektor=""
    for _,r in df_sektor.iterrows():
        if r["Endeks Kolonu"].strip() in df_endeks.columns and kod in set(df_endeks[r["Endeks Kolonu"].strip()].dropna().astype(str).str.strip()):
            sektor=r["Sektor Kodu"].strip(); break
    pazar=""
    for _,r in df_pazar_map.iterrows():
        if r["Pattern"].strip() in df_pazar.columns and kod in set(df_pazar[r["Pattern"].strip()].dropna().astype(str).str.strip()):
            pazar=r["Pazar Kodu"].strip(); break
    fd=df_fd[df_fd["Borsa Kodu"]==kod]
    oran=fd.iloc[0]["Fiili Dolaşımdaki Pay Oranı(%)"] if not fd.empty else ""
    lot=fd.iloc[0]["Fiili Dolaşımdaki Pay Tutarı(TL)"] if not fd.empty else ""
    res.append([kod,pazar,endeks,sektor,oran,lot])

artifact=pd.DataFrame(res,columns=["Hisse","Pazar","Endeks","Sektör","Dolaşım Oranı","Dolaşım Lotu"])
artifact.to_excel("sektorpazar.xlsx", index=False, engine="openpyxl")
print("✅ sektorpazar.xlsx oluşturuldu.")

