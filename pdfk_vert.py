import requests,pandas as pd,numpy as np
from io import BytesIO; from datetime import datetime
import os
from tarih_ayar import secili_tarihleri_bul  # tarih_ayar dosyasından alındı

BASE_URL=os.environ.get("PDFK")

def parse_excel(url,tarih,hedef):
    r=requests.get(url,timeout=15); r.raise_for_status()
    df=pd.read_excel(BytesIO(r.content),header=None); rows=[]
    for j in range(len(df)):
        kod=str(df.iat[j,1]).strip().upper()
        if not kod or kod=="NAN" or kod not in hedef: continue
        rows.append({"Tarih":tarih,"Hisse_Kodu":kod,"Msci":"MSCI" if pd.notna(df.iat[j,6]) else "",
                     "Ozkaynak":df.iat[j,7],"Sermaye":df.iat[j,8],"Aktifler":df.iat[j,9],
                     "Netborc":df.iat[j,14],"Yillik_Kar":df.iat[j,17]})
    return pd.DataFrame(rows)

def main():
    df_dates=pd.read_csv("data/dates.csv",encoding="utf-8")
    tarih_list=df_dates.iloc[:,0].dropna().astype(str).str.strip().tolist()
    codes=df_dates.iloc[:,1].dropna().astype(str).str.strip().str.upper().tolist()

    # tarih_ayar.py’den secili_tarihleri_bul çağrılıyor
    secili=secili_tarihleri_bul(tarih_list)

    dfs=[]
    for d in secili:
        url=f"{BASE_URL}/ZRY Göstergeler-{datetime.strptime(d,'%d.%m.%Y').strftime('%Y_%m_%d')}.xlsx"
        try:
            df=parse_excel(url,d,set(codes))
            if not df.empty: dfs.append(df)
        except Exception as e: print(f"Excel okunamadı ({d}): hata: {e}")
    if not dfs: raise ValueError("❌ Hiç veri bulunamadı, pdfk_vert.xlsx oluşturulamadı.")
    df_final=pd.concat(dfs,ignore_index=True)
    df_final["Hisse_Kodu"]=df_final["Hisse_Kodu"].str.strip().str.upper()
    df_final=df_final[df_final["Hisse_Kodu"].isin(set(codes))].drop_duplicates(subset=["Tarih","Hisse_Kodu"])
    oz,se,ak,nb,yk=[pd.to_numeric(df_final[c],errors="coerce")*1_000_000 for c in ["Ozkaynak","Sermaye","Aktifler","Netborc","Yillik_Kar"]]
    df_final["Pd_Carpan"]=np.where(oz!=0,se/oz,np.nan).round(5)
    df_final["Fk_Carpan"]=np.where(yk!=0,se/yk,np.nan).round(5)
    df_final["Ozkarlilik"]=np.where(oz!=0,(yk/oz)*100,np.nan).round(2)
    df_final["Aktifkarlilik"]=np.where(ak!=0,(yk/ak)*100,np.nan).round(2)
    for col,ser in zip(["Ozkaynak","Sermaye","Aktifler","Netborc","Yillik_Kar"],[oz,se,ak,nb,yk]):
        df_final[col]=ser.apply(lambda x:"" if pd.isna(x) else str(int(x)))
    df_final["Tarih"]=pd.to_datetime(df_final["Tarih"],format="%d.%m.%Y",errors="coerce")
    df_final=df_final.sort_values(by=["Tarih","Hisse_Kodu"],ascending=[False,True]).reset_index(drop=True)
    df_final["Tarih"]=df_final["Tarih"].dt.strftime("%d.%m.%Y")
    df_final.to_excel("pdfk_vert.xlsx",index=False,engine="openpyxl")
    print("✅ Artifact oluşturuldu: pdfk_vert.xlsx")

if __name__=="__main__": main()
