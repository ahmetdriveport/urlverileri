import pandas as pd, numpy as np, yaml

def clean_numeric_series(s):
    s = s.astype(str).str.replace(r"[^\d,.-]", "", regex=True).str.replace(",", ".", regex=False)
    s = s.replace("", pd.NA).replace("nan", pd.NA).replace("<NA>", pd.NA)
    return pd.to_numeric(s, errors="coerce")

def align_to_master(df, master_dates, tarih_kolon="Tarih"):
    if tarih_kolon in df.columns:
        df[tarih_kolon] = pd.to_datetime(df[tarih_kolon], dayfirst=True, errors="coerce")
        df = df.dropna(subset=[tarih_kolon]).set_index(tarih_kolon)
    master_sorted = pd.to_datetime(master_dates, dayfirst=True, errors="coerce").sort_values(ascending=True)
    df.index = pd.to_datetime(df.index, dayfirst=True, errors="coerce")
    df = df.reindex(master_sorted)
    for col in df.columns:
        ser = df[col]; first_valid = ser.first_valid_index()
        if first_valid is not None:
            ser.loc[ser.index < first_valid] = pd.NA
            df[col] = ser
    return df

def normalize(x): 
    if pd.isna(x): return None
    try: return round(float(x),2)
    except: return None

def ema_with_sma_start(series,length):
    s=pd.to_numeric(series,errors="coerce")
    if s.notna().sum()<length: return pd.Series(index=s.index,dtype=float)
    ema=pd.Series(index=s.index,dtype=float); valid=s.dropna()
    sma_start=valid.iloc[:length].mean(); ema.loc[valid.index[length-1]]=sma_start
    alpha=2/(length+1); prev=sma_start
    for idx in valid.index[length:]:
        prev=alpha*valid.loc[idx]+(1-alpha)*prev
        ema.loc[idx]=prev
    return ema

def calculate_rsi(series,period=14):
    s=pd.to_numeric(series,errors="coerce"); d=s.diff()
    g,l=d.clip(lower=0),-d.clip(upper=0)
    ag=g.ewm(alpha=1/period,adjust=False,min_periods=period).mean()
    al=l.ewm(alpha=1/period,adjust=False,min_periods=period).mean()
    rs=ag/al; return 100-(100/(1+rs))

def calculate_macd(series,fast=12,slow=26,signal=9):
    s=pd.to_numeric(series,errors="coerce")
    f=ema_with_sma_start(s,fast); sl=ema_with_sma_start(s,slow)
    m=f-sl; sig=m.ewm(span=signal,adjust=False,min_periods=signal).mean()
    return pd.DataFrame({"MACD":m,"SIGNAL":sig,"HIST":m-sig})

def calculate_bbp(series,length=20,mult=2):
    s=pd.to_numeric(series,errors="coerce")
    ma=s.rolling(window=length,min_periods=length).mean()
    std=s.rolling(window=length,min_periods=length).std(ddof=0)
    up,lo=ma+mult*std,ma-mult*std
    return (s-lo)/(up-lo)

def calculate_williamsr(h,l,c,length=14):
    h,l,c=pd.to_numeric(h,errors="coerce"),pd.to_numeric(l,errors="coerce"),pd.to_numeric(c,errors="coerce")
    hh=h.rolling(window=length,min_periods=length).max(); ll=l.rolling(window=length,min_periods=length).min()
    return (hh-c)/(hh-ll)*-100

def rma(series,length): return series.ewm(alpha=1/length,adjust=False,min_periods=length).mean()

def calculate_diosc(h,l,c,length=14):
    h,l,c=pd.to_numeric(h,errors="coerce"),pd.to_numeric(l,errors="coerce"),pd.to_numeric(c,errors="coerce")
    up,down=h.diff(),-l.diff()
    plus=np.where((up>down)&(up>0),up,0.0); minus=np.where((down>up)&(down>0),down,0.0)
    tr=pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1)
    trr=rma(tr,length)
    p=100*rma(pd.Series(plus,index=h.index),length)/trr
    m=100*rma(pd.Series(minus,index=h.index),length)/trr
    return (p-m)

def hesapla_indikatorler(df,tanimlar):
    sonuc={}
    for t in tanimlar:
        k,p,o=t["kind"],t.get("params",{}),t["output"]
        try:
            if k=="ema": sonuc[o]=ema_with_sma_start(df["close"],p.get("length",20)).apply(normalize)
            elif k=="rsi": sonuc[o]=calculate_rsi(df["close"],p.get("length",14)).apply(normalize)
            elif k=="macd":
                c=calculate_macd(df["close"],fast=p.get("fast",12),slow=p.get("slow",26),signal=p.get("signal",9))
                if isinstance(o,dict): 
                    for src,dst in o.items(): sonuc[dst]=c[src].apply(normalize)
            elif k=="bbp_manual": sonuc[o]=calculate_bbp(df["close"],p.get("length",20)).apply(normalize)
            elif k=="williamsr": sonuc[o]=calculate_williamsr(df["high"],df["low"],df["close"],p.get("length",14)).apply(normalize)
            elif k=="diosc": sonuc[o]=calculate_diosc(df["high"],df["low"],df["close"],p.get("length",14)).apply(normalize)
        except:
            sonuc[o]=pd.Series(index=df.index,dtype=float).apply(normalize)
    return sonuc

def yukle_ayarlar(path="data/indicators.yaml"):
    with open(path,"r",encoding="utf-8") as f:
        return yaml.safe_load(f)["indikatorler"]

def main():
    try:
        # Artık main_indis_fiyat.xlsx dosyasını okuyacak
        xls = pd.ExcelFile("main_indis_fiyat.xlsx")
        dfc = pd.read_excel(xls, "Kapanış", index_col=0)
        dfh = pd.read_excel(xls, "Yüksek", index_col=0)
        dfl = pd.read_excel(xls, "Düşük", index_col=0)

        dfc.index = pd.to_datetime(dfc.index, dayfirst=True, errors="raise")
        dfh.index = pd.to_datetime(dfh.index, dayfirst=True, errors="raise")
        dfl.index = pd.to_datetime(dfl.index, dayfirst=True, errors="raise")
        master = dfc.index.sort_values(ascending=True)

        endeksler = pd.read_csv("data/dates.csv", encoding="utf-8").iloc[:, 2].dropna().unique().tolist()
        sayfa_df = {}

        for e in endeksler:
            try:
                dfs = pd.DataFrame({
                    "close": clean_numeric_series(dfc.get(e)),
                    "high": clean_numeric_series(dfh.get(e)),
                    "low": clean_numeric_series(dfl.get(e))
                }, index=master).sort_index(ascending=True)

                sonuc = hesapla_indikatorler(dfs, yukle_ayarlar())
                for sa, ser in sonuc.items():
                    al = align_to_master(ser.to_frame(e), master)
                    if sa not in sayfa_df:
                        sayfa_df[sa] = {}
                    sayfa_df[sa][e] = al[e]
            except:
                pass

        with pd.ExcelWriter("indis_indicators.xlsx", engine="openpyxl", mode="w") as w:
            for sa, sd in sayfa_df.items():
                df_out = pd.concat(sd.values(), axis=1)
                df_out.columns = list(sd.keys())
                df_out.index.name = "Tarih"
                df_out = df_out.sort_index(ascending=False)
                df_out.to_excel(w, sheet_name=sa)

        print("✅ indis_indicators.xlsx oluşturuldu")
    except:
        print("❌ indis_indicators.xlsx oluştur")
