import pandas as pd, numpy as np

try:
    df=pd.read_excel("fiyat.xlsx")
    for col in df.columns:
        if col!="Tarih":
            df[col]=df[col].astype(str).str.replace(",",".",regex=False).replace("",None)
            df[col]=pd.to_numeric(df[col],errors="coerce")
    df["Tarih"]=pd.to_datetime(df["Tarih"],dayfirst=True,errors="coerce")
    df=df.sort_values("Tarih")
    today=df["Tarih"].max(); today_str=today.strftime("%d.%m.%Y")

    def find_nearest_open(series,target_date):
        valid=series.index[series.index<=target_date]
        return valid.max() if len(valid)>0 else None
    def safe_div(a,b):
        return np.nan if b is None or pd.isna(b) or b==0 else (a/b-1)*100
    def compute_returns(df,col,today,today_str):
        s=df.set_index("Tarih")[col].astype(float)
        if today not in s.index or pd.isna(s.loc[today]):
            return [today_str,col,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan]
        fiyat_today=s.loc[today]
        d=find_nearest_open(s,today-pd.DateOffset(days=1)); daily=safe_div(fiyat_today,s.loc[d]) if d is not None else np.nan
        w=find_nearest_open(s,today-pd.DateOffset(weeks=1)); weekly=safe_div(fiyat_today,s.loc[w]) if w is not None else np.nan
        m=find_nearest_open(s,today-pd.DateOffset(months=1)); monthly=safe_div(fiyat_today,s.loc[m]) if m is not None else np.nan
        sm=find_nearest_open(s,today-pd.DateOffset(months=6)); semi=safe_div(fiyat_today,s.loc[sm]) if sm is not None else np.nan
        y=find_nearest_open(s,today-pd.DateOffset(years=1)); yearly=safe_div(fiyat_today,s.loc[y]) if y is not None else np.nan
        year_start=today-pd.DateOffset(years=1); mask=(s.index>=year_start)&(s.index<=today)
        if mask.sum()==0: year_low,year_high,max_gain_loss,tl_konum=np.nan,np.nan,np.nan,np.nan
        else:
            year_low,year_high=s.loc[mask].min(),s.loc[mask].max()
            low_date,high_date=s.loc[mask].idxmin(),s.loc[mask].idxmax()
            max_gain_loss=np.nan if pd.isna(year_low) or pd.isna(year_high) or year_low==0 else \
                (year_high/year_low-1)*100 if low_date<high_date else (year_low/year_high-1)*100
            tl_konum=np.nan if pd.isna(year_low) or pd.isna(year_high) or year_high==year_low else \
                (fiyat_today-year_low)/(year_high-year_low)*100
        return [today_str,col,fiyat_today,daily,weekly,monthly,semi,yearly,year_low,year_high,max_gain_loss,tl_konum]

    results={col:compute_returns(df,col,today,today_str) for col in df.columns if col!="Tarih"}
    returns_table=pd.DataFrame.from_dict(results,orient="index",
        columns=["Tarih","Hisse Kodu","Fiyat","Günlük%","Haftalık%","Aylık%","6 Aylık%","Yıllık%",
                 "Yıl Düşük","Yıl Yüksek","Max Kar/Zarar","TL Konum"])
    for c in returns_table.columns[2:]:
        returns_table[c]=pd.to_numeric(returns_table[c],errors="coerce").round(2)
    returns_table.reset_index(drop=True,inplace=True)
    returns_table.to_excel("profit.xlsx",index=False,engine="openpyxl")
    print("✅ profit.xlsx yazıldı")
except:
    print("❌ profit.xlsx yazılamadı")
