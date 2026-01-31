import pandas as pd, numpy as np, yaml

def clean_numeric_series(s):
    print("🔍 [DEBUG] clean_numeric_series input:", s.head())
    s = s.astype(str).str.replace(r"[^\d,.-]", "", regex=True)
    s = s.str.replace(",", ".", regex=False)
    s = s.replace("", pd.NA).replace("nan", pd.NA).replace("<NA>", pd.NA)
    out = pd.to_numeric(s, errors="coerce")
    print("🔍 [DEBUG] clean_numeric_series output:", out.head())
    return out

def align_to_master(df, master_dates_desc, tarih_kolon="Tarih"):
    # Eğer dataframe'de tarih kolonu varsa önce datetime'a çevir
    if tarih_kolon in df.columns:
        df[tarih_kolon] = pd.to_datetime(df[tarih_kolon], dayfirst=True, errors="coerce")
        df = df.dropna(subset=[tarih_kolon]).set_index(tarih_kolon)

    # Hem master hem df index'i normalize et
    master_sorted = pd.to_datetime(master_dates_desc, dayfirst=True, errors="coerce").sort_values(ascending=True)
    df.index = pd.to_datetime(df.index, dayfirst=True, errors="coerce")

    # Master tarihleri ile reindex
    df = df.reindex(master_sorted)

    # İlk valid değerden önce NaN, sonrasında forward fill
    for col in df.columns:
        ser = df[col]
        first_valid = ser.first_valid_index()
        if first_valid is None:
            continue
        ser.loc[ser.index < first_valid] = pd.NA
        ser.loc[ser.index >= first_valid] = ser.loc[ser.index >= first_valid].ffill()
        df[col] = ser

    # Sonuç: tarihleri descending sırada döndür
    return df.sort_index(ascending=False)


def normalize(x):
    if pd.isna(x): return None
    try: return round(float(x), 2)
    except: return None

def ema_with_sma_start(series, length):
    s = pd.to_numeric(series, errors="coerce")
    if s.notna().sum() < length: return pd.Series(index=s.index, dtype=float)
    ema = pd.Series(index=s.index, dtype=float); valid = s.dropna()
    sma_start = valid.iloc[:length].mean(); ema.loc[valid.index[length-1]] = sma_start
    alpha = 2/(length+1); prev = sma_start
    for idx in valid.index[length:]:
        prev = alpha*valid.loc[idx] + (1-alpha)*prev; ema.loc[idx] = prev
    return ema

def calculate_rsi(series, period=14):
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < period: return pd.Series(index=series.index, dtype=float)
    delta = s.diff(); gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_gain, avg_loss = gain.iloc[:period].mean(), loss.iloc[:period].mean()
    rsi = pd.Series(index=s.index, dtype=float); rs = avg_gain/avg_loss if avg_loss!=0 else 0
    rsi.iloc[period] = 100 - (100/(1+rs))
    for i in range(period+1, len(s)):
        avg_gain = (avg_gain*(period-1)+gain.iloc[i])/period
        avg_loss = (avg_loss*(period-1)+loss.iloc[i])/period
        rs = avg_gain/avg_loss if avg_loss!=0 else 0
        rsi.iloc[i] = 100 - (100/(1+rs))
    return rsi.reindex(series.index)

def calculate_macd(series, fast=12, slow=26, signal=9):
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty: return pd.DataFrame(index=series.index, columns=["MACD","SIGNAL","HIST"], dtype=float)
    ema_fast = s.ewm(span=fast, min_periods=fast).mean()
    ema_slow = s.ewm(span=slow, min_periods=slow).mean()
    macd_line = ema_fast - ema_slow; signal_line = macd_line.ewm(span=signal, min_periods=signal).mean()
    hist = macd_line - signal_line
    return pd.DataFrame({"MACD": macd_line, "SIGNAL": signal_line, "HIST": hist}).reindex(series.index)

def calculate_bbp(series, length=20):
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < length: return pd.Series(index=series.index, dtype=float)
    ma = s.rolling(window=length, min_periods=length).mean()
    std = s.rolling(window=length, min_periods=length).std()
    upper, lower = ma+2*std, ma-2*std
    return ((s-lower)/(upper-lower)).reindex(series.index)

def calculate_williamsr(high, low, close, length=14):
    h, l, c = pd.to_numeric(high, errors="coerce"), pd.to_numeric(low, errors="coerce"), pd.to_numeric(close, errors="coerce")
    if len(c.dropna()) < length: return pd.Series(index=c.index, dtype=float)
    wr = (h.rolling(length).max()-c)/(h.rolling(length).max()-l.rolling(length).min())*-100
    return wr.reindex(c.index)

def rma(series, length): return series.ewm(alpha=1/length, adjust=False).mean()

def calculate_diosc(high, low, close, length=14):
    h, l, c = pd.to_numeric(high, errors="coerce"), pd.to_numeric(low, errors="coerce"), pd.to_numeric(close, errors="coerce")
    if len(c.dropna()) < length: return pd.Series(index=c.index, dtype=float)
    up, down = h.diff(), -l.diff()
    plus_dm = np.where((up>down)&(up>0), up, 0.0); minus_dm = np.where((down>up)&(down>0), down, 0.0)
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    truerange = rma(tr, length)
    plus = 100*rma(pd.Series(plus_dm, index=h.index), length)/truerange
    minus = 100*rma(pd.Series(minus_dm, index=h.index), length)/truerange
    return (plus-minus).reindex(c.index)

def hesapla_indikatorler(df, tanimlar):
    print("🔍 [DEBUG] hesapla_indikatorler input df (50 rows):", df.head(50))
    sonuc = {}
    for tanim in tanimlar:
        kind, params, output = tanim["kind"], tanim.get("params", {}), tanim["output"]
        try:
            if kind == "ema":
                sonuc[output] = ema_with_sma_start(df["close"], params.get("length",20)).apply(normalize)
                print(f"🔍 [DEBUG] ema output head (50 rows):", sonuc[output].head(50))
                print(f"🔍 [DEBUG] ema output tail (50 rows):", sonuc[output].tail(50))

            elif kind == "rsi":
                sonuc[output] = calculate_rsi(df["close"], params.get("length",14)).apply(normalize)
                print(f"🔍 [DEBUG] rsi output head (50 rows):", sonuc[output].head(50))
                print(f"🔍 [DEBUG] rsi output tail (50 rows):", sonuc[output].tail(50))

            elif kind == "macd":
                cikti = calculate_macd(
                    df["close"],
                    fast=params.get("fast",12),
                    slow=params.get("slow",26),
                    signal=params.get("signal",9)
                )
                if isinstance(output, dict):
                    for kaynak, hedef in output.items():
                        sonuc[hedef] = cikti[kaynak].apply(normalize)
                        print(f"🔍 [DEBUG] macd {kaynak} → {hedef} head (50 rows):", sonuc[hedef].head(50))
                        print(f"🔍 [DEBUG] macd {kaynak} → {hedef} tail (50 rows):", sonuc[hedef].tail(50))

            elif kind == "bbp_manual":
                sonuc[output] = calculate_bbp(df["close"], params.get("length",20)).apply(normalize)
                print(f"🔍 [DEBUG] bbp output head (50 rows):", sonuc[output].head(50))
                print(f"🔍 [DEBUG] bbp output tail (50 rows):", sonuc[output].tail(50))

            elif kind == "williamsr":
                sonuc[output] = calculate_williamsr(df["high"], df["low"], df["close"], params.get("length",14)).apply(normalize)
                print(f"🔍 [DEBUG] williamsr output head (50 rows):", sonuc[output].head(50))
                print(f"🔍 [DEBUG] williamsr output tail (50 rows):", sonuc[output].tail(50))

            elif kind == "diosc":
                sonuc[output] = calculate_diosc(df["high"], df["low"], df["close"], params.get("length",14)).apply(normalize)
                print(f"🔍 [DEBUG] diosc output head (50 rows):", sonuc[output].head(50))
                print(f"🔍 [DEBUG] diosc output tail (50 rows):", sonuc[output].tail(50))

        except Exception as e:
            print(f"❌ {kind} hata: {e}")
            # hata durumunda boş seri oluştur
            if isinstance(output, dict):
                for _, hedef in output.items():
                    sonuc[hedef] = pd.Series(index=df.index, dtype=float).apply(normalize)
            else:
                sonuc[output] = pd.Series(index=df.index, dtype=float).apply(normalize)
    return sonuc

def yukle_ayarlar(path="data/indicators.yaml"):
    with open(path,"r",encoding="utf-8") as f: return yaml.safe_load(f)["indikatorler"]

def main():
    xls = pd.ExcelFile("fiyat.xlsx")
    df_close = pd.read_excel(xls, "Kapanış", index_col=0)
    df_high  = pd.read_excel(xls, "Yüksek", index_col=0)
    df_low   = pd.read_excel(xls, "Düşük", index_col=0)
    print("🔍 [DEBUG] Ham df_close head (50 rows):", df_close.head(50))

    master_dates = pd.to_datetime(df_close.index, dayfirst=True, errors="coerce")
    semboller = pd.read_csv("data/dates.csv", encoding="utf-8").iloc[:, 1].dropna().unique().tolist()
    print("🔍 [DEBUG] Semboller:", semboller)

    sayfa_df = {}
    for sembol in semboller:
        try:
            df_symbol = pd.DataFrame({
                "close": clean_numeric_series(df_close.get(sembol)),
                "high":  clean_numeric_series(df_high.get(sembol)),
                "low":   clean_numeric_series(df_low.get(sembol))
            }, index=df_close.index).sort_index()

            # 🔍 Artık 50 satır gösteriyoruz
            print(f"🔍 [DEBUG] df_symbol sample (50 rows) for {sembol}:", df_symbol.head(50))

            sonuc = hesapla_indikatorler(df_symbol, yukle_ayarlar())
            if not sonuc:
                continue

            for sayfa_adi, ser_out in sonuc.items():
                aligned = align_to_master(ser_out.to_frame(sembol), master_dates)
                print(f"🔍 [DEBUG] aligned sample (50 rows) for {sayfa_adi}/{sembol}:", aligned.head(50))
                aligned.index = aligned.index.strftime("%d.%m.%Y")
                if sayfa_adi not in sayfa_df:
                    sayfa_df[sayfa_adi] = {}
                sayfa_df[sayfa_adi][sembol] = aligned[sembol]

        except Exception as e:
            print(f"❌ {sembol} hata: {e}")

    with pd.ExcelWriter("indicators.xlsx", engine="openpyxl", mode="w") as writer:
        for sayfa_adi, sembol_dict in sayfa_df.items():
            df_out = pd.concat(sembol_dict.values(), axis=1)
            df_out.columns = list(sembol_dict.keys())
            df_out.index.name = "Tarih"
            print(f"🔍 [DEBUG] Final df_out sample (50 rows) for {sayfa_adi}:", df_out.head(50))
            df_out.to_excel(writer, sheet_name=sayfa_adi)

    print("✅ indicators.xlsx oluşturuldu")

if __name__ == "__main__":
    main()
    
