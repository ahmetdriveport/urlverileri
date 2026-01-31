import pandas as pd, numpy as np, yaml

def clean_numeric_series(s):
    s = s.astype(str).str.replace(r"[^\d,.-]", "", regex=True)
    s = s.str.replace(",", ".", regex=False)
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
        ser = df[col]
        first_valid = ser.first_valid_index()
        if first_valid is None:
            continue
        # İlk valid değerden önce NaN bırak
        ser.loc[ser.index < first_valid] = pd.NA
        df[col] = ser

    return df  # kronolojik olarak bırakıyoruz, çıktı aşamasında ters çevrilecek

def normalize(x):
    if pd.isna(x): return None
    try: return round(float(x), 2)
    except: return None

# EMA (SMA ile başlat, ilk n gün boş)
def ema_with_sma_start(series, length):
    s = pd.to_numeric(series, errors="coerce")
    if s.notna().sum() < length: 
        return pd.Series(index=s.index, dtype=float)
    ema = pd.Series(index=s.index, dtype=float)
    valid = s.dropna()
    sma_start = valid.iloc[:length].mean()
    ema.loc[valid.index[length-1]] = sma_start
    alpha = 2/(length+1)
    prev = sma_start
    for idx in valid.index[length:]:
        prev = alpha*valid.loc[idx] + (1-alpha)*prev
        ema.loc[idx] = prev
    return ema

# RSI (ilk period boş)
def calculate_rsi(series, period=14):
    s = pd.to_numeric(series, errors="coerce")
    delta = s.diff()
    gain, loss = delta.clip(lower=0), -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# MACD
def calculate_macd(series, fast=12, slow=26, signal=9):
    s = pd.to_numeric(series, errors="coerce")
    fast_ema = ema_with_sma_start(s, fast)
    slow_ema = ema_with_sma_start(s, slow)
    macd_line = fast_ema - slow_ema
    signal_line = macd_line.ewm(span=signal, adjust=False, min_periods=signal).mean()
    hist = macd_line - signal_line
    return pd.DataFrame({"MACD": macd_line, "SIGNAL": signal_line, "HIST": hist})

# Bollinger %B
def calculate_bbp(series, length=20, mult=2):
    s = pd.to_numeric(series, errors="coerce")
    ma = s.rolling(window=length, min_periods=length).mean()
    std = s.rolling(window=length, min_periods=length).std(ddof=0)
    upper, lower = ma + mult*std, ma - mult*std
    return ((s - lower) / (upper - lower))

# Williams %R
def calculate_williamsr(high, low, close, length=14):
    h, l, c = pd.to_numeric(high, errors="coerce"), pd.to_numeric(low, errors="coerce"), pd.to_numeric(close, errors="coerce")
    highest_high = h.rolling(window=length, min_periods=length).max()
    lowest_low = l.rolling(window=length, min_periods=length).min()
    wr = (highest_high - c) / (highest_high - lowest_low) * -100
    return wr

# Wilder RMA
def rma(series, length): 
    return series.ewm(alpha=1/length, adjust=False, min_periods=length).mean()

# DIOSC
def calculate_diosc(high, low, close, length=14):
    h, l, c = pd.to_numeric(high, errors="coerce"), pd.to_numeric(low, errors="coerce"), pd.to_numeric(close, errors="coerce")
    up, down = h.diff(), -l.diff()
    plus_dm = np.where((up>down)&(up>0), up, 0.0)
    minus_dm = np.where((down>up)&(down>0), down, 0.0)
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    truerange = rma(tr, length)
    plus = 100*rma(pd.Series(plus_dm, index=h.index), length)/truerange
    minus = 100*rma(pd.Series(minus_dm, index=h.index), length)/truerange
    return (plus-minus)

def hesapla_indikatorler(df, tanimlar):
    sonuc = {}
    for tanim in tanimlar:
        kind, params, output = tanim["kind"], tanim.get("params", {}), tanim["output"]
        try:
            if kind == "ema":
                sonuc[output] = ema_with_sma_start(df["close"], params.get("length",20)).apply(normalize)
            elif kind == "rsi":
                sonuc[output] = calculate_rsi(df["close"], params.get("length",14)).apply(normalize)
            elif kind == "macd":
                cikti = calculate_macd(df["close"], fast=params.get("fast",12), slow=params.get("slow",26), signal=params.get("signal",9))
                if isinstance(output, dict):
                    for kaynak, hedef in output.items():
                        sonuc[hedef] = cikti[kaynak].apply(normalize)
            elif kind == "bbp_manual":
                sonuc[output] = calculate_bbp(df["close"], params.get("length",20)).apply(normalize)
            elif kind == "williamsr":
                sonuc[output] = calculate_williamsr(df["high"], df["low"], df["close"], params.get("length",14)).apply(normalize)
            elif kind == "diosc":
                sonuc[output] = calculate_diosc(df["high"], df["low"], df["close"], params.get("length",14)).apply(normalize)
        except Exception as e:
            print(f"❌ {kind} hata: {e}")
            if isinstance(output, dict):
                for _, hedef in output.items():
                    sonuc[hedef] = pd.Series(index=df.index, dtype=float).apply(normalize)
            else:
                sonuc[output] = pd.Series(index=df.index, dtype=float).apply(normalize)
    return sonuc

def yukle_ayarlar(path="data/indicators.yaml"):
    with open(path,"r",encoding="utf-8") as f: 
        return yaml.safe_load(f)["indikatorler"]

def main():
    xls = pd.ExcelFile("fiyat.xlsx")
    df_close = pd.read_excel(xls, "Kapanış", index_col=0)
    df_high  = pd.read_excel(xls, "Yüksek", index_col=0)
    df_low   = pd.read_excel(xls, "Düşük", index_col=0)

    # Master tarihleri kronolojik sıraya sok
    master_dates = pd.to_datetime(df_close.index, dayfirst=True, errors="coerce").sort_values(ascending=True)

    # Sembolleri oku
    semboller = pd.read_csv("data/dates.csv", encoding="utf-8").iloc[:, 1].dropna().unique().tolist()

    sayfa_df = {}
    for sembol in semboller:
        try:
            # Sembol bazlı fiyat serisi, kronolojik sırada
            df_symbol = pd.DataFrame({
                "close": clean_numeric_series(df_close.get(sembol)),
                "high":  clean_numeric_series(df_high.get(sembol)),
                "low":   clean_numeric_series(df_low.get(sembol))
            }, index=pd.to_datetime(df_close.index, dayfirst=True, errors="coerce")).sort_index(ascending=True)

            # İndikatörleri hesapla
            sonuc = hesapla_indikatorler(df_symbol, yukle_ayarlar())
            if not sonuc:
                continue

            # Her indikatör için master tarih ile hizala
            for sayfa_adi, ser_out in sonuc.items():
                aligned = align_to_master(ser_out.to_frame(sembol), master_dates)
                aligned.index = aligned.index.strftime("%d.%m.%Y")
                if sayfa_adi not in sayfa_df:
                    sayfa_df[sayfa_adi] = {}
                sayfa_df[sayfa_adi][sembol] = aligned[sembol]

        except Exception as e:
            print(f"❌ {sembol} hata: {e}")

    # Excel çıktısı oluştur
    with pd.ExcelWriter("indicators.xlsx", engine="openpyxl", mode="w") as writer:
        for sayfa_adi, sembol_dict in sayfa_df.items():
            df_out = pd.concat(sembol_dict.values(), axis=1)
            df_out.columns = list(sembol_dict.keys())
            df_out.index.name = "Tarih"
            # Çıktıyı bugünden geçmişe doğru sırala
            df_out = df_out.sort_index(ascending=False)

            # ✅ Debug sadece final tablolar için
            print(f"🔍 [DEBUG] Final df_out sample (50 rows) for {sayfa_adi}:")
            print(df_out.head(50))

            df_out.to_excel(writer, sheet_name=sayfa_adi)

    print("✅ indicators.xlsx oluşturuldu")

if __name__ == "__main__":
    main()    
