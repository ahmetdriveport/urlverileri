import requests, pandas as pd, json, os
from bs4 import BeautifulSoup
from collections import defaultdict
from tarih_ayar import secili_tarihleri_bul

def temizle_sayi(s):
    s = str(s).strip()
    if not s:
        return None
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    elif "." in s and len(s.split(".")[-1]) > 2:
        s = s.replace(".", "")
    try:
        return float(s)
    except:
        return None

BASE_URL = json.loads(os.environ["MAININDIS"])["DATA_SOURCE_URL"]

def kapanis_tek_gun(g, a, y):
    try:
        r = requests.get(
            f"{BASE_URL}&gun={g}&ay={a}&yil={y}",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        r.raise_for_status()
    except:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    t = soup.find("table")
    if not t:
        return []

    # Başlıkları al
    headers = [th.get_text(strip=True) for th in t.find_all("tr")[0].find_all("td")]
    ts = f"{g:02d}.{a:02d}.{y}"

    rows = []
    for tr in t.find_all("tr")[1:]:
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) != len(headers):
            continue
        row = dict(zip(headers, cells))

        try:
            rows.append({
                "Tarih": ts,
                "Endeks": row.get("Menkul Adı"),
                "Kapanış": temizle_sayi(row.get("Son")),
                "Yüksek": temizle_sayi(row.get("Yüksek")),
                "Düşük": temizle_sayi(row.get("Düşük")),
            })
        except:
            continue

    return rows

try:
    df_csv = pd.read_csv("data/dates.csv", encoding="utf-8")
    secili = list(reversed(secili_tarihleri_bul(df_csv["Tarih"].dropna().tolist(), 150)))
    takip = df_csv.iloc[:, 2].dropna().unique().tolist()
    tum = [v for t in secili for v in kapanis_tek_gun(*map(int, t.split(".")))]
    vg = defaultdict(list)
    [vg[v["Tarih"]].append(v) for v in tum]
    ilk = {}
    [ilk.setdefault(v["Endeks"], pd.to_datetime(v["Tarih"], dayfirst=True)) for v in tum]
    endeksler = sorted({v["Endeks"] for v in tum})
    onceki = {}
    final = []
    for t in secili:
        td = pd.to_datetime(t, dayfirst=True)
        gv = {v["Endeks"]: v for v in vg.get(t, [])}
        sat = []
        for e in endeksler:
            if e in gv:
                sat.append(gv[e])
            elif e in onceki and td >= ilk[e]:
                k = onceki[e].copy()
                k["Tarih"] = t
                sat.append(k)
        onceki.update(gv)
        final.extend(sat)
    df = pd.DataFrame(final)

    def pivotla(df, kolon, takip, tarih, haftalik=False):
        df["Tarih"] = pd.to_datetime(df["Tarih"], dayfirst=True, errors="coerce")
        df = df[
            df["Tarih"].isin(pd.to_datetime(tarih, dayfirst=True))
        ].dropna(subset=["Tarih", "Endeks", kolon])
        df[kolon] = df[kolon].apply(temizle_sayi)
        if haftalik:
            df["Hafta"] = df["Tarih"].dt.to_period("W")
            df["Gun"] = df["Tarih"].dt.dayofweek
            tercih = [4, 3, 2, 1, 0]
            haftalik = []
            for h, g in df.groupby("Hafta"):
                for gun in tercih:
                    s = g[g["Gun"] == gun]
                    if not s.empty:
                        haftalik.append(s)
                        break
            if not haftalik:
                return pd.DataFrame()
            df = pd.concat(haftalik)
        p = df.pivot_table(
            index="Tarih", columns="Endeks", values=kolon, aggfunc="first"
        ).ffill()
        p = p[[e for e in p.columns if e in takip]]
        return p.sort_index(ascending=False).sort_index(axis=1)

    kapanis = pivotla(df, "Kapanış", takip, secili)
    yuksek = pivotla(df, "Yüksek", takip, secili)
    dusuk = pivotla(df, "Düşük", takip, secili)
    haftalik = pivotla(df, "Kapanış", takip, secili, haftalik=True)

    with pd.ExcelWriter("main_indis_fiyat.xlsx", engine="openpyxl") as w:
        for name, tab in {"Kapanis": kapanis, "Yuksek": yuksek, "Dusuk": dusuk}.items():
            tab.index = tab.index.strftime("%d.%m.%Y")
            tab.to_excel(w, sheet_name=name)
        if not haftalik.empty:
            haftalik.index = haftalik.index.strftime("%d.%m.%Y")
            haftalik.to_excel(w, sheet_name="Haftalik_Kapanis")
    print("✅ main_indis_fiyat.xlsx oluşturuldu")
except Exception as e:
    print("❌ main_indis_fiyat.xlsx oluşturulamadı:", e)
