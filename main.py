import requests, pandas as pd, json, os
from bs4 import BeautifulSoup
from collections import defaultdict
from tarih_ayar import secili_tarihleri_bul

# 🔧 Sayı temizleme (ondalık için Türkçe format)
def temizle_sayi(s):
    s = str(s).strip()
    if not s:
        return None
    # Binlik ayırıcı noktaları kaldır
    s = s.replace(".", "")
    # Ondalık ayırıcı virgülü noktaya çevir
    s = s.replace(",", ".")
    try:
        return float(s)
    except:
        return None

# 🔧 Hacim temizleme (her zaman tam sayı)
def temizle_hacim(s):
    s = str(s).strip()
    if not s:
        return None
    # Binlik ayırıcı noktaları kaldır
    s = s.replace(".", "")
    # Virgül varsa kaldır (hacimde ondalık yok)
    s = s.replace(",", "")
    try:
        return int(s)
    except:
        return None

BASE_URL = json.loads(os.environ["MAIN"])["DATA_SOURCE_URL"]

def fiyat_hacim_tek_gun(g, a, y):
    try:
        r = requests.get(
            f"{BASE_URL}?gun={g}&ay={a}&yil={y}&tip=Hisse",
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=10
        )
        r.raise_for_status()
    except:
        return []
    soup = BeautifulSoup(r.text, 'html.parser')
    t = soup.find('table')
    if not t:
        return []
    ts = f"{g:02d}.{a:02d}.{y}"
    return [
        {
            "Tarih": ts,
            "Hisse": c[0],
            "Kapanış": temizle_sayi(c[1]),
            "Yüksek": temizle_sayi(c[4]),
            "Düşük": temizle_sayi(c[5]),
            "Hacim(Lot)": temizle_hacim(c[7])
        }
        for c in ([td.get_text(strip=True) for td in tr.find_all('td')]
                  for tr in t.find_all('tr')[1:])
        if len(c) >= 9 and temizle_sayi(c[1])
    ]

# 📅 Tarih listesi ve takip hisseleri (data klasöründen)
df_csv = pd.read_csv("data/dates.csv", encoding="utf-8")
csv_tarihleri = df_csv["Tarih"].dropna().tolist()

# 🔹 Artık sadece 5 gün seçiyoruz
secili = list(reversed(secili_tarihleri_bul(csv_tarihleri, 5)))
takip_hisseler = df_csv.iloc[:, 1].dropna().unique().tolist()

# 📊 Dikey artifact verisi
tum_veriler = [v for t in secili for v in fiyat_hacim_tek_gun(*map(int, t.split(".")))]
vg = defaultdict(list)
[vg[v["Tarih"]].append(v) for v in tum_veriler]
ilk = {}
[ilk.setdefault(v["Hisse"], pd.to_datetime(v["Tarih"], dayfirst=True)) for v in tum_veriler]
tum_hisseler = sorted({v["Hisse"] for v in tum_veriler})
onceki = {}
final = []

for t in secili:
    td = pd.to_datetime(t, dayfirst=True)
    gv = {v["Hisse"]: v for v in vg.get(t, [])}
    sat = []
    for h in tum_hisseler:
        if h in gv:
            sat.append(gv[h])
        elif h in onceki and td >= ilk[h]:
            k = onceki[h].copy()
            k["Tarih"] = t
            sat.append(k)
    onceki.update(gv)
    final.extend(sat)

if final:
    pd.DataFrame(final).to_csv("artifact_veriler.csv", index=False, encoding="utf-8")

# 📊 Yatay tablolar
df = pd.DataFrame(final)

def pivotla(df, kolon, takip_hisseler, tarih_listesi):
    df["Tarih"] = pd.to_datetime(df["Tarih"], dayfirst=True, errors="coerce")
    df = df[df["Tarih"].isin(pd.to_datetime(tarih_listesi, dayfirst=True))]
    df = df.dropna(subset=["Tarih", "Hisse", kolon])
    # Güvenlik için tekrar temizleme
    df[kolon] = df[kolon].apply(temizle_sayi)
    p = df.pivot_table(index="Tarih", columns="Hisse", values=kolon, aggfunc="first").ffill()
    p = p[[h for h in p.columns if h in takip_hisseler]]
    return p.sort_index(ascending=False).sort_index(axis=1)

tablolar = {col: pivotla(df, col, takip_hisseler, secili) for col in ["Kapanış", "Yüksek", "Düşük", "Hacim(Lot)"]}

for col, p in tablolar.items():
    p.index = p.index.strftime("%d.%m.%Y")
    p.to_csv(f"artifact_{col}.csv", encoding="utf-8")
