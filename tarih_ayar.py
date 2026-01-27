import pandas as pd
from datetime import datetime, timedelta, time
import pytz

def bul_ilk_gun(csv_tarihleri):
    tz = pytz.timezone("Europe/Istanbul")
    now, today, yesterday, saat = datetime.now(tz), datetime.now(tz).date(), datetime.now(tz).date()-timedelta(days=1), datetime.now(tz).time()
    saat_kritik = time(18,25)
    liste_tarihleri = [t.date() for t in sorted(pd.to_datetime([str(x).strip() for x in csv_tarihleri if str(x).strip()], dayfirst=True, errors='coerce'))]
    if saat > saat_kritik:
        return today if today in liste_tarihleri else (max([d for d in liste_tarihleri if d<today]) if any(d<today for d in liste_tarihleri) else None)
    else:
        return yesterday if yesterday in liste_tarihleri else (max([d for d in liste_tarihleri if d<yesterday]) if any(d<yesterday for d in liste_tarihleri) else None)

def sirali_gunler(csv_tarihleri, ilk_gun, hedef_gun_sayisi):
    tarih_serisi = pd.to_datetime(pd.Series(csv_tarihleri), dayfirst=True, errors='coerce')
    try: baslangic_index = tarih_serisi[tarih_serisi.dt.date==ilk_gun].index[0]
    except IndexError: return []
    return tarih_serisi.iloc[baslangic_index:baslangic_index+hedef_gun_sayisi].dt.date.tolist()

def secili_tarihleri_bul(csv_tarihleri, hedef_gun_sayisi=500):
    ilk_gun = bul_ilk_gun(csv_tarihleri)
    return [] if not ilk_gun else [g.strftime("%d.%m.%Y") for g in sirali_gunler(csv_tarihleri, ilk_gun, hedef_gun_sayisi)]

# 📌 CSV dosyasından okuma
csv_tarihleri = pd.read_csv("dates.csv", encoding="utf-8")["Tarih"].tolist()
secili_tarihler = secili_tarihleri_bul(csv_tarihleri, hedef_gun_sayisi=500)
