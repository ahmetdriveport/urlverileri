import pandas as pd
from datetime import datetime, timedelta, time
import pytz

def bul_ilk_gun(csv_tarihleri):
    tz = pytz.timezone("Europe/Istanbul")
    now = datetime.now(tz)
    today = now.date()
    yesterday = today - timedelta(days=1)
    saat = now.time()
    saat_kritik = time(18, 25)

    temiz_tarihler = [str(x).strip() for x in csv_tarihleri if str(x).strip()]
    tarihler_dt = sorted(pd.to_datetime(temiz_tarihler, dayfirst=True, errors='coerce'))
    liste_tarihleri = [t.date() for t in tarihler_dt]

    if saat > saat_kritik:
        if today in liste_tarihleri:
            return today
        else:
            gecmis_tarihler = [d for d in liste_tarihleri if d < today]
            return max(gecmis_tarihler) if gecmis_tarihler else None
    else:
        if yesterday in liste_tarihleri:
            return yesterday
        else:
            gecmis_tarihler = [d for d in liste_tarihleri if d < yesterday]
            return max(gecmis_tarihler) if gecmis_tarihler else None

def sirali_gunler(csv_tarihleri, ilk_gun, hedef_gun_sayisi):
    tarih_serisi = pd.to_datetime(pd.Series(csv_tarihleri), dayfirst=True, errors='coerce')
    try:
        baslangic_index = tarih_serisi[tarih_serisi.dt.date == ilk_gun].index[0]
    except IndexError:
        return []
    return tarih_serisi.iloc[baslangic_index:baslangic_index + hedef_gun_sayisi].dt.date.tolist()

def secili_tarihleri_bul(csv_tarihleri, hedef_gun_sayisi=500):
    ilk_gun = bul_ilk_gun(csv_tarihleri)
    if not ilk_gun:
        return []
    gunler = sirali_gunler(csv_tarihleri, ilk_gun, hedef_gun_sayisi)
    return [g.strftime("%d.%m.%Y") for g in gunler]

# 📌 CSV dosyasından okuma
df = pd.read_csv("hisseler.csv", encoding="utf-8")
csv_tarihleri = df["Tarih"].tolist()   # sadece ilk sütun (Tarih)

secili_tarihler = secili_tarihleri_bul(csv_tarihleri, hedef_gun_sayisi=500)
