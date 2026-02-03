import pandas as pd
from datetime import datetime
import pytz

def secili_tarihleri_bul(csv_tarihleri, hedef=150):
    today = datetime.now(pytz.timezone("Europe/Istanbul")).date()
    tarihler = sorted(pd.to_datetime([str(x).strip() for x in csv_tarihleri if str(x).strip()],
                                     dayfirst=True, errors="coerce"))
    liste = [t.date() for t in tarihler if t.date() <= today]
    if not liste: return []
    ilk = today if today in liste else max(liste)
    idx = liste.index(ilk)
    return [d.strftime("%d.%m.%Y") for d in liste[idx:idx+hedef]]

df = pd.read_csv("data/dates.csv", encoding="utf-8")
secili_tarihler = secili_tarihleri_bul(df["Tarih"].tolist())
