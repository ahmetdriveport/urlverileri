import pandas as pd
from datetime import datetime
import pytz

DEFAULT_GUN_SAYISI = 150

def secili_tarihleri_bul(csv_tarihleri, hedef=DEFAULT_GUN_SAYISI):
    today = datetime.now(pytz.timezone("Europe/Istanbul")).date()
    tarihler = sorted(pd.to_datetime([str(x).strip() for x in csv_tarihleri if str(x).strip()],
                                     dayfirst=True, errors="coerce"))
    liste = [t.date() for t in tarihler if t.date() <= today]
    if not liste: return []
    ilk = today if today in liste else max(liste)
    idx = liste.index(ilk)
    return [d.strftime("%d.%m.%Y") for d in liste[idx:idx+hedef]]
