import requests
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
from tarih_ayar import secili_tarihleri_bul

BASE_URL = "https://img.euromsg.net/54165B4951BD4D81B4668B9B9A6D7E54/files"

def parse_excel(url, tarih, hedef_kodlar):
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    df_raw = pd.read_excel(BytesIO(r.content), header=None)

    veri_listesi = []
    for j in range(len(df_raw)):
        kod = str(df_raw.iat[j, 1]).strip()
        if not kod or kod.lower() == "nan":
            continue
        if kod not in hedef_kodlar:
            continue

        msci_tr = "MSCI" if pd.notna(df_raw.iat[j, 6]) else ""
        ozkaynak = df_raw.iat[j, 7]
        sermaye  = df_raw.iat[j, 8]
        aktifler = df_raw.iat[j, 9]
        net_borc = df_raw.iat[j, 14]
        net_kar  = df_raw.iat[j, 17]

        satir = {
            "Tarih": tarih,
            "Hisse Kodu": kod,
            "MSCI Turkey ETF": msci_tr,
            "Özkaynaklar": ozkaynak,
            "Ödenmiş Sermaye": sermaye,
            "Toplam Aktifler": aktifler,
            "Net Borç": net_borc,
            "Yıllık Net Kar": net_kar
        }
        veri_listesi.append(satir)

    return pd.DataFrame(veri_listesi)

def main():
    # dates.csv içinden tarihleri ve hisse kodlarını oku
    df_dates = pd.read_csv("data/dates.csv", header=None)
    excel_tarihleri = [str(d).strip() for d in df_dates.iloc[:,0] if str(d).strip()]
    codes = [str(c).strip() for c in df_dates.iloc[:,2] if str(c).strip()]

    # sadece 5 günlük seri seç
    secilen_tarihler = secili_tarihleri_bul(excel_tarihleri, hedef_gun_sayisi=5)

    all_dfs = []
    for d_orig in secilen_tarihler:
        d_fmt = datetime.strptime(d_orig, "%d.%m.%Y").strftime("%Y_%m_%d")
        url = f"{BASE_URL}/ZRY Göstergeler-{d_fmt}.xlsx"
        try:
            df_day = parse_excel(url, d_orig, set(codes))
            if not df_day.empty:
                all_dfs.append(df_day)
        except Exception as e:
            print(f"Excel okunamadı: {url}, hata: {e}")
            continue

    if not all_dfs:
        print("Hiç veri bulunamadı.")
        return

    df_final = pd.concat(all_dfs, ignore_index=True)

    # Numerik dönüşüm + milyon çarpanı
    ozkaynak_num = pd.to_numeric(df_final["Özkaynaklar"], errors="coerce") * 1_000_000
    sermaye_num  = pd.to_numeric(df_final["Ödenmiş Sermaye"], errors="coerce") * 1_000_000
    aktifler_num = pd.to_numeric(df_final["Toplam Aktifler"], errors="coerce") * 1_000_000
    netborc_num  = pd.to_numeric(df_final["Net Borç"], errors="coerce") * 1_000_000
    netkar_num   = pd.to_numeric(df_final["Yıllık Net Kar"], errors="coerce") * 1_000_000

    # Yeni hesaplama sütunları
    df_final["PD Çarpan"] = np.where(ozkaynak_num != 0, sermaye_num / ozkaynak_num, np.nan)
    df_final["F/K Çarpan"] = np.where(netkar_num != 0, sermaye_num / netkar_num, np.nan)
    df_final["Öz Karlılık (%)"] = np.where(ozkaynak_num != 0, (netkar_num / ozkaynak_num) * 100, np.nan)
    df_final["Aktif Karlılık (%)"] = np.where(aktifler_num != 0, (netkar_num / aktifler_num) * 100, np.nan)

    # Ham kolonlar → tam sayı string
    for col, series in zip(
        ["Özkaynaklar","Ödenmiş Sermaye","Toplam Aktifler","Net Borç","Yıllık Net Kar"],
        [ozkaynak_num, sermaye_num, aktifler_num, netborc_num, netkar_num]
    ):
        df_final[col] = series.apply(lambda x: "" if pd.isna(x) else str(int(x)))

    # Hesaplama kolonları → 5 basamaklı ondalık string
    for col in ["PD Çarpan","F/K Çarpan","Öz Karlılık (%)","Aktif Karlılık (%)"]:
        df_final[col] = df_final[col].apply(lambda x: "" if pd.isna(x) else str(round(x,5)))

    # Tarih kolonunu datetime'a çevir
    df_final["Tarih"] = pd.to_datetime(df_final["Tarih"], format="%d.%m.%Y", errors="coerce")

    # Sıralama
    df_final = df_final.sort_values(
        by=["Tarih","Hisse Kodu"],
        ascending=[False, True]
    ).reset_index(drop=True)

    # Tarih tekrar string formatına çevrilir
    df_final["Tarih"] = df_final["Tarih"].dt.strftime("%d.%m.%Y")

    # Artifact yazma
    artifact_path = "vert_pdfk.csv"
    df_final.to_csv(artifact_path, index=False)

    print("Artifact oluşturuldu:", artifact_path)
    print("Bu artifact 3 gün sonunda silinecek.")
    print("Workflow sonunda artifact linki: [artifact://vert_pdfk.csv]")

if __name__ == "__main__":
    main()
