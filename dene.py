#!/usr/bin/env python3
import pandas as pd

def main():
    # pivot_gaijin.csv dosyasını oku
    df = pd.read_csv("pivot_gaijin.csv")

    # Kontrol çıktısı
    print("Toplam sütun sayısı:", len(df.columns))
    print("Toplam satır sayısı:", len(df))
    print("\nİlk 5 satır:")
    print(df.head())

    # Excel'e yaz
    output_file = "pivot_gaijin.xlsx"
    df.to_excel(output_file, index=False, engine="openpyxl")
    print(f"\n✅ Excel dosyası '{output_file}' olarak kaydedildi.")

if __name__ == "__main__":
    main()
