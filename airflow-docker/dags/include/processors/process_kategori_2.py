# include/processors/process_kategori_1.py

import requests
import pandas as pd
# ... import lain yang spesifik untuk kategori ini ...

# Impor komponen bersama dari folder 'include'
from include.utilitas import get_api_urls_from_sheet, format_schema_name, format_table_name, log_error
from include.db_config_data import get_db_connection
# Impor fungsi save_to_database jika masih bisa dipakai
# from include.utilitas import save_to_database # Opsional

# ---------------------------------------------------------------------
# FUNGSI-FUNGSI SPESIFIK HANYA UNTUK KATEGORI 1
# ---------------------------------------------------------------------

def get_dataframe_kategori_1(api_url: str):
    """
    Fungsi ini berisi logika TRANSFORMASI data yang benar-benar unik
    untuk Kategori 1. Cara memecah 'kode_data', mapping kolom, dll.
    semuanya ada di sini.
    """
    print("Menjalankan logika transformasi data UNIK untuk Kategori 1...")
    # ...
    # ... Tulis semua kode pemrosesan DataFrame khusus untuk kategori ini ...
    # ...
    # df = ...
    # return df
    pass # Hapus pass dan isi dengan kodemu

def save_data_kategori_1(df):
    """
    Jika cara menyimpan datanya juga unik, buat fungsi save sendiri.
    Jika sama, Anda bisa impor dan pakai fungsi save_to_database dari utilitas.
    """
    print("Menjalankan logika penyimpanan UNIK untuk Kategori 1...")
    pass # Hapus pass dan isi dengan kodemu

# ---------------------------------------------------------------------
# FUNGSI UTAMA YANG AKAN DIPANGGIL OLEH AIRFLOW
# ---------------------------------------------------------------------

def run_etl(sheet_column_name: str):
    """
    Titik masuk (entrypoint) untuk proses ETL Kategori 1.
    Fungsi ini mengorkestrasi seluruh alur kerja untuk kategori ini.
    """
    print(f"🚀 Memulai ETL Kategori 1 dari kolom: {sheet_column_name}")
    api_list = get_api_urls_from_sheet(sheet_column_name)

    for api_url in api_list:
        try:
            # Langkah 1: Extract & Transform (menggunakan fungsi unik di atas)
            df_processed = get_dataframe_kategori_1(api_url)

            # Langkah 2: Load (menggunakan fungsi unik atau fungsi bersama)
            if df_processed is not None and not df_processed.empty:
                 save_data_kategori_1(df_processed)
            
            print(f"✅ Sukses memproses API: {api_url}")

        except Exception as e:
            print(f"❌ Gagal memproses API {api_url}: {e}")
            log_error(f"Error di Kategori 1 ({api_url}): {e}")
            # raise e # Hentikan proses jika satu API gagal

    print(f"🏁 Selesai ETL untuk Kategori 1.")