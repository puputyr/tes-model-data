# include/processors/process_kategori_1.py

import requests
import pandas as pd
import psycopg2.extras
from psycopg2 import sql, extras
import re
import traceback
import string

# Impor komponen bersama dari folder 'include'
from include.utilitas import get_api_urls_from_sheet, format_schema_name, format_table_name, log_error, get_schema_and_table_names
from include.db_config_data import get_db_connection

# ---------------------------------------------------------------------
# FUNGSI-FUNGSI SPESIFIK HANYA UNTUK KATEGORI 1
# ---------------------------------------------------------------------

def get_dataframe_kategori_1(api_list, mode_penyimpanan="append"):
    """
    Fungsi ini berisi logika TRANSFORMASI data yang benar-benar unik
    untuk Kategori 1. Cara memecah 'kode_data', mapping kolom, dll.
    semuanya ada di sini.
    """

    data_untuk_disimpan = []
    daftar_url = api_list

    for url_api in daftar_url:
        try:
            respons_meta = requests.get(url_api, timeout=30).json()
            nama_schema, nama_tabel = get_schema_and_table_names(respons_meta)

            df_bps = proses_data_bps_ke_dataframe(url_api)

            if df_bps is not None and not df_bps.empty:
                data_untuk_disimpan.append({
                    "df": df_bps,
                    "schema": nama_schema,
                    "tabel": nama_tabel
                })
                print(f"Table Shape: {df_bps.shape}")
                print("Columns:", list(df_bps.columns))
                print(f"Schema Name: {nama_schema}, Table Name: {nama_tabel}")
            elif df_bps is not None:
                print("Proses berhasil, namun tidak ada data valid yang dihasilkan dari API ini.")
            else:
                print("Gagal memproses data dari API.")

        except Exception as e:
            log_error(f"Error pemrosesan URL {url_api}: {str(e)}")
            print(f"Gagal memproses URL {url_api}: {str(e)}")


    total_baris_global = 0
    for item in data_untuk_disimpan:
        df = item['df']
        total_baris = len(df)
        total_baris_global += total_baris

    berhasil_simpan = 0
    gagal_simpan = 0

    for item in data_untuk_disimpan:
        if save_data_kategori_1(item['df'], item['tabel'], item['schema'], mode=mode_penyimpanan):
            berhasil_simpan += 1
        else:
            gagal_simpan += 1

    
def konversi_ke_integer(tahun_str):
    """Mengonversi string tahun ke integer dengan penanganan error"""
    try:
        tahun_bersih = re.sub(r'[^\d]', '', str(tahun_str))
        return int(tahun_bersih) if tahun_bersih else None
    except (ValueError, TypeError):
        return None

def ekstrak_nama_variabel(kode_data, mapping_nama_variabel, nama_variabel_default, tipe_format=None):
    """Mengekstrak kode nama_variabel dari berbagai format"""
    try:
        if len(kode_data) == 16 and tipe_format == 'pdrb_provinsi':
            return mapping_nama_variabel.get(kode_data[6:10], nama_variabel_default)
        elif len(kode_data) == 12 and tipe_format == 'kabupaten':
            return mapping_nama_variabel.get(kode_data[5:8], nama_variabel_default)
        elif len(kode_data) == 9 and tipe_format == 'bulan':
            return mapping_nama_variabel.get(kode_data[4:5], nama_variabel_default)
        elif len(kode_data) == 16:
            return mapping_nama_variabel.get(kode_data[8:12], nama_variabel_default)
        elif len(kode_data) == 14:
            return mapping_nama_variabel.get(kode_data[7:10], nama_variabel_default)
        return nama_variabel_default
    except Exception as e:
        log_error(f"Error ekstraksi nama variabel: {str(e)}")
        return nama_variabel_default

def ekstrak_kode_tahun(kode_data, mapping_tahun, tipe_format=None):
    """Mengekstrak kode tahun dari berbagai format"""
    try:
        if len(kode_data) == 16 and tipe_format == 'pdrb_provinsi':
            kode = kode_data[10:13]
            return kode if kode in mapping_tahun else None
        elif len(kode_data) == 12 and tipe_format == 'kabupaten':
            kode = kode_data[8:11]
            return kode if kode in mapping_tahun else None
        elif len(kode_data) == 9 and tipe_format == 'bulan':
            kode = kode_data[5:8]
            return kode if kode in mapping_tahun else None
        elif len(kode_data) == 16:
            kode = kode_data[12:15]
            return kode if kode in mapping_tahun else None
        elif len(kode_data) == 14:
            kode = kode_data[10:13]
            return kode if kode in mapping_tahun else None
        elif len(kode_data) == 13:
            kode = kode_data[9:12]
            return kode if kode in mapping_tahun else None
        elif len(kode_data) == 12:
            kode = kode_data[8:11]
            return kode if kode in mapping_tahun else None
        elif len(kode_data) == 11:
            kode = kode_data[7:10]
            return kode if kode in mapping_tahun else None

        for kode_tahun in mapping_tahun.keys():
            if kode_tahun in kode_data:
                return kode_tahun

        return None
    except Exception as e:
        log_error(f"Error ekstraksi tahun: {str(e)}")
        return None

def proses_data_bps_ke_dataframe(url_api):
    """Memproses data BPS API menjadi DataFrame terformat"""
    try:
        print(f"Fetching data from BPS API Kategori 1: {url_api}")
        respons = requests.get(url_api, timeout=30)
        data = respons.json()

        if respons.status_code != 200 or "datacontent" not in data:
            raise Exception(f"Respons API tidak valid (HTTP {respons.status_code})")

        id_subjek = data['subject'][0]['val'] if data.get('subject') else None
        is_format_kabupaten = any(len(str(item["val"])) <= 3 for item in data.get("vervar", []))
        is_format_inflasi = any(item.get("label", "").lower() == "inflasi tahun ke tahun (y o y)" for item in data.get("var", []))
        is_format_pdrb = any("pdrb" in item.get("label", "").lower() for item in data.get("var", []))

        daftar_wilayah = data.get("vervar", [])
        mapping_wilayah = {str(item["val"]): re.sub(r"</?b>", "", item["label"], flags=re.IGNORECASE).strip() for item in daftar_wilayah}

        daftar_tahun = sorted(data.get("tahun", []), key=lambda x: x["label"])
        mapping_tahun = {str(item["val"]): item["label"] for item in daftar_tahun}

        daftar_nama_variabel = data.get("turvar", [])
        mapping_nama_variabel = {str(item["val"]): item["label"] for item in daftar_nama_variabel}
        nama_variabel_default = daftar_nama_variabel[0]["label"] if daftar_nama_variabel else "N/A"

        if not mapping_tahun:
            raise Exception("Tidak ada data tahun dalam respons API")

        baris_data = []
        for kode_data, nilai in data.get("datacontent", {}).items():
            try:
                id_wilayah, kode_nama_variabel_str, kode_tahun_str = None, None, None

                if is_format_pdrb and len(kode_data) == 16:
                    id_wilayah = kode_data[:2]
                    kode_nama_variabel_str = kode_data[6:10]
                    kode_tahun_str = kode_data[10:13]
                elif is_format_inflasi and len(kode_data) == 9:
                    id_wilayah = kode_data[:2].zfill(2)
                    kode_nama_variabel_str = kode_data[4:5]
                    kode_tahun_str = kode_data[5:8]
                elif is_format_kabupaten and len(kode_data) == 12:
                    id_wilayah = kode_data[:2]
                    kode_nama_variabel_str = kode_data[5:8]
                    kode_tahun_str = kode_data[8:11]
                elif len(kode_data) in [11, 12, 13, 14, 16]:
                    id_wilayah = kode_data[:4].zfill(4) if not is_format_kabupaten else kode_data[:2]
                    kode_tahun_str = ekstrak_kode_tahun(kode_data, mapping_tahun)
                    kode_nama_variabel_str = ekstrak_nama_variabel(kode_data, mapping_nama_variabel, nama_variabel_default)

                if id_wilayah in mapping_wilayah and kode_tahun_str in mapping_tahun:
                    tahun_int = konversi_ke_integer(mapping_tahun[kode_tahun_str])
                    if tahun_int:
                        baris_data.append({
                            "id_kategori": id_subjek,
                            "daerah": mapping_wilayah[id_wilayah],
                            "nama_variabel": mapping_nama_variabel.get(kode_nama_variabel_str, nama_variabel_default) if isinstance(kode_nama_variabel_str, str) else kode_nama_variabel_str,
                            "tahun": tahun_int,
                            "jumlah": nilai
                        })
            except Exception as e:
                log_error(f"Error memproses baris data {kode_data}: {str(e)}")
                continue

        if not baris_data:
            raise Exception("Tidak ada data valid yang bisa diproses")

        df = pd.DataFrame(baris_data)
        df = df.sort_values(by=["tahun", "daerah"])
        df.insert(0, "id", range(1, len(df) + 1))
        df = df[["id", "id_kategori", "daerah", "nama_variabel", "tahun", "jumlah"]]
        df['tahun'] = pd.to_numeric(df['tahun'], errors='coerce').astype('Int64')

        return df

    except Exception as e:
        pesan_error = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        log_error(f"[{url_api}] {pesan_error}")
        print(f"Gagal memproses data API: {str(e)}")
        return None


def save_data_kategori_1(df, nama_tabel, nama_schema="api_bps_data", mode="append"):
    """
    Jika cara menyimpan datanya juga unik, buat fungsi save sendiri.
    Jika sama, Anda bisa impor dan pakai fungsi save_to_database dari utilitas.
    """
    koneksi = None
    try:
        if df.empty:
            return False

        koneksi = get_db_connection()
        kursor = koneksi.cursor()

        kursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(nama_schema)))

        kursor.execute(sql.SQL("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            );
        """), (nama_schema, nama_tabel.lower()))
        tabel_ada = kursor.fetchone()[0]

        if not tabel_ada:
            query_buat_tabel = sql.SQL("""
                CREATE TABLE {}.{} (
                    id SERIAL PRIMARY KEY,
                    id_kategori INTEGER,
                    daerah TEXT,
                    nama_variabel TEXT,
                    tahun INTEGER,
                    jumlah FLOAT
                );
            """).format(sql.Identifier(nama_schema), sql.Identifier(nama_tabel))
            kursor.execute(query_buat_tabel)

        if mode == 'replace' and tabel_ada:
            query_truncate = sql.SQL("TRUNCATE TABLE {}.{} RESTART IDENTITY").format(
                sql.Identifier(nama_schema),
                sql.Identifier(nama_tabel)
            )
            kursor.execute(query_truncate)

        nilai_nilai = [tuple(row) for row in df[['id_kategori', 'daerah', 'nama_variabel', 'tahun', 'jumlah']].itertuples(index=False)]

        if mode == 'replace':
            query_sisip = sql.SQL("""
                INSERT INTO {}.{} (id_kategori, daerah, nama_variabel, tahun, jumlah)
                VALUES %s
            """).format(sql.Identifier(nama_schema), sql.Identifier(nama_tabel))
        else:
            query_sisip = sql.SQL("""
                INSERT INTO {}.{} (id_kategori, daerah, nama_variabel, tahun, jumlah)
                VALUES %s
                ON CONFLICT DO NOTHING
            """).format(sql.Identifier(nama_schema), sql.Identifier(nama_tabel))

        psycopg2.extras.execute_values(kursor, query_sisip, nilai_nilai)
        koneksi.commit()

        return True

    except Exception as e:
        if koneksi:
            koneksi.rollback()
        log_error(f"Error penyimpanan database untuk {nama_schema}.{nama_tabel}: {str(e)}")
        print(f"Gagal menyimpan ke {nama_schema}.{nama_tabel}: {str(e)}")
        return False
    finally:
        if koneksi:
            kursor.close()
            koneksi.close()

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

    # for api_url in api_list:
    try:
        # Langkah 1: Extract & Transform (menggunakan fungsi unik di atas)
        df_processed = get_dataframe_kategori_1(api_list, mode_penyimpanan="replace")

        # Langkah 2: Load (menggunakan fungsi unik atau fungsi bersama)
        # if df_processed is not None and not df_processed.empty:
        #     save_data_kategori_1(df_processed)
            
        print(f"✅ Sukses memproses API")

    except Exception as e:
        print(f"❌ Gagal memproses API : {e}")
        log_error(f"Error di Kategori 1 : {e}")
        # raise e # Hentikan proses jika satu API gagal

    print(f"🏁 Selesai ETL untuk Kategori 1.")