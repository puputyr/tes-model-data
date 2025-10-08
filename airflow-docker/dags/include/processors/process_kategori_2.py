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
# FUNGSI-FUNGSI SPESIFIK HANYA UNTUK Kategori 2
# ---------------------------------------------------------------------

def get_dataframe_kategori_2(api_list, mode_penyimpanan="append"):
    """
    Fungsi ini berisi logika TRANSFORMASI data yang benar-benar unik
    untuk Kategori 2. Cara memecah 'kode_data', mapping kolom, dll.
    semuanya ada di sini.
    """
    data_to_save = []
    url_list = api_list

    for api_url in url_list:
        
        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            data = response.json()

            schema_name, table_name = get_schema_and_table_names(data)

            df_bps = get_bps_formatted_dataframe(api_url)

            if df_bps is not None and not df_bps.empty:
                data_to_save.append({
                    "df": df_bps,
                    "schema": schema_name,
                    "table": table_name
                })
                print(f"Table Shape: {df_bps.shape}")
                print("Columns:", list(df_bps.columns))
                print(f"Schema Name: {schema_name}, Table Name: {table_name}")
            elif df_bps is not None:
                print("Proses berhasil, namun tidak ada data valid yang dihasilkan dari API ini.")
            else:
                print("Gagal memproses data dari API.")
        except Exception as e:
            log_error(f"Error fatal pada URL {api_url}: {str(e)}")
            print(f"Terjadi error fatal saat memproses URL {api_url}. Lihat log.")

    if not data_to_save:
        return

    total_rows_global = 0
    for item in data_to_save:
        df = item['df']
        total_rows = len(df)
        total_rows_global += total_rows
        
    success_count = 0
    fail_count = 0
    for item in data_to_save:
        if save_data_kategori_2(item['df'], item['table'], item['schema'], mode=mode_penyimpanan):
            success_count += 1
        else:
            fail_count += 1


def save_data_kategori_2(df, table_name, schema_name="api_bps_data", mode="append"):
    """
    Jika cara menyimpan datanya juga unik, buat fungsi save sendiri.
    Jika sama, Anda bisa impor dan pakai fungsi save_to_database dari utilitas.
    """
    conn = None
    try:
        if df.empty:
            return False

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))

        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                id SERIAL PRIMARY KEY,
                id_kategori INTEGER,
                daerah TEXT,
                nama_variabel TEXT,
                tahun TEXT,
                jumlah FLOAT,
                UNIQUE(id_kategori, daerah, nama_variabel, tahun)
            );
        """).format(sql.Identifier(schema_name), sql.Identifier(table_name))
        cursor.execute(create_table_query)

        if mode == 'replace':
            truncate_query = sql.SQL("TRUNCATE TABLE {}.{} RESTART IDENTITY CASCADE").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name)
            )
            cursor.execute(truncate_query)

        values = [tuple(row) for row in df[['id_kategori', 'daerah', 'nama_variabel', 'tahun', 'jumlah']].itertuples(index=False)]

        if mode == 'replace':
            insert_query = sql.SQL("""
                INSERT INTO {}.{} (id_kategori, daerah, nama_variabel, tahun, jumlah)
                VALUES %s
            """).format(sql.Identifier(schema_name), sql.Identifier(table_name))
        else: # mode 'append'
            insert_query = sql.SQL("""
                INSERT INTO {}.{} (id_kategori, daerah, nama_variabel, tahun, jumlah)
                VALUES %s
                ON CONFLICT (id_kategori, daerah, nama_variabel, tahun) DO NOTHING;
            """).format(sql.Identifier(schema_name), sql.Identifier(table_name))

        psycopg2.extras.execute_values(cursor, insert_query, values)
        conn.commit()

        return True
    except Exception as e:
        if conn:
            conn.rollback()
        log_error(f"Database save error for {schema_name}.{table_name}: {str(e)}")
        print(f"Gagal menyimpan ke {schema_name}.{table_name}: {str(e)}")
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()
    
    
def extract_nama_variabel(kode_data, nama_variabel_map, default_nama_variabel, format_type=None):
    """Mengekstrak kode nama_variabel dari berbagai format"""
    try:
        if len(kode_data) == 10 and format_type == 'proporsi_sekolah':
            return nama_variabel_map.get(kode_data[5], default_nama_variabel)
        if len(kode_data) == 13 and format_type == 'pendidikan':
            return nama_variabel_map.get(kode_data[5:9], default_nama_variabel)
        if len(kode_data) == 16 and format_type == 'pdrb_provinsi':
            return nama_variabel_map.get(kode_data[6:10], default_nama_variabel)
        elif len(kode_data) == 12 and format_type == 'kabupaten':
            return nama_variabel_map.get(kode_data[5:8], default_nama_variabel)
        elif len(kode_data) == 9 and format_type == 'bulan':
            return nama_variabel_map.get(kode_data[4:5], default_nama_variabel)
        elif len(kode_data) == 16:
            return nama_variabel_map.get(kode_data[8:12], default_nama_variabel)
        elif len(kode_data) == 14:
            return nama_variabel_map.get(kode_data[7:10], default_nama_variabel)
        return default_nama_variabel
    except Exception as e:
        log_error(f"Nama variabel extraction error: {str(e)}")
        return default_nama_variabel

def extract_year_code(kode_data, tahun_map, format_type=None):
    """Mengekstrak kode tahun dari berbagai format"""
    try:
        if len(kode_data) == 10 and format_type == 'proporsi_sekolah':
            code = kode_data[6:9]
            return code if code in tahun_map else None
        if len(kode_data) == 13 and format_type == 'pendidikan':
            code = kode_data[9:12]
            return code if code in tahun_map else None
        if len(kode_data) == 16 and format_type == 'pdrb_provinsi':
            code = kode_data[10:13]
            return code if code in tahun_map else None
        elif len(kode_data) == 12 and format_type == 'kabupaten':
            code = kode_data[8:11]
            return code if code in tahun_map else None
        elif len(kode_data) == 9 and format_type == 'bulan':
            code = kode_data[5:8]
            return code if code in tahun_map else None
        elif len(kode_data) == 16:
            code = kode_data[12:15]
            return code if code in tahun_map else None
        elif len(kode_data) == 14:
            code = kode_data[10:13]
            return code if code in tahun_map else None
        elif len(kode_data) == 13:
            code = kode_data[9:12]
            return code if code in tahun_map else None
        elif len(kode_data) == 12:
            code = kode_data[8:11]
            return code if code in tahun_map else None
        elif len(kode_data) == 11:
            code = kode_data[7:10]
            return code if code in tahun_map else None
        for year_code in tahun_map.keys():
            if year_code in kode_data:
                return year_code
        return None
    except Exception as e:
        log_error(f"Year extraction error: {str(e)}")
        return None

def get_bps_formatted_dataframe(api_url):
    """Memproses data BPS API menjadi DataFrame terformat"""
    try:
        print(f"Fetching data from BPS API Kategori 2: {api_url}")
        response = requests.get(api_url)
        data = response.json()
        if response.status_code != 200 or "datacontent" not in data:
            raise Exception("Respons API tidak valid atau tidak ada konten data")
  
        subject_id = data['subject'][0]['val'] if data.get('subject') else None
        subject_label = data['subject'][0]['label'] if data.get('subject') else None
        var_label = data['var'][0]['label'] if data.get('var') else None
        is_kabupaten_format = any(len(str(item["val"])) <= 3 for item in data.get("vervar", []))
        is_inflasi_format = "Inflasi" in var_label if var_label else False
        is_pdrb_format = "PDRB" in var_label if var_label else False
        is_pendidikan_format = subject_label == "Pendidikan"
        is_proporsi_sekolah = "Proporsi Sekolah" in var_label if var_label else False
        wilayah_items = data.get("vervar", [])
        wilayah = {str(item["val"]): re.sub(r"</?b>", "", item["label"], flags=re.IGNORECASE).strip() for item in wilayah_items}
        tahun_map = {str(item["val"]): item["label"] for item in sorted(data.get("tahun", []), key=lambda x: x["label"])}
        nama_variabel_items = data.get("turvar", [])
        nama_variabel_map = {str(item["val"]): item["label"] for item in nama_variabel_items}
        default_nama_variabel = nama_variabel_items[0]["label"] if nama_variabel_items else "N/A"
        if not tahun_map:
            raise Exception("Tidak ada data tahun dalam respons API")
        data_rows = []
        for kode_data, nilai in data.get("datacontent", {}).items():
            try:
                if (is_pendidikan_format and is_proporsi_sekolah and len(kode_data) == 10):
                    wilayah_id = kode_data[0]
                    kode_turvar = kode_data[5]
                    kode_tahun = kode_data[6:9]
                    if (wilayah_id in wilayah and kode_tahun in tahun_map):
                        data_rows.append({"id_kategori": subject_id, "daerah": wilayah[wilayah_id], "nama_variabel": nama_variabel_map.get(kode_turvar, default_nama_variabel), "tahun": tahun_map[kode_tahun], "jumlah": nilai})
                elif is_pendidikan_format and len(kode_data) == 13:
                    wilayah_id = kode_data[0]
                    jenjang_pendidikan = kode_data[5:9]
                    kode_tahun = kode_data[9:12]
                    if (wilayah_id in wilayah and kode_tahun in tahun_map and jenjang_pendidikan in nama_variabel_map):
                        data_rows.append({"id_kategori": subject_id, "daerah": wilayah[wilayah_id], "nama_variabel": nama_variabel_map[jenjang_pendidikan], "tahun": tahun_map[kode_tahun], "jumlah": nilai})
                elif is_pdrb_format and len(kode_data) == 16:
                    wilayah_id = kode_data[:2]
                    nama_variabel_code = kode_data[6:10]
                    kode_tahun = kode_data[10:13]
                    if (wilayah_id in wilayah and kode_tahun in tahun_map):
                        data_rows.append({"id_kategori": subject_id, "daerah": wilayah[wilayah_id], "nama_variabel": nama_variabel_map.get(nama_variabel_code, default_nama_variabel), "tahun": tahun_map[kode_tahun], "jumlah": nilai})
                elif is_inflasi_format and len(kode_data) == 9:
                    wilayah_id = kode_data[:2].zfill(2)
                    kode_tahun = kode_data[5:8]
                    nama_variabel_code = kode_data[4:5]
                    if wilayah_id in wilayah and kode_tahun in tahun_map:
                        data_rows.append({"id_kategori": subject_id, "daerah": wilayah[wilayah_id], "nama_variabel": nama_variabel_map.get(nama_variabel_code, default_nama_variabel), "tahun": tahun_map[kode_tahun], "jumlah": nilai})
                elif is_kabupaten_format and len(kode_data) == 12:
                    wilayah_id = kode_data[:2]
                    kode_tahun = kode_data[8:11]
                    nama_variabel_code = kode_data[5:8]
                    if wilayah_id in wilayah and kode_tahun in tahun_map:
                        data_rows.append({"id_kategori": subject_id, "daerah": wilayah[wilayah_id], "nama_variabel": nama_variabel_map.get(nama_variabel_code, default_nama_variabel), "tahun": tahun_map[kode_tahun], "jumlah": nilai})
                elif len(kode_data) in [11, 12, 13, 14, 16]:
                    wilayah_id = kode_data[:4].zfill(4) if not is_kabupaten_format else kode_data[:2]
                    kode_tahun = extract_year_code(kode_data, tahun_map)
                    nama_variabel_label = extract_nama_variabel(kode_data, nama_variabel_map, default_nama_variabel)
                    if wilayah_id in wilayah and kode_tahun:
                        data_rows.append({"id_kategori": subject_id, "daerah": wilayah[wilayah_id], "nama_variabel": nama_variabel_label, "tahun": tahun_map[kode_tahun], "jumlah": nilai})
            except Exception as e:
                log_error(f"Error memproses baris data {kode_data}: {str(e)}")
                continue
        if not data_rows:
            raise Exception("Tidak ada data valid yang bisa diproses")
        df = pd.DataFrame(data_rows)
        df = df.sort_values(by=["tahun", "daerah"])
        df.insert(0, "id", range(1, len(df) + 1))
        df = df[["id", "id_kategori", "daerah", "nama_variabel", "tahun", "jumlah"]]
        return df
    except Exception as e:
        error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        log_error(f"[{api_url}] {error_message}")
        print(f"Gagal memproses data API: {str(e)}")
        return None

# ---------------------------------------------------------------------
# FUNGSI UTAMA YANG AKAN DIPANGGIL OLEH AIRFLOW
# ---------------------------------------------------------------------

def run_etl(sheet_column_name: str):
    """
    Titik masuk (entrypoint) untuk proses ETL Kategori 2.
    Fungsi ini mengorkestrasi seluruh alur kerja untuk kategori ini.
    """
    print(f"🚀 Memulai ETL Kategori 2 dari kolom: {sheet_column_name}")
    api_list = get_api_urls_from_sheet(sheet_column_name)

     # for api_url in api_list:
    try:
        # Langkah 1: Extract & Transform (menggunakan fungsi unik di atas)
        df_processed = get_dataframe_kategori_2(api_list, mode_penyimpanan="replace")

        # Langkah 2: Load (menggunakan fungsi unik atau fungsi bersama)
        # if df_processed is not None and not df_processed.empty:
        #     save_data_kategori_1(df_processed)
            
        print(f"✅ Sukses memproses API")

    except Exception as e:
        print(f"❌ Gagal memproses API : {e}")
        log_error(f"Error di Kategori 1 : {e}")
        # raise e # Hentikan proses jika satu API gagal

    print(f"🏁 Selesai ETL untuk Kategori 2.")