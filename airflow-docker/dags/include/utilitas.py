# include/utilitas.py
import pandas as pd
import re
import datetime

# URL Google Sheet terpusat di sini
URL_SHEET = "https://docs.google.com/spreadsheets/d/1hZNDzfOVC7syH_seSqf2NGLziKvjkGHa8KnfuEbSsz4/export?format=csv&gid=357398258"

def get_api_urls_from_sheet(target_col):
    """Mengambil daftar URL API dari Google Sheet berdasarkan nama kolom."""
    try:
        df = pd.read_csv(URL_SHEET)
        if target_col not in df.columns:
            print(f"❌ Kolom '{target_col}' tidak ditemukan.")
            return []
        urls = df[target_col].dropna().tolist()
        print(f"ℹ️ Ditemukan {len(urls)} API dari kolom '{target_col}'.")
        return urls
    except Exception as e:
        print(f"❌ Gagal ambil dari Google Sheet kolom '{target_col}': {e}")
        return []

def log_error(message):
    with open("error_log.txt", "a", encoding="utf-8") as f:
        f.write(f"[{datetime.datetime.now()}] {message}\n")

def format_schema_name(name_suggestion):
    name = str(name_suggestion).lower()
    name = re.sub(r'[^a-z0-9\s]', ' ', name)
    words = name.split()
    words = [word for word in words if word != 'dan']
    words = words[:3]
    return f"api_bps_{'_'.join(words)}"

def format_table_name(name_suggestion, max_length=63):
    name = str(name_suggestion).lower()
    name = re.sub(r'<[^>]+>', '', name)
    name = re.sub(r'[\s,.-]+', '_', name)
    name = re.sub(r'[^\w_]', '', name)
    name = re.sub(r'__+', '_', name)
    vowels = "aiueo"
    name_no_vowels = "".join([char for char in name if char not in vowels or char.isdigit()])
    return name_no_vowels[:max_length]

def get_schema_and_table_names(api_response):
    try:
        subject_label = api_response['subject'][0]['label'] if api_response.get('subject') else 'bps_data'
        var_label = api_response['var'][0]['label'] if api_response.get('var') else 'data'
        schema_name = format_schema_name(subject_label)
        table_name = format_table_name(var_label)
        return schema_name, table_name

    except Exception as e:
        log_error(f"Error extracting names: {str(e)}")