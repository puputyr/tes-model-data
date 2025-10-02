# dags/bps_data_pipeline.py
import sys
import importlib
from datetime import datetime
from airflow.decorators import dag, task

# Tambahkan path ke folder 'include'
sys.path.append('/opt/airflow/include')

# =================================================================================
# 'CONTROL PANEL' UNTUK DAG ANDA
# Mapping antara nama kolom di sheet dengan file processor yang sesuai.
# =================================================================================
KATEGORI_MAPPING = {
    # 'nama_kolom_di_google_sheet': 'nama_file_processor_tanpa_py'
    'infrastruktur code 1 (catherine)': 'process_kategori_1',
    'pariwisata': 'process_kategori_2',
    # ... tambahkan 8 mapping lainnya di sini ...
}
# =================================================================================

@dag(
    dag_id='bps_data_pipeline',
    start_date=datetime(2025, 10, 2),
    schedule_interval='0 1 * * *',
    catchup=False,
    tags=['bps', 'etl', '11 processor', 'puput','data'],
)
def bps_data_pipeline_dag():
    
    for sheet_col, processor_module_name in KATEGORI_MAPPING.items():
        
        task_id = f"etl_task_{processor_module_name}"
        
        @task(task_id=task_id)
        def dynamic_etl_task(column_name: str, module_name: str):
            """
            Task ini secara dinamis mengimpor dan menjalankan
            fungsi run_etl() dari modul processor yang benar.
            """
            print(f"Mengimpor modul 'processors.{module_name}' untuk kolom '{column_name}'")
            
            # Mengimpor modul secara dinamis, cth: from processors import process_kategori_1
            processor_module = importlib.import_module(f"processors.{module_name}")
            
            # Menjalankan fungsi run_etl() dari modul yang sudah diimpor
            processor_module.run_etl(sheet_column_name=column_name)

        # Menjalankan task dengan parameter yang sesuai dari mapping
        dynamic_etl_task(column_name=sheet_col, module_name=processor_module_name)

# Panggil fungsi DAG untuk mendaftarkannya
bps_data_pipeline_dag()