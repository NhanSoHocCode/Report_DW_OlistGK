from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

# Import các script khởi tạo
from create_table import create_dw_staging
from delete_table import delete_table
from load_into_mysql import load

# Import các Dimension Transformations
from transform_customers import trans_customers
from transform_products import trans_products
from transform_sellers import trans_sellers
from transform_geolocation import trans_geolocation
from transform_product_category import trans_product_category

# Import các Fact Transformations (Quan trọng)
from transform_full_orders import trans_full_orders
from transform_payments import trans_payments
from transform_delivery import trans_delivery
from transform_reviews import trans_reviews

default_args = {
    'owner': 'Student',
    'retries': 0,
    'retry_delay': timedelta(seconds=20)
}

with DAG(
    dag_id='project_giuaky_dag',
    default_args=default_args,
    start_date=datetime(2026, 4, 3),
    schedule_interval='@daily', # Tự động chạy vào lúc 00:00 hàng ngày
    catchup=False
) as dag:
    # ─── BƯỚC KHỞI TẠO ─────────────────────────────────────────
    task_delete = PythonOperator(task_id='delete_staging', python_callable=delete_table)
    task_create = PythonOperator(task_id='create_dw_staging', python_callable=create_dw_staging)
    task_load = PythonOperator(task_id='load_staging', python_callable=load)
    
    # ─── BƯỚC CHUYỂN HÓA DỮ LIỆU (DIMENSIONS) ───────────────────
    task_trans_cus = PythonOperator(task_id='trans_customers', python_callable=trans_customers)
    task_trans_prod = PythonOperator(task_id='trans_products', python_callable=trans_products)
    task_trans_sell = PythonOperator(task_id='trans_sellers', python_callable=trans_sellers)
    task_trans_geo = PythonOperator(task_id='trans_geolocation', python_callable=trans_geolocation)
    task_trans_pro_cate = PythonOperator(task_id='trans_product_category', python_callable=trans_product_category)
    
    # ─── BƯỚC CHUYỂN HÓA DỮ LIỆU (FACT TABLES) ───────────────────
    task_trans_ords = PythonOperator(task_id='trans_orders', python_callable=trans_full_orders)
    task_trans_pay  = PythonOperator(task_id='trans_payments', python_callable=trans_payments)
    task_trans_delv = PythonOperator(task_id='trans_delivery', python_callable=trans_delivery)
    task_trans_revs = PythonOperator(task_id='trans_reviews', python_callable=trans_reviews)

    # ─── THỨ TỰ THỰC THI (WORKFLOW) ──────────────────────────────
    # 1. Khởi tạo xong mới load dữ liệu
    task_delete >> task_create >> task_load
    
    # 2. Load xong mới bắt đầu transform Dimension
    task_load >> [task_trans_cus, task_trans_prod, task_trans_sell, task_trans_geo, task_trans_pro_cate]
    
    # 3. Sau khi Dimensions sẵn sàng (hoặc song song), transform các bảng FACT
    # Lưu ý: Fact thường chạy sau cùng vì nó phụ thuộc vào khóa ngoại các Dimension
    # 3. Sau khi Dimensions sẵn sàng, transform toàn bộ các bảng FACT (CHẠY SONG SONG)
    [task_trans_cus, task_trans_prod, task_trans_sell, task_trans_geo, task_trans_pro_cate] >> task_trans_ords
    [task_trans_cus, task_trans_prod, task_trans_sell, task_trans_geo, task_trans_pro_cate] >> task_trans_pay
    [task_trans_cus, task_trans_prod, task_trans_sell, task_trans_geo, task_trans_pro_cate] >> task_trans_delv
    [task_trans_cus, task_trans_prod, task_trans_sell, task_trans_geo, task_trans_pro_cate] >> task_trans_revs
