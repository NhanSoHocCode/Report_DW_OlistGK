import os
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pymssql

def create_dw_staging():
    # ─── 1. KHỞI TẠO MYSQL STAGING (Vẫn giữ logic chuẩn) ──────────
    print("--- Khởi tạo MySQL Staging Tables ---")
    mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
    mysql_tables = [
        "DROP TABLE IF EXISTS olist_customers",
        "CREATE TABLE olist_customers (customer_id VARCHAR(100), customer_unique_id VARCHAR(100), customer_zip_code_prefix INT, customer_city VARCHAR(100), customer_state CHAR(5))",
        "DROP TABLE IF EXISTS olist_sellers",
        "CREATE TABLE olist_sellers (seller_id VARCHAR(100), seller_zip_code_prefix INT, seller_city VARCHAR(100), seller_state CHAR(5))",
        "DROP TABLE IF EXISTS olist_products",
        "CREATE TABLE olist_products (product_id VARCHAR(100), product_category_name VARCHAR(100), product_name_lenght INT, product_description_lenght INT, product_photos_qty INT, product_weight_g FLOAT, product_length_cm FLOAT, product_height_cm FLOAT, product_width_cm FLOAT)",
        "DROP TABLE IF EXISTS olist_order_items",
        "CREATE TABLE olist_order_items (order_id VARCHAR(100), order_item_id INT, product_id VARCHAR(100), seller_id VARCHAR(100), shipping_limit_date DATETIME, price FLOAT, freight_value FLOAT)",
        "DROP TABLE IF EXISTS olist_orders",
        "CREATE TABLE olist_orders (order_id VARCHAR(100), customer_id VARCHAR(100), order_status VARCHAR(20), order_purchase_timestamp DATETIME, order_approved_at DATETIME, order_delivered_carrier_date DATETIME, order_delivered_customer_date DATETIME, order_estimated_delivery_date DATETIME)",
        "DROP TABLE IF EXISTS olist_order_payments",
        "CREATE TABLE olist_order_payments (order_id VARCHAR(100), payment_sequential INT, payment_type VARCHAR(50), payment_installments INT, payment_value FLOAT)",
        "DROP TABLE IF EXISTS olist_order_reviews",
        "CREATE TABLE olist_order_reviews (review_id VARCHAR(100), order_id VARCHAR(100), review_score INT, review_comment_title TEXT, review_comment_message TEXT, review_creation_date DATETIME, review_answer_timestamp DATETIME)",
        "DROP TABLE IF EXISTS olist_geolocation",
        "CREATE TABLE olist_geolocation (geolocation_zip_code_prefix INT, geolocation_lat DOUBLE, geolocation_lng DOUBLE, geolocation_city VARCHAR(255), geolocation_state CHAR(5))",
        "DROP TABLE IF EXISTS product_category_name_translation",
        "CREATE TABLE product_category_name_translation (product_category_name VARCHAR(100), product_category_name_english VARCHAR(100))"
    ]
    for sql in mysql_tables:
        try: mysql_hook.run(sql)
        except: pass

    # ─── 2. ĐỌC VÀ THỰC THI SCRIPT SQL TỪ FILE (SQL SERVER) ───────
    print("--- Khởi tạo Data Warehouse từ file scriptDW.sql ---")
    user = os.getenv('MSSQL_USER', 'sa').strip()
    password = os.getenv('MSSQL_PASSWORD', 'Ngocnhan2711#').strip()
    conn = pymssql.connect(server='sqlserver', user=user, password=password, database='master')
    conn.autocommit(True)
    cursor = conn.cursor()

    # Đọc file scriptDW.sql (Đường dẫn /tmp/scriptDW.sql trong container Airflow)
    script_path = '/tmp/scriptDW.sql'
    if not os.path.exists(script_path):
        print(f"Lỗi: Không tìm thấy file {script_path} trong container!")
        return

    with open(script_path, 'r', encoding='utf-8') as f:
        full_script = f.read()

    # Tách script theo lệnh "GO" và loại bỏ các dòng comment/rác
    commands = full_script.split('GO')
    
    for cmd in commands:
        clean_cmd = cmd.strip()
        if clean_cmd:
            try:
                cursor.execute(clean_cmd)
                print(f"Thành công: {clean_cmd[:50]}...")
            except Exception as e:
                print(f"Bỏ qua/Lỗi một khối lệnh: {e}")

    conn.close()
    print("--- KHỞI TẠO TOÀN BỘ HỆ THỐNG XONG! ---")

if __name__ == "__main__":
    create_dw_staging()
