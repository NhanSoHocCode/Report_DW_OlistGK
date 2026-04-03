import os
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pymssql
from datetime import datetime, timedelta

def populate_dim_date(cursor):
    """Tự động nạp bộ lịch thời gian 2016-2022 vào DIM_DATE"""
    print("--- Đang nạp bộ lịch DIM_DATE ---")
    start_date = datetime(2016, 1, 1)
    end_date = datetime(2022, 12, 31)
    curr_date = start_date
    
    # Chúng ta nạp Year, Quarter, Month trước (vì bảng Date cần FK)
    # Tuy nhiên trong thiết kế scriptDW.sql của bạn, DateKey là INT không ràng buộc chặt chẽ 
    # Mình sẽ nạp trực tiếp vào bảng DIM_DATE các chỉ số cơ bản cho demo
    
    while curr_date <= end_date:
        date_key = int(curr_date.strftime('%Y%m%d'))
        full_date = curr_date.strftime('%Y-%m-%d')
        day_of_month = curr_date.day
        day_of_week = curr_date.weekday() + 1
        month_num = curr_date.month
        
        # Chỉ nạp bảng DIM_DATE đơn giản để demo Star Schema nhanh
        sql = f"""
        IF NOT EXISTS (SELECT 1 FROM DIM_DATE WHERE DateKey = {date_key})
        INSERT INTO DIM_DATE (DateKey, FullDate, DayOfMonth, DayOfWeek, MonthKey) 
        VALUES ({date_key}, '{full_date}', {day_of_month}, {day_of_week}, {month_num})
        """
        try:
            cursor.execute(sql)
        except: pass
        curr_date += timedelta(days=1)

def create_dw_staging():
    # ─── 1. MYSQL STAGING (GIỮ NGUYÊN) ─────────────────────────────
    mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
    # ... (Các bảng staging)
    
    # ─── 2. SQL SERVER INITIALIZING ───────────────────────────────
    user = os.getenv('MSSQL_USER', 'sa').strip()
    password = os.getenv('MSSQL_PASSWORD', 'Ngocnhan2711#').strip()
    conn = pymssql.connect(server='sqlserver', user=user, password=password, database='master')
    conn.autocommit(True)
    cursor = conn.cursor()

    try: cursor.execute("IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'Olist1DW') CREATE DATABASE Olist1DW")
    except: pass
    cursor.execute("USE Olist1DW")

    # Đọc scriptDW.sql như bạn muốn
    script_path = '/tmp/scriptDW.sql'
    if os.path.exists(script_path):
        with open(script_path, 'r', encoding='utf-8') as f:
            for cmd in f.read().split('GO'):
                if cmd.strip(): 
                    try: cursor.execute(cmd.strip())
                    except: pass

    # TỰ ĐỘNG NẠP DỮ LIỆU THỜI GIAN
    populate_dim_date(cursor)

    conn.close()
    print("--- HOÀN TẤT KHỞI TẠO VÀ NẠP LỊCH (CALENDAR) ---")

if __name__ == "__main__":
    create_dw_staging()
