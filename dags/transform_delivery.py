import pandas as pd
from transformations import get_mysql_engine, get_mssql_engine
from datetime import datetime

def trans_delivery():
    # 1. Load from MySQL Staging
    mysql_engine = get_mysql_engine()
    df_orders = pd.read_sql('SELECT * FROM olist_orders', mysql_engine)
    df_items = pd.read_sql('SELECT * FROM olist_order_items', mysql_engine) # Để lấy SellerID
    
    # Merge để có đủ thông tin Order và Seller
    merged = pd.merge(df_orders, df_items, on='order_id')
    
    # 2. TRANSFORMING: Tính toán các chỉ số vận chuyển
    dw_df = pd.DataFrame()
    dw_df['OrderID'] = merged['order_id']
    dw_df['CustomerKey'] = 1 # Dummy
    dw_df['SellerKey'] = 1   # Dummy
    
    # Chuyển đổi sang DateKey (YYYYMMDD) - Giả định lấy ngày mua hàng làm key
    merged['purchase_date'] = pd.to_datetime(merged['order_purchase_timestamp'])
    dw_df['DatePurchaseKey'] = merged['purchase_date'].dt.strftime('%Y%m%d').astype(int)
    
    # Ngày giao hàng thực tế
    merged['delivered_date'] = pd.to_datetime(merged['order_delivered_customer_date'])
    dw_df['DateDeliveredKey'] = merged['delivered_date'].dt.strftime('%Y%m%d').fillna(0).astype(int)
    
    # Chỉ số: Số ngày giao hàng thực tế (Delivered - Purchase)
    dw_df['ActualDeliveryDays'] = (merged['delivered_date'] - merged['purchase_date']).dt.days
    
    # Chỉ số: Có bị trễ không (Delivered > Estimated)
    merged['estimated_date'] = pd.to_datetime(merged['order_estimated_delivery_date'])
    dw_df['IsLate'] = (merged['delivered_date'] > merged['estimated_date']).astype(int)
    
    # 3. Load to MSSQL Warehouse
    mssql_engine = get_mssql_engine()
    dw_df.to_sql('FACT_DELIVERY', con=mssql_engine, if_exists='append', index=False)
    print("Transformed and Loaded Delivery Data to DW")
