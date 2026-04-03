import pandas as pd
from transformations import get_mysql_engine, get_mssql_engine

def trans_full_orders():
    # 1. Load from MySQL Staging
    mysql_engine = get_mysql_engine()
    df_orders = pd.read_sql('SELECT * FROM olist_orders', mysql_engine)
    df_items = pd.read_sql('SELECT * FROM olist_order_items', mysql_engine)
    
    # 2. Merge for Fact table
    merged = pd.merge(df_orders, df_items, on='order_id')
    
    # 3. TRANSFORMING: Mapping to Fact Table Schema
    dw_df = pd.DataFrame()
    dw_df['OrderID'] = merged['order_id']
    dw_df['OrderItemID'] = merged['order_item_id']
    dw_df['ProductKey'] = 1 # Dummy Key
    dw_df['CustomerKey'] = 1 # Dummy Key
    dw_df['SellerKey'] = 1 # Dummy Key
    dw_df['OrderDateKey'] = 20240403 # Dummy Date
    dw_df['ShippingDateKey'] = 20240403 # Dummy Date
    dw_df['TotalPrice'] = merged['price']
    dw_df['FreightValue'] = merged['freight_value']
    
    # 4. Load to MSSQL Warehouse
    mssql_engine = get_mssql_engine()
    dw_df.to_sql('FACT_ORDER_ITEM', con=mssql_engine, if_exists='append', index=False)
    print("Transformed and Loaded Fact Order Items to DW")
