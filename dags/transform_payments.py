import pandas as pd
from transformations import get_mysql_engine, get_mssql_engine

def trans_payments():
    # 1. Load from MySQL Staging
    mysql_engine = get_mysql_engine()
    df_pay = pd.read_sql('SELECT * FROM olist_order_payments', mysql_engine)
    
    # 2. TRANSFORMING: Mapping to FACT_PAYMENT
    # Chúng ta lấy OrderID và các cột Measure. DateKey tạm thời lấy từ hôm nay 
    # hoặc bạn có thể join với bảng orders để lấy ngày chính xác.
    dw_df = pd.DataFrame()
    dw_df['OrderID'] = df_pay['order_id']
    dw_df['CustomerKey'] = 1 # Dummy
    dw_df['DateKey'] = 20240403 # Dummy
    dw_df['SellerKey'] = 1 # Dummy
    dw_df['PaymentTypeKey'] = 1 # Sẽ cần bảng mapping sau
    dw_df['PaymentValue'] = df_pay['payment_value']
    dw_df['PaymentInstallments'] = df_pay['payment_installments']
    
    # 3. Load to MSSQL Warehouse
    mssql_engine = get_mssql_engine()
    dw_df.to_sql('FACT_PAYMENT', con=mssql_engine, if_exists='append', index=False)
    print("Transformed and Loaded Payments to DW")
