import pandas as pd
from transformations import get_mysql_engine, get_mssql_engine
from datetime import datetime

def trans_customers():
    # 1. Load from MySQL Staging
    mysql_engine = get_mysql_engine()
    df_customers = pd.read_sql('SELECT * FROM olist_customers', mysql_engine)
    
    # 2. TRANSFORMING: Mapping to Warehouse Schema
    dw_df = pd.DataFrame()
    dw_df['CustomerID'] = df_customers['customer_id']
    dw_df['LocationKey'] = None 
    dw_df['RowIsCurrent'] = 1
    dw_df['RowStartDate'] = datetime.now().date()
    dw_df['RowEndDate'] = None
    dw_df['RowChangeReason'] = 'Initial Load'
    
    # 3. Load to MSSQL Warehouse
    mssql_engine = get_mssql_engine()
    dw_df.to_sql('DIM_CUSTOMER', con=mssql_engine, if_exists='append', index=False)
    print("Transformed and Loaded Customers to DW")
