import pandas as pd
from transformations import get_mysql_engine, get_mssql_engine
from datetime import datetime

def trans_products():
    # 1. Load from MySQL Staging
    mysql_engine = get_mysql_engine()
    df_products = pd.read_sql('SELECT * FROM olist_products', mysql_engine)
    
    # 2. TRANSFORMING
    dw_df = pd.DataFrame()
    dw_df['ProductID'] = df_products['product_id']
    dw_df['CategoryKey'] = None # Để NULL vì chúng ta đã nới lỏng schema
    dw_df['ProductWeight'] = df_products['product_weight_g']
    dw_df['RowIsCurrent'] = 1
    dw_df['RowStartDate'] = datetime.now().date()
    dw_df['RowEndDate'] = None
    dw_df['RowChangeReason'] = 'Initial Load'
    
    # 3. Load to MSSQL Warehouse
    mssql_engine = get_mssql_engine()
    dw_df.to_sql('DIM_PRODUCT', con=mssql_engine, if_exists='append', index=False)
    print("Transformed and Loaded Products to DW")
