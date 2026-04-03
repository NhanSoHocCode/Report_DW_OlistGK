import pandas as pd
from transformations import get_mysql_engine, get_mssql_engine

def trans_product_category():
    # 1. Load from MySQL Staging
    mysql_engine = get_mysql_engine()
    df_cate = pd.read_sql('SELECT * FROM product_category_name_translation', mysql_engine)
    
    # 2. TRANSFORMING
    dw_df = pd.DataFrame()
    dw_df['CategoryName'] = df_cate['product_category_name']
    dw_df['CategoryNameEnglish'] = df_cate['product_category_name_english']
    
    # 3. Load to MSSQL Warehouse
    mssql_engine = get_mssql_engine()
    dw_df.to_sql('DIM_CATEGORY', con=mssql_engine, if_exists='append', index=False)
    print("Transformed and Loaded Product Categories to DW")
