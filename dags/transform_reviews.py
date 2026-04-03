import pandas as pd
from transformations import get_mysql_engine, get_mssql_engine

def trans_reviews():
    # 1. Load from MySQL Staging
    mysql_engine = get_mysql_engine()
    df_reviews = pd.read_sql('SELECT * FROM olist_order_reviews', mysql_engine)
    
    # 2. TRANSFORMING: Đo lường CSAT và Response Time
    dw_df = pd.DataFrame()
    dw_df['ReviewID'] = df_reviews['review_id']
    dw_df['OrderID'] = df_reviews['order_id']
    dw_df['CustomerKey'] = 1 # Dummy
    dw_df['ProductKey'] = 1  # Dummy
    dw_df['ReviewScore'] = df_reviews['review_score']
    
    # Tính thời gian phản hồi đánh giá (ResponseTime)
    creation_date = pd.to_datetime(df_reviews['review_creation_date'])
    answer_date = pd.to_datetime(df_reviews['review_answer_timestamp'])
    
    # Đưa về số giờ trung bình khách hàng mất để đánh giá
    # (Bạn có thể đổi sang số ngày nếu muốn)
    dw_df['ResponseTime'] = (answer_date - creation_date).dt.total_seconds() / 3600
    
    # 3. Load to MSSQL Warehouse
    mssql_engine = get_mssql_engine()
    dw_df.to_sql('FACT_REVIEW', con=mssql_engine, if_exists='append', index=False)
    print("Transformed and Loaded Reviews to DW")
