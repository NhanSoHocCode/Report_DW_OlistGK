
import pandas as pd
from transformations import get_mysql_engine

def load():
    engine = get_mysql_engine()
    
    datasets = {
        'olist_customers_dataset.csv': 'olist_customers',
        'olist_orders_dataset.csv': 'olist_orders',
        'olist_products_dataset.csv': 'olist_products',
        'olist_sellers_dataset.csv': 'olist_sellers',
        'olist_order_items_dataset.csv': 'olist_order_items',
        'olist_order_payments_dataset.csv': 'olist_order_payments',
        'olist_order_reviews_dataset.csv': 'olist_order_reviews',
        'olist_geolocation_dataset.csv': 'olist_geolocation',
        'product_category_name_translation.csv': 'product_category_name_translation'
    }
    
    for file, table in datasets.items():
        path = f'/tmp/dataset/{file}'
        df = pd.read_csv(path)
        df.to_sql(table, con=engine, if_exists='append', index=False)
        print(f"Loaded {file} into {table}")
