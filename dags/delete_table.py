
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

def delete_table():
    # Delete Staging Tables (MySQL)
    mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
    mysql_hook.run("DROP TABLE IF EXISTS olist_customers, olist_orders, olist_products, olist_sellers, olist_order_items, olist_order_payments, olist_order_reviews, olist_geolocation, product_category_name_translation;")
    
    # Delete DW Tables (SQL Server) - The scriptDW.sql already has IF EXISTS DROP logic, 
    # but we can explicitly clear if we want to run scriptDW.sql every time (which we do in task_create_table).
    print("Deleted all staging tables.")
