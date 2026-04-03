import pandas as pd
from transformations import get_mysql_engine, get_mssql_engine

def trans_geolocation():
    # 1. Load from MySQL Staging
    mysql_engine = get_mysql_engine()
    df_geo = pd.read_sql('SELECT * FROM olist_geolocation', mysql_engine)
    
    # 2. TRANSFORMING
    dw_df = pd.DataFrame()
    dw_df['ZipCodePrefix'] = df_geo['geolocation_zip_code_prefix']
    dw_df['GeoLocationLat'] = df_geo['geolocation_lat']
    dw_df['GeoLocationLng'] = df_geo['geolocation_lng']
    dw_df['CityKey'] = None 
    
    # 3. Load to MSSQL Warehouse
    mssql_engine = get_mssql_engine()
    dw_df.to_sql('DIM_GEOLOCATION', con=mssql_engine, if_exists='append', index=False)
    print("Transformed and Loaded Geolocation to DW")
