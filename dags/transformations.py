import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv()

def get_mysql_engine():
    connection_url = URL.create(
        drivername="mysql+pymysql",
        username=os.getenv('MYSQL_USER', 'nhan').strip(),
        password=os.getenv('MYSQL_PASSWORD', 'nhan').strip(),
        host=os.getenv('MYSQL_HOST', 'mysql').strip(),
        port=int(os.getenv('MYSQL_PORT', '3306').strip()),
        database=os.getenv('MYSQL_DATABASE', 'olist').strip(),
    )
    return create_engine(connection_url)

def get_mssql_engine():
    connection_url = URL.create(
        drivername="mssql+pymssql",
        username=os.getenv('MSSQL_USER', 'sa').strip(),
        password=os.getenv('MSSQL_PASSWORD', 'Ngocnhan2711#').strip(),
        host=os.getenv('MSSQL_HOST', 'sqlserver').strip(),
        port=int(os.getenv('MSSQL_PORT', '1433').strip()),
        database=os.getenv('MSSQL_DATABASE', 'Olist1DW').strip(),
    )
    return create_engine(connection_url)
