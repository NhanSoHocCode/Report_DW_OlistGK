FROM apache/airflow:2.10.2

# Cài đặt các thư viện cần thiết cho project
RUN pip install --no-cache-dir \
    apache-airflow-providers-mysql \
    apache-airflow-providers-microsoft-mssql \
    pymssql \
    pymysql \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt"