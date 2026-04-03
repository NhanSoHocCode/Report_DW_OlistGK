
to_mysql_staging:
	docker exec -it mysql_staging mysql -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DATABASE}"

to_sqlserver:
	/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "${MSSQL_PASSWORD}" -d "${MSSQL_DATABASE}"

mysql_load:
	docker exec -it mysql_staging mysql --local_infile -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DATABASE}" -e "source /tmp/load_dataset/mysql_load.sql"
