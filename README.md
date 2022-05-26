# api_exchange_rate_project
Loading exchange rate data from api to PG

Запуск докера в корне проекта (api_project)

docker-compose up

После того, как запуск отработает нужно создать в airflow подключение, а в PG зарегистрировать сервер
PG:
http://localhost:15432/
usr@company.com
postgres
Object -> Register -> Server
Name: localhost
Host: api_project-postgres-1
Port: 5432
Database/Username/Password: airflow


Airflow:
http://localhost:8080/
airflow
airflow
Admin->connections
Добавляем
Connection_id:postgres_airflow
Connection_type: postgres
Host: api_project-postgres-1
Schema/Login/Password: airflow

Готово, можем запускать даги

exch_load_hist - создает схему и таблицу raw_data.exch_rate_btc_usd при необходимости, очищает перед загрузкой .
Грузит данные из api с начала 2021 года. Загрузка занимает 4-6 мин.
exch_load - раз в 3 часа записывает в таблицу новые данные.

SELECT currency_pair, dt, rate
	FROM raw_data.exch_rate_btc_usd;
