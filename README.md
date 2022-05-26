# api_exchange_rate_project
Loading exchange rate data from api to PG

Запуск докера в корне проекта (api_project)

docker-compose up

После того, как запуск отработает нужно создать в airflow подключение, а в PG зарегистрировать сервер

PG: <br />
http://localhost:15432/ <br />
usr@company.com <br />
postgres<br />

Host соответсвует name container для postgres:13<br />
Object -> Register -> Server<br />
Name: localhost<br />
Host: api_project-postgres-1<br />
Port: 5432<br />
Database/Username/Password: airflow<br />

Airflow:<br />
http://localhost:8080/<br />
airflow<br />
airflow<br />
Admin->connections<br />

Добавляем новый<br />
Connection_id:postgres_airflow<br />
Connection_type: postgres<br />
Host: api_project-postgres-1<br />
Schema/Login/Password: airflow<br />

Готово, можем запускать даги<br />

exch_load_hist - создает схему и таблицу raw_data.exch_rate_btc_usd при необходимости, очищает перед загрузкой .<br />
Грузит данные из api с начала 2021 года. Загрузка занимает 4-6 мин.<br />
exch_load - раз в 3 часа записывает в таблицу новые данные.<br />

SELECT currency_pair, dt, rate<br />
	FROM raw_data.exch_rate_btc_usd;<br />
