# Data Engineer Portfolio

Портфолио проектов по Data Engineering.

## Кейс 01: 1C Excel Pipeline

Загрузка заказов из Excel-выгрузок 1С в PostgreSQL.

**Технологии:** Apache Airflow, Pandas, openpyxl, PostgreSQL, Docker.

## Быстрый старт

git clone https://github.com/olkhovii/data-engineer-portfolio.git
cd data-engineer-portfolio
docker-compose up -d

Airflow UI: http://localhost:8080 (admin/admin)

## Проверка результата

docker exec data-engineer-portfolio-postgres-1 psql -U airflow -d airflow_db -c "SELECT * FROM staging_client_orders;"

## Контакты

GitHub: https://github.com/olkhovii