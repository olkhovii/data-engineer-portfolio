# Data Engineer Portfolio

[![Airflow](https://img.shields.io/badge/Airflow-2.10.3-blue)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-24.0-blue)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.12-green)](https://www.python.org/)

Портфолио проектов по Data Engineering. Каждый кейс — самостоятельный ETL-пайплайн с полной документацией и возможностью локального запуска.

---

## 📋 Кейсы

| № | Название | Описание | Технологии | Статус |
|---|----------|----------|------------|--------|
| 01 | [1C Excel Pipeline](cases/01-1c-excel-pipeline/) | Загрузка заказов из Excel-выгрузок 1С в PostgreSQL | Airflow, Pandas, openpyxl | ✅ Готов |
| 02 | Yandex Direct API | Асинхронная выгрузка статистики рекламных кампаний | Airflow, Yandex API | 📅 Планируется |
| 03 | Google Sheets Connector | ETL из Google Sheets в DWH | Airflow, Google Sheets API | 📅 Планируется |

---

## 🚀 Быстрый старт

```bash
git clone https://github.com/olkhovii/data-engineer-portfolio.git
cd data-engineer-portfolio
docker-compose up -d
```

Airflow UI: http://localhost:8080 (admin/admin)

---

## 📊 Кейс 01: 1C Excel Pipeline

### Проблема

Выгрузки из 1С приходят в виде "грязных" Excel-файлов:
- Служебные строки перед данными
- Объединённые ячейки
- Даты в разных форматах
- Лишние колонки

### Решение

ETL-пайплайн на Apache Airflow:

```
1С (Excel) → Airflow DAG → PostgreSQL (staging_client_orders)
```

### Быстрый запуск кейса

```bash
# 1. Создать тестовый файл за вчерашнюю дату
docker exec -u airflow data-engineer-portfolio-webserver-1 python -c "
import pandas as pd
from datetime import datetime, timedelta

yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
data = {
    'Клиент': ['ООО Тест', 'ИП Пример'],
    'Заказ клиента.Номер': ['ТЕСТ-001', 'ТЕСТ-002'],
    'Сумма документа': [10000, 20000]
}
pd.DataFrame(data).to_excel(
    f'/opt/airflow/dags/cases/01-1c-excel-pipeline/data/samples/Реализации_{yesterday}.xlsx',
    index=False
)
print('✅ Тестовый файл создан')
"

# 2. Запустить DAG
docker exec -u airflow data-engineer-portfolio-scheduler-1 airflow dags trigger 1c_orders_etl

# 3. Проверить результат
docker exec data-engineer-portfolio-postgres-1 psql -U airflow -d airflow_db -c "SELECT * FROM staging_client_orders;"
```

### Структура таблицы

```sql
CREATE TABLE staging_client_orders (
    id SERIAL PRIMARY KEY,
    client_name VARCHAR(255),
    order_number VARCHAR(100),
    order_date DATE,
    amount DECIMAL(15,2),
    load_date DATE,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Особенности реализации

- ✅ Автоопределение заголовков
- ✅ Идемпотентность (нет дублей)
- ✅ Обработка ошибок
- ✅ Docker-оркестрация

---

## 📁 Структура проекта

```
data-engineer-portfolio/
├── cases/
│   └── 01-1c-excel-pipeline/
│       ├── dags/orders_etl.py
│       ├── sql/01_create_table.sql
│       └── data/samples/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## 🛠 Требования

- Docker 24.0+
- Docker Compose 2.20+

---

## 🐛 Troubleshooting

### Контейнеры не запускаются

```bash
docker-compose down && docker-compose up -d
```

### DAG не появляется в UI

```bash
docker-compose restart scheduler webserver
```

---

## 📞 Контакты

- [GitHub](https://github.com/olkhovii)

---

## 📝 Лицензия

MIT

---

*⭐ Если проект оказался полезным, поставьте звезду!*
