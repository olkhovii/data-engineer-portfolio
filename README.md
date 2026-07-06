# Data Engineer Portfolio

[![Airflow](https://img.shields.io/badge/Airflow-2.10.3-blue)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)](https://www.postgresql.org/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-24.3-yellow)](https://clickhouse.com/)
[![dbt](https://img.shields.io/badge/dbt-1.8-orange)](https://www.getdbt.com/)
[![Docker](https://img.shields.io/badge/Docker-24.0-blue)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.12-green)](https://www.python.org/)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-1.5-orange)](https://scikit-learn.org/)

Портфолио проектов по **Data Engineering**.  
Каждый кейс — самостоятельный ETL/ELT-пайплайн с полной документацией, тестами и возможностью локального запуска через Docker.

**Что внутри:**
- Автоматизация выгрузок из 1С и API
- Трансформация данных в ClickHouse через dbt
- ML-модели для предсказания цен
- Оркестрация через Apache Airflow
- Воспроизводимые окружения в Docker

---

## 📋 Кейсы

| № | Название | Описание | Технологии | Статус |
|---|----------|----------|------------|--------|
| 01 | [1C Excel Pipeline](cases/01-1c-excel-pipeline/) | Загрузка заказов из Excel-выгрузок 1С в PostgreSQL | Airflow, Pandas, openpyxl | ✅ Готов |
| 02 | [Yandex Direct API](cases/02-yandex-direct-api/) | Выгрузка статистики рекламных кампаний из Яндекс.Директа | Airflow, Yandex API, async reports | ✅ Готов |
 03 | [Moscow Real Estate dbt](cases/03-moscow-real-estate-dbt/) | ELT-пайплайн + ML для недвижимости | ClickHouse, dbt, Docker, Python, scikit-learn | ✅ Готов |
| 04 | Google Sheets Connector | ETL из Google Sheets в DWH | Airflow, Google Sheets API | 📅 Планируется |

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
print('Тестовый файл создан')
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

## 📊 Кейс 02: Yandex Direct API

### Проблема

Маркетинговой команде нужна ежедневная статистика по рекламным кампаниям из Яндекс.Директа. API работает асинхронно, требует обработки rate limits и поддерживает несколько аккаунтов.

### Решение

ETL-пайплайн на Apache Airflow с асинхронными отчётами:

```
Yandex Direct API → Airflow DAG → PostgreSQL (yd_campaigns_stats)
```

### Быстрый запуск кейса

```bash
# 1. Убедиться, что Docker запущен (выполнить в корне репозитория)
docker-compose up -d

# 2. Настроить конфиг (скопировать example и добавить токены)
cp cases/02-yandex-direct-api/config/config.example.json cases/02-yandex-direct-api/config/config.json
# Отредактировать config.json, добавив свои access_token и goal_id

# 3. Запустить DAG
docker exec -u airflow data-engineer-portfolio-scheduler-1 airflow dags trigger yandex_direct_daily_load

# 4. Проверить результат
docker exec data-engineer-portfolio-postgres-1 psql -U airflow -d airflow_db -c "SELECT * FROM yd_campaigns_stats LIMIT 10;"
```

### Структура таблицы

```sql
CREATE TABLE yd_campaigns_stats (
    id SERIAL PRIMARY KEY,
    client_login VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    campaign_name VARCHAR(255),
    campaign_id VARCHAR(100),
    targeting_location_name VARCHAR(255),
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    cost DECIMAL(15,2) DEFAULT 0,
    conversions INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    load_date DATE DEFAULT CURRENT_DATE,
    UNIQUE(client_login, date, campaign_id, targeting_location_name)
);
```

### Особенности реализации

- ✅ Асинхронные отчёты (статус 201 → ожидание → 200)
- ✅ Автоматическая атрибуция (AUTO модель)
- ✅ Rate limits (коды 52, 53) с retry-логикой
- ✅ Поддержка нескольких аккаунтов
- ✅ Идемпотентная загрузка (ON CONFLICT)



## 📊 Кейс 03: Moscow Real Estate dbt

> **Полная документация:** [README кейса](cases/03-moscow-real-estate-dbt/README.md)

### Проблема

Аналитикам рынка недвижимости нужна чистая, обогащённая витрина данных для мониторинга цен и ML-прогнозирования. Данные лежат в 5 разрозненных CSV-файлах с разными схемами.

### Решение

ELT-пайплайн на dbt + ClickHouse:

```
CSV → ClickHouse → dbt (Staging → Intermediate → Marts) → ML-модель
```

### Быстрый запуск

```bash
# Из корня портфолио
docker-compose up -d clickhouse
cd cases/03-moscow-real-estate-dbt
python scripts/load_data.py
cd moscow_real_estate_dbt
dbt run
dbt test
```

### Результаты
Объём данных: 78 000 объявлений

ML-модель: Random Forest, R² = 0.85

Главный фактор цены: удалённость от центра (60.7% важности)

### Особенности реализации

- ✅ Многослойная архитектура (RAW → Staging → Intermediate → Marts)
- ✅ 22 автоматических теста на качество данных
- ✅ Автоматическая документация с линэйджем
- ✅ ML-модель с сохранением артефактов



## 📞 Контакты

- [GitHub](https://github.com/olkhovii)
- [Дашборды и проекты](https://serdyukov.in/datalens)
- [Telegram](https://t.me/olkhovii)
- [Email](mailto:olkhovayae@ya.ru)
