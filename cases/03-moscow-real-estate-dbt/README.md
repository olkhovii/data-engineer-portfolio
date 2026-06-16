# Кейс 03: Moscow Real Estate — dbt + ClickHouse + ML

> **ELT-пайплайн для анализа рынка недвижимости Москвы:**  
> сырые CSV → ClickHouse → dbt (Staging / Intermediate / Marts) → ML-модель предсказания цены

---

## Содержание

- [Проблема и цель](#проблема-и-цель)
- [Архитектура решения](#архитектура-решения)
- [Данные](#данные)
- [Технологии](#технологии)
- [Быстрый запуск](#быстрый-запуск)
- [Слои данных](#слои-данных)
- [Результаты анализа](#результаты-анализа)
- [ML-модель](#ml-модель)
- [Тесты и документация](#тесты-и-документация)
- [Структура проекта](#структура-проекта)
- [Связь с другими кейсами](#связь-с-другими-кейсами)

---

## Проблема и цель

Аналитикам рынка недвижимости нужна **чистая, обогащённая и документированная** витрина данных для трёх задач:

1. **Мониторинг рынка** — динамика цен по районам Москвы в разрезе типов объектов
2. **ML-прогнозирование** — предсказание цены за кв.м на основе характеристик объекта и локации
3. **Макроанализ** — связь цен с ключевой ставкой ЦБ и доступностью метро

**Исходная проблема:** данные лежат в 5 разрозненных CSV-файлах с разными схемами, дублями, пропусками и несовместимыми форматами дат и адресов.

---

## Архитектура решения

```
┌─────────────────────────────────────────────────────────────────┐
│                        ИСТОЧНИКИ                                │
│  secondary_market.csv  new_builds.csv  rentals.csv              │
│  district_prices.csv   metro_stations.csv                       │
└────────────────────────────┬────────────────────────────────────┘
                             │ Python load_data.py
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   RAW LAYER  (ClickHouse)                       │
│  raw.secondary_market  raw.new_builds  raw.rentals              │
│  raw.district_prices   raw.metro_stations                       │
└────────────────────────────┬────────────────────────────────────┘
                             │ dbt run
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGING LAYER  (dbt models)                     │
│  stg_secondary_market  stg_new_builds  stg_rentals              │
│  stg_district_prices   stg_metro_stations                       │
│  → очистка, типизация, переименование колонок                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              INTERMEDIATE LAYER  (dbt models)                   │
│  int_listings_enriched                                          │
│  → объединение вторичка + новостройки + аренда                  │
│  → обогащение метро (ближайшая станция, расстояние)             │
│  → подтяжка ставки ЦБ по дате                                   │
│  → категории цен, сегменты доступности метро                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                    ┌────────┴────────┐
                    ▼                 ▼
┌──────────────────────┐  ┌──────────────────────────┐
│    MARTS LAYER       │  │       MARTS LAYER        │
│  dashboard_district  │  │    ml_price_factors      │
│  _trends             │  │                          │
│  → агрегаты по       │  │  → плоская таблица       │
│    районам и месяцам │  │    для обучения модели   │
└──────────────────────┘  └──────────┬───────────────┘
                                     │ python train_price_model.py
                                     ▼
                          ┌──────────────────────────┐
                          │   ML-МОДЕЛЬ              │
                          │   Random Forest          │
                          │   R² = 0.85              │
                          └──────────────────────────┘
```

---

## Данные

| Источник | Файл | Записей | Описание |
|---|---|---|---|
| Вторичный рынок | `secondary_market.csv` | ~50 000 | Объявления о продаже квартир |
| Новостройки | `new_builds.csv` | ~8 000 | Объявления от застройщиков |
| Аренда | `rentals.csv` | ~20 000 | Объявления об аренде |
| Цены по районам | `district_prices.csv` | ~9 800 | Помесячная динамика цен |
| Станции метро | `metro_stations.csv` | 104 | Координаты и линии метро |

**Итого в пайплайне: ~78 000 объявлений** из трёх сегментов рынка.

Источник данных: [Kaggle — Moscow Real Estate](https://www.kaggle.com/datasets/sergionefedov/moscow-real-estate-sales-and-rentals-20202026)

---

## Технологии

| Технология | Роль |
|---|---|
| **ClickHouse 24.3** | Колоночная БД: хранение RAW и трансформированных данных |
| **dbt Core** | Трансформация: SQL-модели, тесты, документация, линэдж |
| **Docker / docker-compose** | Изолированное окружение, воспроизводимость |
| **Python 3.11** | Загрузка CSV в ClickHouse, обучение ML-модели |
| **scikit-learn** | Random Forest для предсказания цены за кв.м |
| **clickhouse-driver** | Python-коннектор к ClickHouse |

---

## Быстрый запуск

### Предварительные требования

- Docker Desktop (или Docker Engine + Compose)
- Python 3.9+
- dbt-clickhouse (`pip install dbt-clickhouse`)

### 1. Запустить ClickHouse

ClickHouse уже добавлен в общий docker-compose.yml в корне портфолио.

```bash
# Из корня портфолио
cd /path/to/data-engineer-portfolio
docker-compose up -d clickhouse

# Проверка
curl http://localhost:8123/ping   # → Ok.
```

### 2. Загрузить RAW-данные

```bash
# Перейди в папку кейса
cd cases/03-moscow-real-estate-dbt

# Активируй виртуальное окружение (если ещё нет)
python3 -m venv venv
source venv/bin/activate
pip install clickhouse-connect pandas

# Скачай CSV-файлы с Kaggle в папку data/raw/
# (ссылку на датасет см. в разделе "Данные")

# Загрузи данные
python scripts/load_data.py
```

Проверка загрузки:

```sql
-- подключись через clickhouse-client
SELECT name FROM system.tables WHERE database = 'real_estate' AND name LIKE 'raw_%';
```

### 3. Запустить dbt-трансформации

```bash
cd moscow_real_estate_dbt

# Установи dbt и адаптер (если не сделано)
pip install dbt-core dbt-clickhouse

# Настрой профиль dbt: создай ~/.dbt/profiles.yml
# Пример конфига:
# moscow_real_estate_dbt:
#   target: dev
#   outputs:
#     dev:
#       type: clickhouse
#       host: localhost
#       port: 8123
#       user: default
#       password: ''
#       schema: real_estate

# Запусти трансформации
dbt run
dbt test
```

### 4. Открыть документацию

```bash
dbt docs generate
dbt docs serve --port 8081
# → http://localhost:8081
```

### 5. Обучить ML-модель

```bash
# Из папки moscow_real_estate_dbt
cd ..
python ml/train_price_model.py
# Артефакты сохраняются в moscow_real_estate_dbt/ml_artifacts/
```

---

## Слои данных

### Staging — очистка и типизация

Каждая модель делает одно и то же: приводит сырые данные к единому стандарту.

| Модель | Что делает |
|---|---|
| `stg_secondary_market` | Чистка адресов, cast цен в Float64, парсинг дат |
| `stg_new_builds` | Нормализация имён застройщиков, фильтр нулевых цен |
| `stg_rentals` | Унификация типов комнат, удаление дублей |
| `stg_district_prices` | Парсинг year_month → Date, фильтр выбросов цен |
| `stg_metro_stations` | Cast координат в Float64, нормализация названий линий |

### Intermediate — обогащение

`int_listings_enriched` объединяет все три сегмента и добавляет:

- **Ближайшее метро** — через оконную функцию по минимальной дистанции (haversine)
- **Ставку ЦБ** — `asof join` по дате объявления
- **Сегмент цены** — `economy / business / premium` (по квантилям внутри района)
- **Доступность метро** — `walking / close / reachable / far` (по минутам пешком)
- **Удалённость от центра** — в км от координат Кремля

### Marts — витрины для конечного потребления

**`dashboard_district_trends`** — для дашборда:

```
district | month | listing_type | avg_price_sqm | median_price_sqm
         | count_listings | yoy_change_pct | mom_change_pct
```

**`ml_price_factors`** — для ML-модели:

```
listing_id | price_sqm (target) | total_area | rooms | floor_ratio
           | to_center_km | metro_distance_min | metro_accessibility
           | district | listing_type | cbr_key_rate_pct | price_segment
```

---

## Результаты анализа

### Распределение по ценовым сегментам (продажа)

| Сегмент | Доля |
|---|---|
| Economy | 32.5% |
| Business | 32.3% |
| Premium | 35.2% |

### Топ-5 районов по медианной цене за кв.м

| # | Район | Медиана, руб/кв.м |
|---|---|---|
| 1 | Хамовники | ~480 000 |
| 2 | Арбат | ~460 000 |
| 3 | Тверской | ~440 000 |
| 4 | Пресненский | ~410 000 |
| 5 | Якиманка | ~395 000 |

### Влияние метро

Объекты в пешей доступности (до 5 мин) стоят в среднем на **18–24% дороже**, чем аналогичные объекты в той же локации с ближайшим метро более 15 минут ходьбы.

---

## ML-модель

### Подход

- Алгоритм: **Random Forest Regressor** (scikit-learn)
- Целевая переменная: `price_sqm` (цена за кв.м, руб.)
- Обучение: 80% / тест: 20%, стратификация по `district`
- Категориальные признаки закодированы через `LabelEncoder` (артефакты сохранены)

### Метрики качества

| Метрика | Значение |
|---|---|
| **R² (тест)** | **0.85** |
| MAE | 30 345 руб/кв.м |
| RMSE | 43 966 руб/кв.м |

### Важность признаков (топ-5)

| # | Признак | Важность |
|---|---|---|
| 1 | `to_center_km` (удалённость от центра) | 60.7% |
| 2 | `district_encoded` (район) | 9.6% |
| 3 | `metro_accessibility_encoded` (доступность метро) | 8.6% |
| 4 | `metro_distance_min` (расстояние до метро, мин) | 5.7% |
| 5 | `cbr_key_rate_pct` (ключевая ставка ЦБ) | 5.2% |

**Главный вывод:** локация (центр → район → метро) объясняет почти **75–80% вариации цены**. Макроэкономика (ставка ЦБ) даёт дополнительные ~5% — значимо, но не определяющий фактор.

### Артефакты модели

```
ml_artifacts/
├── price_model.joblib    # обученная модель
├── le_listing.joblib     # энкодер типа объявления
├── le_district.joblib    # энкодер района
└── le_metro.joblib       # энкодер доступности метро
```

---

## Тесты и документация

### dbt-тесты

В `schema.yml` покрыты:

- `not_null` на все ключевые поля (id, price, district, date)
- `unique` на `listing_id` в marts-витринах
- `accepted_values` для `listing_type`, `price_segment`, `metro_accessibility`

### Кастомные тесты

| Тест | Что проверяет |
|---|---|
| `assert_price_positive` | `price_sqm > 0` во всех моделях |
| `assert_metro_distance_valid` | `metro_distance_min` от 0 до 120 |
| `assert_coords_moscow_bounds` | Координаты в пределах Московской агломерации |
| `assert_area_realistic` | `total_area` от 10 до 1000 кв.м |

### Документация

После `dbt docs generate` доступны:

- **Граф линэджа** — визуальный DAG от RAW до Marts
- **Описание всех таблиц и колонок** — с типами и бизнес-смыслом
- **Результаты тестов** — история прогонов
- **Источники** — описание 5 RAW-таблиц с ClickHouse

---

## Структура проекта

```
cases/03-moscow-real-estate-dbt/
├── README.md                       # Документация кейса
├── data/
│   └── raw/                        # CSV-файлы (не в git)
├── scripts/
│   └── load_data.py                # Загрузка CSV → ClickHouse
├── sql/raw/                        # Схемы RAW-таблиц
│   ├── 01_raw_secondary_market.sql
│   ├── 02_raw_new_builds.sql
│   ├── 03_raw_rentals.sql
│   ├── 04_raw_district_prices.sql
│   └── 05_raw_metro_stations.sql
├── moscow_real_estate_dbt/         # dbt-проект
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── stg_secondary_market.sql
│   │   │   ├── stg_new_builds.sql
│   │   │   ├── stg_rentals.sql
│   │   │   ├── stg_district_prices.sql
│   │   │   └── stg_metro_stations.sql
│   │   ├── intermediate/
│   │   │   └── int_listings_enriched.sql
│   │   ├── marts/
│   │   │   ├── dashboard_district_trends.sql
│   │   │   ├── ml_price_factors.sql
│   │   │   └── schema.yml          # Тесты
│   │   └── schema.yml
│   └── ml/                         # ML-скрипты
│       └── train_price_model.py
└── ml_artifacts/                   # .gitkeep (модели не в Git)
```

---

## Связь с другими кейсами

| Кейс | Стек | Тема |
|---|---|---|
| [01 — 1C Excel](../01-1c-excel/) | Python, PostgreSQL | Загрузка и нормализация выгрузок 1C |
| [02 — Yandex Direct](../02-yandex-direct/) | Python, PostgreSQL, API | Автоматизация рекламной аналитики |
| **03 — Moscow Real Estate** | **dbt, ClickHouse, ML** | **ELT-пайплайн + предсказание цен** |

---

## Контакты

- **GitHub:** [github.com/olkhovii](https://github.com/olkhovii)
- **Telegram:** [@olkhovii](https://t.me/olkhovii)

---

*Кейс выполнен как часть портфолио Data Engineer. Данные взяты из открытых источников (Kaggle) и используются в образовательных целях.*
