"""
 DAG: Ежедневная загрузка статистики из Яндекс.Директа

Особенности:
- Поддержка нескольких аккаунтов
- Асинхронные отчёты (API v5)
- Автоматическая атрибуция (AUTO)
- Идемпотентная загрузка в PostgreSQL
- Rate limits и retry-логика
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import json
import requests
import time
from psycopg2.extras import execute_values
from psycopg2 import sql
import psycopg2

# ============= КОНФИГУРАЦИЯ =============
API_URL = "https://api.direct.yandex.com/json/v5/"
CONFIG_PATH = os.getenv('CONFIG_PATH', '/opt/airflow/dags/02-yandex-direct-api/config/config.json')

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ============= ЗАГРУЗКА КОНФИГА =============
def load_config(config_path=CONFIG_PATH):
    """Загружает конфигурацию из JSON-файла"""
    with open(config_path, 'r') as f:
        return json.load(f)

def load_db_params():
    """Загружает параметры PostgreSQL из конфига"""
    config = load_config()
    return config['postgres']

# ============= РАБОТА С API =============
def get_headers(account):
    """Формирует заголовки для запроса к API Яндекс.Директа"""
    return {
        "Authorization": f"Bearer {account['access_token']}",
        "Client-Login": account['login'],
        "Accept-Language": "ru",
        "Content-Type": "application/json; charset=utf-8",
        "processingMode": "auto",
        "returnMoneyInMicros": "false",
        "skipReportSummary": "true"
    }

def make_request(account, method, body=None, retries=3, delay=5):
    """Выполняет запрос к API с обработкой ошибок и rate limits"""
    timeout = account.get('timeout', 60)
    is_get = body is None
    
    for attempt in range(retries):
        try:
            if is_get:
                response = requests.get(
                    f"{API_URL}{method}",
                    headers=get_headers(account),
                    timeout=timeout
                )
            else:
                response = requests.post(
                    f"{API_URL}{method}",
                    headers=get_headers(account),
                    json=body,
                    timeout=timeout
                )
            
            if response.status_code in [200, 201]:
                return response
            
            # Обработка rate limits (коды 52, 53)
            try:
                error_data = response.json()
                if 'error' in error_data:
                    error_code = error_data['error'].get('error_code')
                    if error_code in [52, 53]:
                        wait_time = int(error_data['error'].get('error_detail', delay))
                        print(f"Лимит запросов. Ждём {wait_time} сек.")
                        time.sleep(wait_time)
                        continue
            except:
                pass
            
            if response.status_code == 400:
                print(f"Ошибка 400: {response.text}")
            response.raise_for_status()
            
        except requests.exceptions.RequestException as e:
            if attempt == retries - 1:
                raise
            print(f"Ошибка (попытка {attempt+1}/{retries}): {e}")
            time.sleep(delay)
    
    return None

def get_custom_report(account, start_date, end_date, goal_id, max_retries=15, wait_time=30):
    """
    Получает отчёт из Яндекс.Директа с автоматической атрибуцией.
    Поддерживает асинхронный режим (статус 201).
    """
    
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": start_date,
                "DateTo": end_date
            },
            "FieldNames": [
                "Date",
                "CampaignId",
                "CampaignName",
                "TargetingLocationName",
                "Impressions",
                "Clicks",
                "Cost"
            ],
            "ReportName": f"Report_{start_date}_{end_date}",
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }
    
    if goal_id:
        body["params"]["Goals"] = [goal_id]
        body["params"]["AttributionModels"] = ["AUTO"]
        body["params"]["FieldNames"].append("Conversions")
    
    print(f"Аккаунт {account['login']}, даты {start_date} - {end_date}")
    
    try:
        # Шаг 1: Отправляем запрос на генерацию отчёта
        response = make_request(account, "reports", body)
        if not response:
            print("Не удалось отправить запрос")
            return None
        
        # Шаг 2: Асинхронная обработка (статус 201)
        if response.status_code == 201:
            report_id = response.headers.get("ReportId") or response.headers.get("report_id")
            if not report_id:
                print("Не удалось получить ReportId")
                return None
            
            print(f"Отчёт генерируется асинхронно. ReportId: {report_id}")
            
            for attempt in range(max_retries):
                time.sleep(wait_time)
                print(f"Проверка готовности ({attempt+1}/{max_retries})...")
                
                ready_response = make_request(account, f"reports/{report_id}")
                
                if ready_response and ready_response.status_code == 200:
                    response = ready_response
                    print("Отчёт успешно сгенерирован!")
                    break
                elif ready_response and ready_response.status_code == 201:
                    print("Отчёт ещё не готов...")
                elif ready_response and ready_response.status_code == 400:
                    print(f"Ошибка: {ready_response.text}")
                    return None
            
            if response.status_code != 200:
                print(f"Не удалось получить отчёт после {max_retries} попыток")
                return None
        
        # Шаг 3: Парсим TSV-ответ
        if response.status_code != 200:
            print(f"Ошибка: статус {response.status_code}")
            return None
        
        raw_text = response.content.decode('utf-8')
        if not raw_text.strip():
            print("Отчёт пуст")
            return None
        
        lines = raw_text.strip().split('\n')
        if len(lines) < 2:
            print(f"Отчёт содержит менее 2 строк")
            return None
        
        # Первая строка - имя отчёта (пропускаем), вторая - заголовки
        headers = lines[1].split('\t')
        data_lines = lines[2:]
        
        data = [line.split('\t') for line in data_lines]
        df = pd.DataFrame(data, columns=headers)
        
        # Преобразование типов
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        numeric_fields = ['Impressions', 'Clicks', 'Cost']
        
        # Обработка конверсий
        conv_cols = [c for c in df.columns if c.startswith('Conversions_')]
        if conv_cols:
            df.rename(columns={conv_cols[0]: 'Conversions'}, inplace=True)
            numeric_fields.append('Conversions')
        elif 'Conversions' in df.columns:
            numeric_fields.append('Conversions')
        else:
            df['Conversions'] = 0
        
        for field in numeric_fields:
            if field in df.columns:
                df[field] = df[field].replace('--', '0')
                df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0)
            else:
                df[field] = 0
        
        # Добавляем логин аккаунта
        df['ClientLogin'] = account['login']
        
        print(f"Загружено {len(df)} строк для аккаунта {account['login']}")
        return df
    
    except Exception as e:
        print(f"Ошибка при получении отчёта: {e}")
        return None

# ============= РАБОТА С БАЗОЙ ДАННЫХ =============
def save_to_postgresql(df: pd.DataFrame, db_params: dict, batch_size=500):
    """Сохраняет данные в PostgreSQL с идемпотентностью (ON CONFLICT)"""
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        
        if df.empty:
            print("Нет данных для загрузки")
            return 0
        
        total_rows = len(df)
        processed = 0
        
        # Создание таблицы, если не существует
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS yd_campaigns_stats (
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
                )
            """)
            conn.commit()
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i+batch_size]
            data_to_insert = []
            
            for _, row in batch.iterrows():
                date_val = row['Date']
                if pd.isna(date_val):
                    continue
                if isinstance(date_val, pd.Timestamp):
                    date_val = date_val.date()
                    
                data_to_insert.append((
                    str(row['ClientLogin']),
                    date_val,
                    str(row['CampaignName'])[:255] if pd.notna(row['CampaignName']) else None,
                    str(row['CampaignId']),
                    str(row['TargetingLocationName'])[:255] if pd.notna(row['TargetingLocationName']) else None,
                    int(row['Impressions']),
                    int(row['Clicks']),
                    float(row['Cost']),
                    int(row['Conversions']),
                    datetime.now(),
                    datetime.now().date()
                ))
            
            if not data_to_insert:
                continue
            
            insert_sql = sql.SQL("""
                INSERT INTO yd_campaigns_stats AS target (
                    client_login, date, campaign_name, campaign_id, targeting_location_name,
                    impressions, clicks, cost, conversions, created_at, load_date
                ) VALUES %s
                ON CONFLICT (client_login, date, campaign_id, targeting_location_name)
                DO UPDATE SET
                    campaign_name = EXCLUDED.campaign_name,
                    impressions = EXCLUDED.impressions,
                    clicks = EXCLUDED.clicks,
                    cost = EXCLUDED.cost,
                    conversions = EXCLUDED.conversions,
                    load_date = EXCLUDED.load_date
            """)
            
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, data_to_insert)
                conn.commit()
            
            processed += len(data_to_insert)
            print(f"Загружено {processed}/{total_rows} строк")
        
        print(f"✅ Загружено {processed} записей")
        return processed
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Ошибка БД: {e}")
        raise
    finally:
        if conn:
            conn.close()

# ============= ОСНОВНАЯ ФУНКЦИЯ ЗАГРУЗКИ =============
def load_direct_data(**context):
    """Основная функция загрузки: получает данные из API и сохраняет в БД"""
    
    dag_run_conf = context.get('dag_run').conf if context.get('dag_run') else {}
    start_date_str = dag_run_conf.get('start_date')
    end_date_str = dag_run_conf.get('end_date')
    
    if not start_date_str or not end_date_str:
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date_str = yesterday
        end_date_str = yesterday
        print(f"Даты не заданы, загружаем за {start_date_str}")
    
    print(f"Период: {start_date_str} – {end_date_str}")
    
    config = load_config()
    accounts = config['yd']['accounts']
    
    all_dfs = []
    for acc in accounts:
        goal_id = acc.get('goal_id')
        if not goal_id:
            print(f"У аккаунта {acc['login']} нет goal_id, пропускаем")
            continue
        
        df = get_custom_report(acc, start_date_str, end_date_str, goal_id)
        if df is not None and not df.empty:
            all_dfs.append(df)
        else:
            print(f"Для {acc['login']} данных нет")
    
    if not all_dfs:
        print("Нет данных для загрузки")
        return
    
    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"Всего строк для загрузки: {len(combined)}")
    print(f"Суммарно кликов: {combined['Clicks'].sum()}")
    print(f"Суммарно затрат: {combined['Cost'].sum()}")
    
    db_params = load_db_params()
    save_to_postgresql(combined, db_params)

# ============= DAG =============
with DAG(
    'yandex_direct_daily_load',
    default_args=default_args,
    description='Ежедневная выгрузка статистики из Яндекс.Директа (автоатрибуция)',
    schedule_interval='0 5 * * *',  # каждый день в 5:00 утра
    catchup=False,
    max_active_runs=1,
    tags=['yandex', 'direct', 'etl', 'marketing'],
) as dag:
    
    load_task = PythonOperator(
        task_id='load_direct_data',
        python_callable=load_direct_data,
        provide_context=True,
    )
    
    load_task