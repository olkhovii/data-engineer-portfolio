"""
DAG: Загрузка заказов из выгрузок 1С
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
import pandas as pd
import psycopg2

DATA_PATH = '/opt/airflow/dags/cases/01-1c-excel-pipeline/data'
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def check_file_exists(**context):
    execution_date = context['execution_date']
    target_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    file_path = f"{DATA_PATH}/samples/Реализации_{target_date}.xlsx"
    
    if not os.path.exists(file_path):
        print(f"Файл не найден: {file_path}")
        raise AirflowSkipException("Файл не найден")
    
    context['ti'].xcom_push(key='file_path', value=file_path)
    return file_path

def process_and_load(**context):
    file_path = context['ti'].xcom_pull(key='file_path')
    
    print(f"Обработка файла: {file_path}")
    
    # Пробуем найти заголовки автоматически
    df = pd.read_excel(file_path)
    
    # Если в первых строках мусор - ищем строку с нужными колонками
    for i in range(min(10, len(df))):
        row = df.iloc[i].astype(str)
        if 'Клиент' in row.values or 'Заказ клиента.Номер' in row.values:
            df = pd.read_excel(file_path, header=i)
            print(f"Заголовки найдены на строке {i}")
            break
    
    # Переименовываем колонки для простоты
    df.columns = [str(c).strip() for c in df.columns]
    
    print(f"Найдено {len(df)} строк данных")
    
    # Подключение к БД
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="airflow_db",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    
    # Создание таблицы
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS staging_client_orders (
            id SERIAL PRIMARY KEY,
            client_name VARCHAR(255),
            order_number VARCHAR(100),
            order_date DATE,
            amount DECIMAL(15,2),
            load_date DATE,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    
    # Определяем колонки по их названиям
    client_col = next((c for c in df.columns if 'Клиент' in c), None)
    order_col = next((c for c in df.columns if 'Номер' in c and 'Заказ' in c), None)
    date_col = next((c for c in df.columns if 'Дата' in c and 'Заказ' in c), None)
    amount_col = next((c for c in df.columns if 'Сумма' in c), None)
    
    if not client_col or not order_col:
        print(f"Колонки не найдены. Доступные: {list(df.columns)}")
        print("Сохраняем всё как есть для отладки")
        # Сохраняем DataFrame для отладки
        df.to_csv('/opt/airflow/dags/cases/01-1c-excel-pipeline/data/debug.csv', index=False)
        return
    
    rows_loaded = 0
    for _, row in df.iterrows():
        if pd.isna(row.get(order_col)):
            continue
            
        cursor.execute("""
            INSERT INTO staging_client_orders (client_name, order_number, order_date, amount, load_date)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            str(row[client_col])[:255] if pd.notna(row[client_col]) else None,
            str(row[order_col])[:100] if pd.notna(row[order_col]) else None,
            row[date_col] if date_col and pd.notna(row[date_col]) else None,
            float(row[amount_col]) if amount_col and pd.notna(row[amount_col]) else None,
            datetime.now().date()
        ))
        rows_loaded += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Загружено {rows_loaded} записей")

with DAG(
    '1c_orders_etl',
    default_args=default_args,
    description='Загрузка заказов из 1С',
    schedule_interval='0 8 * * *',
    catchup=False,
    tags=['1c', 'orders'],
) as dag:
    
    check_file = PythonOperator(
        task_id='check_file_exists',
        python_callable=check_file_exists,
        provide_context=True,
    )
    
    process_load = PythonOperator(
        task_id='process_and_load',
        python_callable=process_and_load,
        provide_context=True,
    )
    
    check_file >> process_load
