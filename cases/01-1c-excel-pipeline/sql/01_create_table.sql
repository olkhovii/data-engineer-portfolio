-- Создание таблицы для хранения заказов из 1С
CREATE TABLE IF NOT EXISTS staging_client_orders (
    id SERIAL PRIMARY KEY,
    client_name VARCHAR(255),
    order_number VARCHAR(100),
    order_date DATE,
    amount DECIMAL(15,2),
    load_date DATE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Индекс для быстрого поиска по номеру заказа
CREATE INDEX IF NOT EXISTS idx_order_number ON staging_client_orders(order_number);

-- Комментарий к таблице
COMMENT ON TABLE staging_client_orders IS 'Заказы клиентов из выгрузок 1С';
