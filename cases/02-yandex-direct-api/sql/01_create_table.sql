-- Таблица для статистики кампаний из Яндекс.Директа
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
    
    -- Уникальный ключ для идемпотентности
    UNIQUE(client_login, date, campaign_id, targeting_location_name)
);

-- Индексы для ускорения
CREATE INDEX IF NOT EXISTS idx_yd_stats_date ON yd_campaigns_stats(date);
CREATE INDEX IF NOT EXISTS idx_yd_stats_login ON yd_campaigns_stats(client_login);