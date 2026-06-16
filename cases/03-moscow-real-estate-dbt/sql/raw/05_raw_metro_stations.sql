CREATE TABLE IF NOT EXISTS raw_metro_stations (
    station_name String,
    line String,
    lat Float64,
    lon Float64,
    year_opened Int32,
    to_center_km Float64
) ENGINE = MergeTree()
ORDER BY station_name