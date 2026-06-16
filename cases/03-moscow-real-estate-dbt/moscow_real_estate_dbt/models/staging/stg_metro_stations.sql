select
    station_name,
    line,
    lat,
    lon,
    year_opened,
    to_center_km,
    now() as _loaded_at
from {{ source('raw', 'raw_metro_stations') }}
