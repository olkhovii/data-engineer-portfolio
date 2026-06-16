select 
    listing_id,
    listing_type,
    district,
    total_area,
    rooms,
    floor,
    total_floors,
    building_year,
    metro_distance_min,
    to_center_km,
    price_per_sqm,
    cbr_key_rate_pct,
    metro_accessibility,
    price_segment,
    -- Добавим производные признаки для ML
    case 
        when floor = total_floors then 'penthouse'
        when floor = 1 then 'first_floor'
        else 'standard'
    end as floor_type,
    total_area / rooms as area_per_room
from {{ ref('int_listings_enriched') }}
where price_per_sqm is not null
