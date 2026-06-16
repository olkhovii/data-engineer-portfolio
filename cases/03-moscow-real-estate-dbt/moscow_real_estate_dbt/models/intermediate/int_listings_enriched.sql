-- Объединяем все типы объявлений
with secondary_market as (
    select
        listing_id,
        date_posted,
        'secondary' as listing_type,
        district,
        okrug,
        lat,
        lon,
        total_area,
        rooms,
        floor,
        total_floors,
        building_year,
        metro_station,
        metro_line,
        metro_distance_min,
        metro_distance_type,
        to_center_km,
        price_rub,
        price_per_sqm,
        NULL as monthly_rent_rub,
        mortgage_rate_at_listing as mortgage_rate
    from {{ ref('stg_secondary_market') }}
),

new_builds as (
    select
        listing_id,
        date_posted,
        'new_build' as listing_type,
        district,
        okrug,
        lat,
        lon,
        total_area,
        rooms,
        floor,
        total_floors,
        completion_year as building_year,
        metro_station,
        metro_line,
        metro_distance_min,
        NULL as metro_distance_type,
        to_center_km,
        price_rub,
        price_per_sqm,
        NULL as monthly_rent_rub,
        mortgage_rate_at_listing as mortgage_rate
    from {{ ref('stg_new_builds') }}
),

rentals as (
    select
        listing_id,
        date_posted,
        'rental' as listing_type,
        district,
        okrug,
        lat,
        lon,
        total_area,
        rooms,
        floor,
        total_floors,
        building_year,
        metro_station,
        metro_line,
        metro_distance_min,
        NULL as metro_distance_type,
        to_center_km,
        NULL as price_rub,
        NULL as price_per_sqm,
        monthly_rent_rub,
        NULL as mortgage_rate
    from {{ ref('stg_rentals') }}
),

unioned as (
    select * from secondary_market
    union all
    select * from new_builds
    union all
    select * from rentals
),

-- Добавляем близость к метро
with_metro_category as (
    select
        *,
        case
            when metro_distance_min <= 15 and metro_distance_type = 'transport' then 'walk_<15min'
            when metro_distance_min <= 30 and metro_distance_type = 'transport' then 'walk_15-30min'
            when metro_distance_type = 'transport' then 'walk_>30min'
            when metro_distance_type = 'by_car' then 'by_car'
            else 'unknown'
        end as metro_accessibility
    from unioned
),

-- Добавляем макроэкономику (ставка ЦБ на дату объявления)
with_macro as (
    select
        m.*,
        dp.cbr_key_rate_pct,
        dp.avg_mortgage_rate_pct
    from with_metro_category m
    left join {{ ref('stg_district_prices') }} dp
        on m.district = dp.district
        and toYear(m.date_posted) = toYear(dp.year_month)
        and toMonth(m.date_posted) = toMonth(dp.year_month)
),

-- Добавляем ценовые сегменты (только для продажи)
with_segments as (
    select
        *,
        case
            when price_per_sqm is not null 
                 and price_per_sqm <= (select quantile(0.33)(price_per_sqm) from with_macro where price_per_sqm is not null)
                then 'economy'
            when price_per_sqm is not null 
                 and price_per_sqm <= (select quantile(0.66)(price_per_sqm) from with_macro where price_per_sqm is not null)
                then 'business'
            when price_per_sqm is not null
                then 'premium'
            else 'n/a'
        end as price_segment
    from with_macro
)

select * from with_segments