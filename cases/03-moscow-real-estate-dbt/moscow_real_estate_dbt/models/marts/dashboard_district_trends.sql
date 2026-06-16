select 
    district,
    toStartOfMonth(date_posted) as month,
    listing_type,
    count(*) as listings_count,
    avg(price_per_sqm) as avg_price_per_sqm,
    median(price_per_sqm) as median_price_per_sqm,
    avg(cbr_key_rate_pct) as avg_cbr_rate,
    count(distinct metro_accessibility) as metro_accessibility_types
from {{ ref('int_listings_enriched') }}
where price_per_sqm is not null
group by district, month, listing_type
order by district, month, listing_type
