select
    year_month,
    district,
    okrug,
    secondary_price_per_sqm,
    newbuild_price_per_sqm,
    rental_price_per_sqm_monthly,
    secondary_mom_change_pct,
    newbuild_mom_change_pct,
    n_listings_secondary,
    n_listings_newbuild,
    n_listings_rental,
    cbr_key_rate_pct,
    avg_mortgage_rate_pct,
    now() as _loaded_at
from {{ source('raw', 'raw_district_prices') }}
