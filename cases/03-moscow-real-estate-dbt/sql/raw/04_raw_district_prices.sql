CREATE TABLE IF NOT EXISTS raw_district_prices (
    year_month Date,
    district String,
    okrug String,
    secondary_price_per_sqm Int32,
    newbuild_price_per_sqm Int32,
    rental_price_per_sqm_monthly Int32,
    secondary_mom_change_pct Float64,
    newbuild_mom_change_pct Float64,
    n_listings_secondary Int32,
    n_listings_newbuild Int32,
    n_listings_rental Int32,
    cbr_key_rate_pct Float64,
    avg_mortgage_rate_pct Float64
) ENGINE = MergeTree()
ORDER BY (year_month, district)