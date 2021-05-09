CREATE TABLE IF NOT EXISTS mart.monthly_neighbourhood_stats (
    month DATE NOT NULL
    ,neighbourhood VARCHAR(255) NOT NULL
    ,active_listings_rate NUMERIC(7,4)
    ,min_price_active_listings NUMERIC(8,2)
    ,max_price_active_listings NUMERIC(8,2)
    ,med_price_active_listings NUMERIC(8,2)
    ,avg_price_active_listings NUMERIC(8,2)
    ,distinct_hosts INT
    ,super_host_rate NUMERIC(7,4)
    ,avg_review_score_rating NUMERIC(7,4)
    ,pct_change_active_listings NUMERIC(7,4)
    ,pct_change_inactive_listings NUMERIC(7,4)
    ,total_num_of_stays INT
    ,est_revenue_per_listing NUMERIC(8,2)
);

CREATE TABLE IF NOT EXISTS mart.monthly_accommodation_type_stats (
    month DATE NOT NULL
    ,max_guests INT NOT NULL
    ,room_type VARCHAR(100) NOT NULL
    ,property_type VARCHAR(255) NOT NULL
    ,active_listings_rate NUMERIC(7,4)
    ,min_price_active_listings NUMERIC(8,2)
    ,max_price_active_listings NUMERIC(8,2)
    ,med_price_active_listings NUMERIC(8,2)
    ,avg_price_active_listings NUMERIC(8,2)
    ,distinct_hosts INT
    ,super_host_rate NUMERIC(7,4)
    ,avg_review_score_rating NUMERIC(7,4)
    ,pct_change_active_listings NUMERIC(7,4)
    ,pct_change_inactive_listings NUMERIC(7,4)
    ,total_num_of_stays INT
    ,est_revenue_per_listing NUMERIC(8,2)
);

CREATE TABLE IF NOT EXISTS mart.monthly_host_neighbourhood_stats (
  month DATE NOT NULL
  ,host_neighbourhood VARCHAR(255) NOT NULL
  ,num_of_hosts INT
  ,est_revenue NUMERIC(8,2)
  ,est_revenue_per_host NUMERIC(8,2)
);