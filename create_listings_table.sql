CREATE TABLE staging.listings (
id VARCHAR(50) NOT NULL
,listing_url VARCHAR(255) NOT NULL
,scrape_id VARCHAR(50) NOT NULL
,last_scraped VARCHAR(255) NOT NULL
,name VARCHAR(500)
,host_id VARCHAR(50)
,host_name VARCHAR(100)
,host_since VARCHAR(50)
,host_neighbourhood VARCHAR(255)
,host_listings_count VARCHAR(10)
,host_total_listings_count VARCHAR(10)
,neighbourhood_cleansed VARCHAR(255)
,property_type VARCHAR(255)
,room_type VARCHAR(100)
,accommodates VARCHAR(10)
,price VARCHAR(100)
,has_availability VARCHAR(10)
,availability_30 VARCHAR(10)
,review_scores_rating VARCHAR(10)
,reviews_per_month VARCHAR(10)
)
;
