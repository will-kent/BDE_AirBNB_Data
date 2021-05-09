DELETE FROM dwh.fact_airbnb_listings f
USING dwh.dim_date d
WHERE f.run_date_id = d.date_id
AND d.date = '{{ ds }}'
;

INSERT INTO dwh.fact_airbnb_listings (
    run_date_id
    ,scrape_date_id
    ,listing_neighbourhood_id
    ,accommodation_id
    ,lga_id
    ,host_neighbourhood_id
    ,host_id
    ,is_available
    ,price
    ,future_availability_30d
    ,num_of_stays
    ,total_num_reviews
    ,review_scores_rating
    )
SELECT COALESCE(rd.date_id, -999) AS run_date_id
    ,COALESCE(sd.date_id, -999) AS scrape_date_id
    ,COALESCE(n.neighbourhood_id,-999) AS listing_neighbourhood_id
    ,COALESCE(a.accommodation_id, -999) AS accommodation_id
    ,COALESCE(lga.lga_id, -999) AS lga_id
    ,-999 AS host_neighbourhood_id
    ,COALESCE(h.host_id, -999) AS host_id
    ,CASE
    	WHEN  has_availability = 't' THEN 1
    	ELSE 0
    	END AS is_available
    ,TO_NUMBER(price, '$999G999.99')::NUMERIC(8,2) AS price
    ,availability_30::INT AS future_availability_30d
    ,30 - availability_30::INT AS num_of_stays
    ,number_of_reviews::INT AS total_num_reviews
    ,NULLIF(review_scores_rating,'NaN')::NUMERIC(5,2) AS review_scores_rating
from staging.listings l
    LEFT JOIN dwh.dim_date rd
        ON '{{ ds }}' = rd.date
    LEFT JOIN dwh.dim_date sd
        ON COALESCE(NULLIF(l.last_scraped,'NaN'),'1900-01-01')::DATE = sd.date
	LEFT JOIN dwh.dim_host h
		ON l.host_id = CAST(h.source_host_id AS VARCHAR(50))
	LEFT JOIN dwh.dim_neighbourhood n
		ON l.neighbourhood_cleansed = n.neighbourhood_name
	LEFT JOIN dwh.dim_accommodation a
		ON l.accommodates = CAST(a.max_guests AS VARCHAR(10))
		AND l.room_type = a.room_type
		AND l.property_type = a.property_type
    LEFT JOIN dwh.mapping_neighbourhood_to_lga m
        ON l.neighbourhood_cleansed = m.neighbourhood_name
    LEFT JOIN dwh.dim_local_government_area lga
        ON m.lga_name = lga.lga_name
;