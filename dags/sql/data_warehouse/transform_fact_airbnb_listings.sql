DELETE FROM dwh.fact_airbnb_listings f
USING dwh.dim_date d
WHERE f.run_date_id = d.date_id
AND d.date = '{{ ds }}'
;

WITH base_listings AS (
    SELECT  CASE
            WHEN (last_scraped SIMILAR TO '[0-9]+/[0-9]+/[0-9]+')
            THEN TO_DATE(last_scraped, 'dd/mm/yy')
            WHEN (last_scraped SIMILAR TO '[0-9]+\-[0-9]+\-[0-9]+')
            THEN TO_DATE(last_scraped, 'yyyy-mm-dd')
            ELSE '1900-01-01'::DATE
            END AS last_scraped
        ,host_id
        ,neighbourhood_cleansed
        ,accommodates
		,room_type
		,property_type
		,host_neighbourhood
		,has_availability
		,CASE
			WHEN (price SIMILAR TO '\$\d+(,\d+)*.\d+')
			THEN TO_NUMBER(price, 'L999G999.99')::NUMERIC(8,2)
			ELSE price::NUMERIC(8,2)
			END AS price
		,availability_30
		,number_of_reviews
        ,review_scores_rating
    FROM    staging.listings l
)

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
    ,COALESCE(hn.neighbourhood_id, -999) AS host_neighbourhood_id
    ,COALESCE(h.host_id, -999) AS host_id
    ,CASE
    	WHEN  has_availability = 't' THEN 1
    	ELSE 0
    	END AS is_available
    ,price AS price
    ,availability_30::INT AS future_availability_30d
    ,CASE
        WHEN has_availability = 't'
        THEN 30 - availability_30::INT
        ELSE 0
        END AS num_of_stays
    ,number_of_reviews::INT AS total_num_reviews
    ,NULLIF(review_scores_rating,'NaN')::NUMERIC(5,2) AS review_scores_rating
from base_listings l
    LEFT JOIN dwh.dim_date rd
        ON '{{ ds }}' = rd.date
    LEFT JOIN dwh.dim_date sd
        ON last_scraped = sd.date
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
    LEFT JOIN staging.host_neighbourhood_mapping hnm
        ON l.host_neighbourhood = hnm.host_neighbourhood
    LEFT JOIN dwh.dim_neighbourhood hn
        ON hnm.neighbourhood_cleansed = hn.neighbourhood_name
;