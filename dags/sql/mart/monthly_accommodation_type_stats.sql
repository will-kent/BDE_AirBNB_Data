--delete any previous runs for the run date
DELETE FROM mart.monthly_accommodation_type_stats
WHERE month = '{{ ds }}';

--using base in case null months/neighbourhoods required
WITH base AS (
	SELECT date_trunc('month', '{{ ds }}'::DATE) AS month
		,property_type AS property_type
		,room_type AS room_type
		,max_guests AS accommodates
	FROM dwh.dim_accommodation
	)
,all_listings AS (
	SELECT date_trunc('month', '{{ ds }}'::DATE) AS month
		,a.property_type AS property_type
		,a.room_type AS room_type
		,a.max_guests AS accommodates
		,(SUM(f.is_available::NUMERIC(7,4))/COUNT(*)) * 100 AS active_listings_rate
		,COUNT(DISTINCT f.host_id) AS distinct_hosts
		,COUNT(DISTINCT CASE
		        WHEN h.is_super_host THEN h.host_id
			    ELSE NULL
			    END)::NUMERIC(7,4) / COUNT(DISTINCT h.host_id) * 100 AS super_host_rate
	FROM dwh.fact_airbnb_listings f
		JOIN dwh.dim_accommodation a
			ON f.accommodation_id = a.accommodation_id
		JOIN dwh.dim_date d
			ON f.run_date_id = d.date_id
		JOIN dwh.dim_host h
		    ON f.host_id = h.host_id
	WHERE d.date = '{{ ds }}'
	GROUP BY date_trunc('month', '{{ ds }}'::DATE)
		,a.property_type
		,a.room_type
		,a.max_guests
	)
,active_listings AS (
	SELECT date_trunc('month', d.date) AS month
		,a.property_type AS property_type
		,a.room_type AS room_type
		,a.max_guests AS accommodates
		,MIN(price) AS min_price
		,MAX(price) AS max_price
		,percentile_disc(0.5) within group (ORDER BY price) AS median_price
		,AVG(price) AS avg_price
		,AVG(f.review_scores_rating) AS avg_review_scores_rating
		,SUM(num_of_stays) AS total_num_of_stays
		,AVG(price * num_of_stays) AS est_revenue_per_listing
	FROM dwh.fact_airbnb_listings f
		JOIN dwh.dim_accommodation a
			ON f.accommodation_id = a.accommodation_id
		JOIN dwh.dim_date d
			ON f.run_date_id = d.date_id
	WHERE d.date = '{{ ds }}'
	AND f.is_available = 1
	GROUP BY date_trunc('month', d.date)
		,a.property_type
		,a.room_type
		,a.max_guests
	)

-- insert data in mart table
INSERT INTO mart.monthly_accommodation_type_stats (
    month
    ,property_type
	,room_type
	,accommodates
    ,active_listings_rate
    ,min_price_active_listings
    ,max_price_active_listings
    ,med_price_active_listings
    ,avg_price_active_listings
    ,distinct_hosts
    ,super_host_rate
    ,avg_review_score_rating
    ,total_num_of_stays
    ,est_revenue_per_listing
    ,pct_change_active_listings
    ,pct_change_inactive_listings
    )
SELECT b.month
	,b.property_type
	,b.room_type
	,b.accommodates
	,COALESCE(al.active_listings_rate,0) AS active_listings_rate
	,COALESCE(act.min_price,0) AS min_price_active_listings
	,COALESCE(act.max_price,0) AS max_price_active_listings
	,COALESCE(act.median_price,0) AS median_price_active_listings
	,COALESCE(act.avg_price,0) AS avg_price_active_listings
	,COALESCE(al.distinct_hosts,0) AS distinct_hosts
	,COALESCE(al.super_host_rate,0) AS super_host_rate
	,COALESCE(act.avg_review_scores_rating,0) AS avg_review_scores_rating
    ,COALESCE(act.total_num_of_stays,0) AS total_num_of_stays
    ,COALESCE(act.est_revenue_per_listing,0) AS est_revenue_per_listing
    ,0 AS pct_change_active_listings
    ,0 AS pct_change_inactive_listings
FROM base b
	LEFT JOIN all_listings al
		ON b.month = al.month
		AND b.property_type = al.property_type
        AND b.room_type = al.room_type
        AND b.accommodates = al.accommodates
	LEFT JOIN active_listings act
		ON b.month = act.month
		AND b.property_type = act.property_type
        AND b.room_type = act.room_type
        AND b.accommodates = act.accommodates
;

--need to update pct changes in monthly table for current run month and, in case
--this is a reload, any run for a subsequent month which will impact percent calc
WITH history AS (
	select a.property_type AS property_type
		,a.room_type AS room_type
		,a.max_guests AS accommodates
		,d.date AS run_date
		,SUM(is_available)::INT AS active_listings
		,SUM(abs(is_available  - 1))::INT AS inactive_listings
	FROM dwh.fact_airbnb_listings f
		JOIN dwh.dim_date d
			ON f.run_date_id = d.date_id
		JOIN dwh.dim_accommodation a
			ON f.listing_neighbourhood_id = a.accommodation_id
	WHERE d.date BETWEEN '{{ ds }}'::DATE - INTERVAL '1 month' AND '{{ ds }}'::DATE + INTERVAL '1 month'
	GROUP BY a.property_type
		,a.room_type
		,a.max_guests
		,date
	)
,changes AS (
	SELECT property_type
		,room_type
		,accommodates
		,run_date
		,CASE
			WHEN LAG(active_listings, 1, 0) over (PARTITION BY property_type, room_type, accommodates ORDER BY run_date) = 0
			THEN 0
			ELSE (active_listings - LAG(active_listings, 1, 0) over (PARTITION BY property_type, room_type, accommodates ORDER BY run_date))::NUMERIC(7,4)
				/ LAG(active_listings, 1) over (PARTITION BY property_type, room_type, accommodates ORDER BY run_date)
			END AS pct_change_active_listings
		,CASE
			when LAG(inactive_listings, 1, 0) over (PARTITION BY property_type, room_type, accommodates ORDER BY run_date) = 0
			then 0
			else (inactive_listings - LAG(inactive_listings, 1, 0) over (PARTITION BY property_type, room_type, accommodates ORDER BY run_date))::NUMERIC(7,4)
				/ LAG(inactive_listings, 1) over (PARTITION BY property_type, room_type, accommodates ORDER BY run_date)
			END AS pct_change_inactive_listings
	FROM history
	)

UPDATE mart.monthly_accommodation_type_stats m
SET	pct_change_active_listings = c.pct_change_active_listings
    ,pct_change_inactive_listings = c.pct_change_inactive_listings
FROM changes c
WHERE m.property_type = c.property_type
AND m.room_type = c.room_type
AND m.accommodates = c.accommodates
AND m.month = c.run_date
AND c.run_date >= '{{ ds }}'::DATE
;