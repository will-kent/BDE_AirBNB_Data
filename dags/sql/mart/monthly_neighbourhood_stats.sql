--delete any previous runs for the run date
DELETE FROM mart.monthly_neighbourhood_stats
WHERE month = '{{ ds }}';

--using base in case null months/neighbourhoods required
WITH base AS (
	SELECT date_trunc('month', '{{ ds }}'::DATE) AS month
		,neighbourhood_name AS neighbourhood_name
	FROM dwh.dim_neighbourhood
	)
,all_listings AS (
	SELECT date_trunc('month', '{{ ds }}'::DATE) AS month
		,n.neighbourhood_name AS neighbourhood_name
		,(SUM(f.is_available::NUMERIC(7,4))/COUNT(*)) * 100 AS active_listings_rate
		,COUNT(DISTINCT f.host_id) AS distinct_hosts
		,COUNT(DISTINCT CASE
		        WHEN h.is_super_host THEN h.host_id
			    ELSE NULL
			    END)::NUMERIC(7,4) / COUNT(DISTINCT h.host_id) * 100 AS super_host_rate
	FROM dwh.fact_airbnb_listings f
		JOIN dwh.dim_neighbourhood n
			ON f.listing_neighbourhood_id = n.neighbourhood_id
		JOIN dwh.dim_date d
			ON f.run_date_id = d.date_id
		JOIN dwh.dim_host h
		    ON f.host_id = h.host_id
	WHERE d.date = '{{ ds }}'
	GROUP BY date_trunc('month', '{{ ds }}'::DATE)
		,n.neighbourhood_name
	)
,active_listings AS (
	SELECT date_trunc('month', d.date) AS month
		,n.neighbourhood_name AS neighbourhood_name
		,MIN(price) AS min_price
		,MAX(price) AS max_price
		,percentile_disc(0.5) within group (ORDER BY price) AS median_price
		,AVG(price) AS avg_price
		,AVG(f.review_scores_rating) AS avg_review_scores_rating
		,SUM(num_of_stays) AS total_num_of_stays
		,AVG(price * num_of_stays) AS est_revenue_per_listing
	FROM dwh.fact_airbnb_listings f
		JOIN dwh.dim_neighbourhood n
			ON f.listing_neighbourhood_id = n.neighbourhood_id
		JOIN dwh.dim_date d
			ON f.run_date_id = d.date_id
	WHERE d.date = '{{ ds }}'
	AND f.is_available = 1
	GROUP BY date_trunc('month', d.date)
		,n.neighbourhood_name
	)

INSERT INTO mart.monthly_neighbourhood_stats (
    month
    ,neighbourhood
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
	,b.neighbourhood_name
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
		AND b.neighbourhood_name = al.neighbourhood_name
	LEFT JOIN active_listings act
		ON b.month = act.month
		AND b.neighbourhood_name = act.neighbourhood_name
;

--need to update pct changes in monthly table for current run month and, in case
--this is a reload, any run for a subsequent month which will impact percent calc
WITH history AS (
	select n.neighbourhood_name 
		,date
		,SUM(is_available)::INT AS active_listings
		,SUM(abs(is_available  - 1))::INT AS inactive_listings
	FROM dwh.fact_airbnb_listings f
		JOIN dwh.dim_date d
			ON f.run_date_id = d.date_id
		JOIN dwh.dim_neighbourhood n
			ON f.listing_neighbourhood_id = n.neighbourhood_id
	WHERE d.date BETWEEN '2021-04-01'::DATE - INTERVAL '1 month' AND '2021-04-01'::DATE + INTERVAL '1 month'
	GROUP BY n.neighbourhood_name
		,date
	)
,changes AS (
	SELECT neighbourhood_name
		,date
		,CASE
			WHEN LAG(active_listings, 1, 0) over (PARTITION BY neighbourhood_name ORDER BY date) = 0
			THEN 0
			ELSE (active_listings - LAG(active_listings, 1, 0) over (PARTITION BY neighbourhood_name ORDER BY date))::NUMERIC(7,4)
				/ LAG(active_listings, 1) over (PARTITION BY neighbourhood_name ORDER BY date)
			END AS pct_change_active_listings
		,CASE
			when LAG(inactive_listings, 1, 0) over (PARTITION BY neighbourhood_name ORDER BY date) = 0
			then 0
			else (inactive_listings - LAG(inactive_listings, 1, 0) over (PARTITION BY neighbourhood_name ORDER BY date))::NUMERIC(7,4)
				/ LAG(inactive_listings, 1) over (PARTITION BY neighbourhood_name ORDER BY date)
			END AS pct_change_inactive_listings
	FROM history
	)

UPDATE mart.monthly_neighbourhood_stats m
SET	pct_change_active_listings = c.pct_change_active_listings
    ,pct_change_inactive_listings = c.pct_change_inactive_listings
FROM changes c
WHERE m.neighbourhood = c.neighbourhood_name
AND m.month = c.date
AND c.date >= '{{ ds }}'::DATE
;