SELECT SUM(CASE
			WHEN fal.host_neighbourhood_id = fal.listing_neighbourhood_id
			THEN 1
			ELSE 0
			END) as total_listings_in_host_neighbourhood
	,COUNT(*) as total_listings
	,COUNT(distinct fal.host_id) as total_hosts
	,SUM(CASE
			WHEN fal.host_neighbourhood_id = fal.listing_neighbourhood_id
			THEN 1.0
			ELSE 0.0
			END) / COUNT(*) as pct_listings_in_host_neighbourhood
FROM dwh.fact_airbnb_listings fal
	JOIN dwh.dim_host dh
		ON fal.host_id = dh.host_id
	JOIN dwh.dim_date dd
		ON fal.run_date_id = dd.date_id
WHERE dh.num_of_listing > 1
AND fal.is_available = 1
AND dd.date = '2021-04-01'
;