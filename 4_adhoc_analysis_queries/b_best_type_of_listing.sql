WITH top_5_neighbourhoods AS (
    -- Find top 5 neighbourhoods based on est revenue per active listing
	SELECT	f.listing_neighbourhood_id AS neighbourhood_id
		,SUM(f.num_of_stays * f.price) / COUNT(*) AS est_revenue
	FROM	dwh.fact_airbnb_listings f
	WHERE f.is_available = 1
	GROUP BY f.listing_neighbourhood_id
	ORDER BY est_revenue DESC
	LIMIT 5
	)

-- For top 5 neighbourhoods find accommodation type with the highest number of stay (as an average
-- to determine popularity of accommodation type)
SELECT	a.property_type
	,a.room_type
	,a.max_guests AS accommodates
	,AVG(num_of_stays) AS avg_num_of_stays
	,SUM(num_of_stays) AS total_stays
FROM	top_5_neighbourhoods n
	JOIN dwh.fact_airbnb_listings f
		ON n.neighbourhood_id = f.listing_neighbourhood_id
	JOIN dwh.dim_accommodation a
		ON f.accommodation_id = a.accommodation_id
WHERE f.is_available = 1
GROUP BY a.property_type
	,a.room_type
	,a.max_guests
ORDER BY avg_num_of_stays DESC, total_stays DESC
;