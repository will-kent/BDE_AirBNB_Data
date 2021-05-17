WITH host_financials AS (
	SELECT	f.host_id
		,dl.median_mortgage_repayment * 12 AS annual_mortgage_repayments
		,(SUM(num_of_stays * price) / COUNT(DISTINCT scrape_date_id)) * 12 AS est_annual_revenue
	FROM	dwh.fact_airbnb_listings f
		JOIN dwh.dim_host dh
			ON f.host_id = dh.host_id
		JOIN dwh.dim_local_government_area dl
			ON f.lga_id = dl.lga_id
	WHERE dh.num_of_listing = 1
	GROUP BY f.host_id
		,dl.median_mortgage_repayment
	)

SELECT	SUM(CASE
			WHEN annual_mortgage_repayments - est_annual_revenue >= 0
			THEN 1.0
			ELSE 0
			END) / COUNT(*) AS pct_host_covers_repayments
FROM	host_financials
;