INSERT INTO staging.host_neighbourhood_mapping (
	host_neighbourhood
	,neighbourhood_cleansed
	)
SELECT DISTINCT host_neighbourhood, 'Unknown'
FROM staging.listings l
WHERE NOT EXISTS(SELECT 1 FROM staging.host_neighbourhood_mapping hnm
					WHERE l.host_neighbourhood = host_neighbourhood)
;