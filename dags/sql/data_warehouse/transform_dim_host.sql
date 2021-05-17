INSERT INTO dwh.dim_host (
    source_host_id
    ,host_name
    ,created_date
    ,num_of_listing
    ,is_super_host
    )
SELECT host_id::INT AS source_host_id
    ,MIN(COALESCE(NULLIF(host_name,'NaN'),'Unknown')) AS host_name
    ,MIN(CASE
    	WHEN (host_since SIMILAR TO '[0-9]+/[0-9]+/[0-9]+')
		THEN TO_DATE(host_since, 'dd/mm/yy')
		WHEN (host_since SIMILAR TO '[0-9]+\-[0-9]+\-[0-9]+')
		THEN TO_DATE(host_since, 'yyyy-mm-dd')
    	ELSE '1900-01-01'
    	END) AS created_date
    ,MAX(to_number(COALESCE(NULLIF(host_listings_count,'NaN'),'0'),'999999D99')::INT) AS num_of_listing
    ,CASE
        WHEN MAX(host_is_superhost) = 't' THEN TRUE
        ELSE FALSE
        END AS is_super_host
FROM staging.listings
GROUP BY host_id
ON CONFLICT (source_host_id)
DO UPDATE SET
host_name = EXCLUDED.host_name
,created_date = EXCLUDED.created_date
,num_of_listing = EXCLUDED.num_of_listing
,is_super_host = EXCLUDED.is_super_host
;