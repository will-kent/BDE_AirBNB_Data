--delete any previous runs for the run date
DELETE FROM mart.monthly_host_neighbourhood_stats
WHERE month = '{{ ds }}';

--using base in case null months/neighbourhoods required
WITH base AS (
	SELECT date_trunc('month', '{{ ds }}'::DATE) AS month
		,neighbourhood_name AS neighbourhood_name
	FROM dwh.dim_neighbourhood
	)
,hosts AS (
    SELECT date_trunc('month', '{{ ds }}'::DATE) AS month
        ,hn.neighbourhood_name AS host_neighbourhood
        ,COUNT(DISTINCT fal.host_id) as num_of_hosts
        ,ROUND(SUM(fal.num_of_stays * fal.price),2) as est_revenue
        ,ROUND(SUM(fal.num_of_stays * fal.price) / COUNT(DISTINCT fal.host_id),2) AS est_revenue_per_host
    FROM dwh.fact_airbnb_listings fal
        JOIN dwh.dim_neighbourhood hn
            ON fal.host_neighbourhood_id = hn.neighbourhood_id
    GROUP BY hn.neighbourhood_name
    )

INSERT INTO mart.monthly_host_neighbourhood_stats (
  month
  ,host_neighbourhood
  ,num_of_hosts
  ,est_revenue
  ,est_revenue_per_host
  )
SELECT  b.month
    ,b.neighbourhood_name
    ,COALESCE(h.num_of_hosts,0)
    ,COALESCE(h.est_revenue,0)
    ,COALESCE(h.est_revenue_per_host,0)
FROM    base b
    LEFT JOIN hosts h
        ON b.neighbourhood_name = h.host_neighbourhood
        AND b.month = h.month
;