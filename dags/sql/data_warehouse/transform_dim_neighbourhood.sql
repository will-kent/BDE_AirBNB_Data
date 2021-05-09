INSERT INTO dwh.dim_neighbourhood (
    neighbourhood_name
    )
SELECT neighbourhood_cleansed
FROM staging.listings
GROUP BY neighbourhood_cleansed
ON CONFLICT (neighbourhood_name)
DO NOTHING
;