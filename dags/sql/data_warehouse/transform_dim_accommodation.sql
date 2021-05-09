INSERT INTO dwh.dim_accommodation (
    max_guests
    ,room_type
    ,property_type
)
SELECT accommodates::int AS max_guests
	,room_type
	,property_type
FROM staging.listings
GROUP BY accommodates
	,room_type
	,property_type
ON CONFLICT(max_guests, room_type, property_type)
DO NOTHING
;