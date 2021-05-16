INSERT INTO dwh.dim_date(date_id, date, month, quarter, year)
OVERRIDING SYSTEM VALUE
SELECT -999, '1900-01-01',1,1,1900
WHERE NOT EXISTS (SELECT 1 FROM dwh.dim_date WHERE date_id = -999);

INSERT INTO dwh.dim_neighbourhood(neighbourhood_id, neighbourhood_name)
OVERRIDING SYSTEM VALUE
SELECT -999, 'Unknown'
WHERE NOT EXISTS (SELECT 1 FROM dwh.dim_neighbourhood WHERE neighbourhood_id = -999)
UNION ALL
SELECT -1, 'Outside Australia'
WHERE NOT EXISTS (SELECT 1 FROM dwh.dim_neighbourhood WHERE neighbourhood_id = -1)
UNION ALL
SELECT -2, 'Australia - outside Sydney'
WHERE NOT EXISTS (SELECT 1 FROM dwh.dim_neighbourhood WHERE neighbourhood_id = -2);


INSERT INTO dwh.dim_accommodation(accommodation_id, max_guests, room_type, property_type)
OVERRIDING SYSTEM VALUE
SELECT -999, -999, 'Unknown', 'Unknown'
WHERE NOT EXISTS (SELECT 1 FROM dwh.dim_accommodation WHERE accommodation_id = -999);

INSERT INTO dwh.dim_host(host_id, source_host_id, host_name, created_date, num_of_listing, is_super_host)
OVERRIDING SYSTEM VALUE
SELECT -999, -999, 'Unknown', '1900-01-01', -999, FALSE
WHERE NOT EXISTS (SELECT 1 FROM dwh.dim_host WHERE host_id = -999);

INSERT INTO dwh.dim_local_government_area(lga_id, lga_reference, lga_name, state_name, median_age,
    median_mortgage_repayment, median_weekly_personal_income, median_weekly_family_income,
    median_weekly_household_income, median_weekly_rent, avg_persons_per_bedroom, avg_household_size)
OVERRIDING SYSTEM VALUE
SELECT -999, 'Unknown', 'Unknown', 'Unknown', -1, -1, -1, -1, -1, -1, -1, -1
WHERE NOT EXISTS (SELECT 1 FROM dwh.dim_local_government_area WHERE lga_id = -999);

-- Also need to populate mapping tables
WITH starting_values AS (
    SELECT *
    FROM (VALUES('Ashfield','Inner West'),('Auburn','Cumberland'),('Bankstown','Canterbury-Bankstown')
        ,('Blacktown','Blacktown'),('Botany Bay','Botany Bay'),('Burwood','Burwood'),('Camden','Camden')
        ,('Campbelltown','Campbelltown'),('Canada Bay','Canada Bay'),('Canterbury','Canterbury-Bankstown')
        ,('City Of Kogarah','Georges River'),('Fairfield','Fairfield'),('Holroyd','Cumberland')
        ,('Hornsby','Hornsby'),('Hunters Hill','Hunters Hill'),('Hurstville','Georges River')
        ,('Ku-Ring-Gai','Ku-ring-gai'),('Lane Cove','Lane Cove'),('Leichhardt','Inner West')
        ,('Liverpool','Liverpool'),('Manly','Northern Beaches'),('Marrickville','Inner West')
        ,('Mosman','Mosman'),('North Sydney','North Sydney'),('Parramatta','Parramatta'),('Penrith','Penrith')
        ,('Pittwater','Northern Beaches'),('Randwick','Randwick'),('Rockdale','Rockdale'),('Ryde','Ryde')
        ,('Strathfield','Strathfield'),('Sutherland Shire','Sutherland Shire'),('Sydney','Sydney')
        ,('The Hills Shire','The Hills Shire'),('Warringah','Northern Beaches'),('Waverley','Waverley')
        ,('Willoughby','Willoughby'),('Woollahra','Woollahra')) AS tbl(neighbourhood, lga)
    )

INSERT INTO dwh.mapping_neighbourhood_to_lga (
    neighbourhood_name
    ,lga_name
    )
SELECT  s.*
FROM    starting_values s
    LEFT JOIN dwh.mapping_neighbourhood_to_lga d
        ON s.neighbourhood = d.neighbourhood_name
WHERE d.neighbourhood_name IS NULL
