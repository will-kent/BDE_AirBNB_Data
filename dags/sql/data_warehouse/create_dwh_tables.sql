CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY
    ,date DATE UNIQUE
    ,month SMALLINT NOT NULL
    ,quarter SMALLINT NOT NULL
    ,year SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_neighbourhood (
    neighbourhood_id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY
    ,neighbourhood_name VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_accommodation (
    accommodation_id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY
    ,max_guests INT NOT NULL
    ,room_type VARCHAR(100) NOT NULL
    ,property_type VARCHAR(255) NOT NULL,
    UNIQUE (max_guests, room_type, property_type)
);

CREATE TABLE IF NOT EXISTS dwh.dim_local_government_area (
    lga_id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY
    ,lga_reference VARCHAR(20) UNIQUE
    ,lga_name VARCHAR(255) NOT NULL
    ,state_name VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_host (
    host_id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY
    ,source_host_id INT NOT NULL UNIQUE
    ,host_name VARCHAR(100)
    ,created_date DATE
    ,num_of_listing INT
    ,is_super_host BOOLEAN
);

CREATE TABLE IF NOT EXISTS dwh.fact_airbnb_listings (
    run_date_id INT NOT NULL REFERENCES dwh.dim_date(date_id)
    ,scrape_date_id INT NOT NULL REFERENCES dwh.dim_date(date_id)
    ,listing_neighbourhood_id INT NOT NULL REFERENCES dwh.dim_neighbourhood(neighbourhood_id)
    ,accommodation_id INT NOT NULL REFERENCES dwh.dim_accommodation(accommodation_id)
    ,lga_id INT NOT NULL REFERENCES dwh.dim_local_government_area(lga_id)
    ,host_neighbourhood_id INT NOT NULL REFERENCES dwh.dim_neighbourhood(neighbourhood_id)
    ,host_id INT NOT NULL REFERENCES dwh.dim_host(host_id)
    ,is_available SMALLINT NOT NULL
    ,price NUMERIC(8,2)
    ,future_availability_30d INT
    ,num_of_stays INT
    ,total_num_reviews INT
    ,review_scores_rating NUMERIC(5,2)
);

CREATE TABLE IF NOT EXISTS dwh.mapping_neighbourhood_to_lga (
    neighbourhood_name VARCHAR(255) NOT NULL
    ,lga_name VARCHAR(255) NOT NULL
);