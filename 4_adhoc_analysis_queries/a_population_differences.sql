WITH best_neighbourhood AS (
    -- Find best performing neighbourhood based on est revenue per active listing
	SELECT	f.listing_neighbourhood_id AS neighbourhood_id
		,f.lga_id
		,dn.neighbourhood_name
		,SUM(f.num_of_stays * f.price) / COUNT(*) AS est_revenue
	FROM	dwh.fact_airbnb_listings f
		JOIN dwh.dim_neighbourhood dn
			on f.listing_neighbourhood_id = dn.neighbourhood_id
	WHERE f.is_available = 1
	AND dn.neighbourhood_name != 'Unknown'
	GROUP BY f.listing_neighbourhood_id
		,f.lga_id
		,dn.neighbourhood_name
	ORDER BY est_revenue DESC
	LIMIT 1
	)
,worst_neighbourhood AS (
    -- Find worst performing neighbourhood based on est revenue per active listing
	SELECT	f.listing_neighbourhood_id AS neighbourhood_id
		,f.lga_id
		,dn.neighbourhood_name
		,SUM(f.num_of_stays * f.price) / COUNT(*) AS est_revenue
	FROM	dwh.fact_airbnb_listings f
		JOIN dwh.dim_neighbourhood dn
			on f.listing_neighbourhood_id = dn.neighbourhood_id
	WHERE f.is_available = 1
	AND dn.neighbourhood_name != 'Unknown'
	GROUP BY f.listing_neighbourhood_id
		,f.lga_id
		,dn.neighbourhood_name
	ORDER BY est_revenue ASC
	LIMIT 1
	)
,neighbourhoods AS (
	SELECT neighbourhood_id
		,lga_id
		,neighbourhood_name
		,'best' AS type
	FROM best_neighbourhood
	UNION ALL
	SELECT neighbourhood_id
		,lga_id
		,neighbourhood_name
		,'worst' AS type
	FROM worst_neighbourhood
	)
,calcs AS (
	SELECT	l.lga_name
		,n.neighbourhood_name
		,n.type
		,l.median_age
		,l.median_mortgage_repayment
		,l.median_weekly_personal_income
		,l.median_weekly_household_income
		,l.median_weekly_rent
		,l.avg_persons_per_bedroom
		,l.avg_household_size
		,fld.total_pop_male / (fld.total_pop * 1.0) AS pct_male
		,fld.total_pop_female / (fld.total_pop * 1.0) AS pct_female
		,fld.total_pop_age_0_4 / (fld.total_pop * 1.0) AS pct_age_0_4
		,fld.total_pop_age_5_14 / (fld.total_pop * 1.0) AS pct_age_5_14
		,fld.total_pop_age_15_19 / (fld.total_pop * 1.0) AS pct_age_15_19
		,fld.total_pop_age_20_24 / (fld.total_pop * 1.0) AS pct_age_20_24
		,fld.total_pop_age_25_34 / (fld.total_pop * 1.0) AS pct_age_25_34
		,fld.total_pop_age_35_44 / (fld.total_pop * 1.0) AS pct_age_35_44
		,fld.total_pop_age_45_54 / (fld.total_pop * 1.0) AS pct_age_45_54
		,fld.total_pop_age_55_64 / (fld.total_pop * 1.0) AS pct_age_55_64
		,fld.total_pop_age_65_74 / (fld.total_pop * 1.0) AS pct_age_65_74
		,fld.total_pop_age_75_84 / (fld.total_pop * 1.0) AS pct_age_75_84
		,fld.total_pop_age_85_over / (fld.total_pop * 1.0) AS pct_age_85_over
		,fld.total_pop_aboriginal / (fld.total_pop * 1.0) AS pct_aboriginal
		,fld.total_pop_torres_strait_islander / (fld.total_pop * 1.0) AS pct_torres_strait_islander
		,fld.total_pop_birthplace_australia / (fld.total_pop * 1.0) AS pct_born_australia
		,fld.total_pop_birthplace_elsewhere / (fld.total_pop * 1.0) AS pct_born_overseas
		,fld.total_pop_home_language_english_only / (fld.total_pop * 1.0) AS pct_english_only_home_language
		,fld.total_pop_home_language_other / (fld.total_pop * 1.0) AS pct_other_home_language
		,fld.total_pop_australia_citizen / (fld.total_pop * 1.0) AS pct_australian_citizen
		,fld.total_pop_age_0_4_in_education / (fld.total_pop_age_0_4 * 1.0) AS pct_0_4_education
		,fld.total_pop_age_5_14_in_education / (fld.total_pop_age_5_14 * 1.0) AS pct_5_14_education
		,fld.total_pop_age_15_19_in_education / (fld.total_pop_age_15_19 * 1.0) AS pct_15_19_education
		,fld.total_pop_age_20_24_in_education / (fld.total_pop_age_20_24 * 1.0) AS pct_20_24_education
		,fld.total_pop_age_25_over_in_education / ((fld.total_pop - fld.total_pop_age_0_4 - fld.total_pop_age_5_14
			- fld.total_pop_age_15_19 - fld.total_pop_age_20_24) * 1.0) AS pct_25_over_education
		,fld.total_pop_completed_year_8_below / (fld.total_pop * 1.0) AS pct_completed_year_8_below
		,fld.total_pop_completed_year_9 / (fld.total_pop * 1.0) AS pct_completed_year_9
		,fld.total_pop_completed_year_10 / (fld.total_pop * 1.0) AS pct_completed_year_10
		,fld.total_pop_completed_year_11 / (fld.total_pop * 1.0) AS pct_completed_year_11
		,fld.total_pop_completed_year_12 / (fld.total_pop * 1.0) AS pct_completed_year_12
		,fld.total_pop_private_dwelling / (fld.total_pop * 1.0) AS pct_private_dwelling
		,fld.total_pop_other_dwelling / (fld.total_pop * 1.0) AS pct_other_dwelling
	FROM	neighbourhoods n
		JOIN dwh.dim_local_government_area l
			ON l.lga_id = n.lga_id
		JOIN dwh.fact_lga_demographics fld
			ON l.lga_id = fld.lga_id
	)
,unpivot AS (
	SELECT lga_name, t.*
	FROM calcs c
		CROSS JOIN LATERAL (
			VALUES
			(c.median_age, 'Median Age'),
			(c.median_mortgage_repayment, 'Median Mortgage Repayment'),
			(c.median_weekly_personal_income, 'Median Weekly Personal Income'),
			(c.median_weekly_household_income, 'Median Weekly Household Income'),
			(c.median_weekly_rent, 'Median Weekly Rent'),
			(c.avg_persons_per_bedroom, 'Persons Per Bedroom'),
			(c.avg_household_size, 'Household Size'),
			(c.pct_male, 'Percent Male'),
			(c.pct_female, 'Percent Female'),
			(c.pct_age_0_4, 'Percent Age 0-4'),
			(c.pct_age_5_14, 'Percent Age 5-14'),
			(c.pct_age_15_19, 'Percent Age 15-19'),
			(c.pct_age_20_24, 'Percent Age 20-24'),
			(c.pct_age_25_34, 'Percent Age 25-34'),
			(c.pct_age_35_44, 'Percent Age 35-44'),
			(c.pct_age_45_54, 'Percent Age 45-54'),
			(c.pct_age_55_64, 'Percent Age 55-64'),
			(c.pct_age_65_74, 'Percent Age 65-74'),
			(c.pct_age_75_84, 'Percent Age 75-84'),
			(c.pct_age_85_over, 'Percent Age Over 85'),
			(c.pct_aboriginal, 'Percent Aboriginal'),
			(c.pct_torres_strait_islander, 'Percent Torres Strait Islander'),
			(c.pct_born_australia, 'Percent Born in Australia'),
			(c.pct_born_overseas, 'Percent Born Overseas'),
			(c.pct_english_only_home_language, 'Percent English Only at Home'),
			(c.pct_other_home_language, 'Percent Other Home Language'),
			(c.pct_australian_citizen, 'Percent Austrlia Citizens'),
			(c.pct_0_4_education, 'Percent 0-4 in Education'),
			(c.pct_5_14_education, 'Percent 5-14 in Education'),
			(c.pct_15_19_education, 'Percent 15-19 in Education'),
			(c.pct_20_24_education, 'Percent 20-24 in Education'),
			(c.pct_25_over_education, 'Percent Over 25 in Education'),
			(c.pct_completed_year_8_below, 'Percent Completed Year 8 or Below'),
			(c.pct_completed_year_9, 'Percent Completed Year 9'),
			(c.pct_completed_year_10, 'Percent Completed Year 10'),
			(c.pct_completed_year_11, 'Percent Completed Year 11'),
			(c.pct_completed_year_12, 'Percent Completed Year 12'),
			(c.pct_private_dwelling, 'Percent Private Dwelling'),
			(c.pct_other_dwelling, 'Percent Other Dwelling')
		) AS t(value, metric)
	)

SELECT	p1.metric
	,p1.value AS worst_lga
	,p2.value AS best_lga
	,ABS((p2.value - p1.value) / p1.value) AS difference
FROM 	unpivot p1
	JOIN unpivot p2
		ON p1.metric = p2.metric
WHERE p1.lga_name = (SELECT DISTINCT lga_name FROM calcs WHERE type = 'worst')
AND p2.lga_name = (SELECT DISTINCT lga_name FROM calcs WHERE type = 'best')
ORDER BY difference DESC