WITH lga AS(
    SELECT lga_code_2016
        ,TRIM(SPLIT_PART(lga_name_2016,'(',1)) as lga_name_2016
        ,state_name_2016
    FROM staging.abs_lga_2016_nsw
    WHERE lga_name_2016 in ('Inner West (A)','Cumberland (A)','Canterbury-Bankstown (A)'
    ,'Georges River (A)','Northern Beaches (A)','Blacktown (C)','Botany Bay (C)','Burwood (A)'
    ,'Camden (A)','Campbelltown (C) (NSW)','Canada Bay (A)','Fairfield (C)','Hornsby (A)'
    ,'Hunters Hill (A)','Ku-ring-gai (A)','Lane Cove (A)','Liverpool (C)','Mosman (A)'
    ,'North Sydney (A)','Parramatta (C)','Penrith (C)','Randwick (C)','Rockdale (C)'
    ,'Ryde (C)','Strathfield (A)','Sutherland Shire (A)','Sydney (C)','The Hills Shire (A)'
    ,'Waverley (A)','Willoughby (C)','Woollahra (A)')
    GROUP BY lga_code_2016
        ,lga_name_2016
        ,state_name_2016
    )
,lga_census_g02 AS (
    SELECT replace(lga_code_2016,'LGA','') AS lga_code_2016
    	,median_age_persons::INT AS median_age_persons
    	,median_mortgage_repay_monthly::NUMERIC AS median_mortgage_repay_monthly
    	,median_tot_prsnl_inc_weekly::NUMERIC AS median_tot_prsnl_inc_weekly
    	,median_rent_weekly::NUMERIC AS median_rent_weekly
    	,median_tot_fam_inc_weekly::NUMERIC AS median_tot_fam_inc_weekly
    	,average_num_psns_per_bedroom::NUMERIC AS average_num_psns_per_bedroom
    	,median_tot_hhd_inc_weekly::NUMERIC AS median_tot_hhd_inc_weekly
    	,average_household_size::NUMERIC AS average_household_size
    FROM staging.abs_2016_census_g02_nsw_lga
    )
INSERT INTO dwh.dim_local_government_area (
    lga_reference
    ,lga_name
    ,state_name
    ,median_age
    ,median_mortgage_repayment
    ,median_weekly_personal_income
    ,median_weekly_family_income
    ,median_weekly_household_income
    ,median_weekly_rent
    ,avg_persons_per_bedroom
    ,avg_household_size
	)
SELECT l.lga_code_2016
	,l.lga_name_2016
	,l.state_name_2016
	,g2.median_age_persons
	,g2.median_mortgage_repay_monthly
	,g2.median_tot_prsnl_inc_weekly
	,g2.median_rent_weekly
	,g2.median_tot_fam_inc_weekly
	,g2.average_num_psns_per_bedroom
	,g2.median_tot_hhd_inc_weekly
	,g2.average_household_size
FROM lga l
	JOIN lga_census_g02 g2
		ON l.lga_code_2016 = g2.lga_code_2016
ON CONFLICT (lga_reference)
DO UPDATE SET
lga_name = EXCLUDED.lga_name
,state_name = EXCLUDED.state_name
,median_age = EXCLUDED.median_age
,median_mortgage_repayment = EXCLUDED.median_mortgage_repayment
,median_weekly_personal_income = EXCLUDED.median_weekly_personal_income
,median_weekly_family_income = EXCLUDED.median_weekly_family_income
,median_weekly_household_income = EXCLUDED.median_weekly_household_income
,median_weekly_rent = EXCLUDED.median_weekly_rent
,avg_persons_per_bedroom = EXCLUDED.avg_persons_per_bedroom
,avg_household_size = EXCLUDED.avg_household_size
;