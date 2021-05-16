WITH census AS (
	SELECT replace(lga_code_2016,'LGA','') AS lga_code
		,tot_p_p::INT as tot_p_p
		,tot_p_m::INT as tot_p_m
		,tot_p_f::INT  AS  tot_p_f
		,age_0_4_yr_p::INT  AS  age_0_4_yr_p
		,age_5_14_yr_p::INT  AS  age_5_14_yr_p
		,age_15_19_yr_p::INT  AS  age_15_19_yr_p
		,age_20_24_yr_p::INT  AS  age_20_24_yr_p
		,age_25_34_yr_p::INT  AS  age_25_34_yr_p
		,age_35_44_yr_p::INT  AS  age_35_44_yr_p
		,age_45_54_yr_p::INT  AS  age_45_54_yr_p
		,age_55_64_yr_p::INT  AS  age_55_64_yr_p
		,age_65_74_yr_p::INT  AS  age_65_74_yr_p
		,age_75_84_yr_p::INT  AS  age_75_84_yr_p
		,age_85ov_p::INT  AS  age_85ov_p
		,indigenous_p_tot_p::INT  AS  indigenous_p_tot_p
		,indig_psns_torres_strait_is_p::INT  AS  indig_psns_torres_strait_is_p
		,birthplace_australia_p::INT  AS  birthplace_australia_p
		,birthplace_elsewhere_p::INT  AS  birthplace_elsewhere_p
		,lang_spoken_home_eng_only_p::INT  AS  lang_spoken_home_eng_only_p
		,lang_spoken_home_oth_lang_p::INT  AS  lang_spoken_home_oth_lang_p
		,australian_citizen_p::INT  AS  australian_citizen_p
		,age_psns_att_educ_inst_0_4_p::INT  AS  age_psns_att_educ_inst_0_4_p
		,age_psns_att_educ_inst_5_14_p::INT  AS  age_psns_att_educ_inst_5_14_p
		,age_psns_att_edu_inst_15_19_p::INT  AS  age_psns_att_edu_inst_15_19_p
		,age_psns_att_edu_inst_20_24_p::INT  AS  age_psns_att_edu_inst_20_24_p
		,age_psns_att_edu_inst_25_ov_p::INT  AS  age_psns_att_edu_inst_25_ov_p
		,high_yr_schl_comp_yr_12_eq_p::INT  AS  high_yr_schl_comp_yr_12_eq_p
		,high_yr_schl_comp_yr_11_eq_p::INT  AS  high_yr_schl_comp_yr_11_eq_p
		,high_yr_schl_comp_yr_10_eq_p::INT  AS  high_yr_schl_comp_yr_10_eq_p
		,high_yr_schl_comp_yr_9_eq_p::INT  AS  high_yr_schl_comp_yr_9_eq_p
		,high_yr_schl_comp_yr_8_belw_p::INT  AS  high_yr_schl_comp_yr_8_belw_p
		,count_psns_occ_priv_dwgs_p::INT  AS  count_psns_occ_priv_dwgs_p
		,count_persons_other_dwgs_p::INT  AS  count_persons_other_dwgs_p
	FROM staging.abs_2016_census_g01_nsw_lga
	)

INSERT INTO dwh.fact_lga_demographics (
	lga_id
    ,total_pop
    ,total_pop_male
    ,total_pop_female
    ,total_pop_age_0_4
    ,total_pop_age_5_14
    ,total_pop_age_15_19
    ,total_pop_age_20_24
    ,total_pop_age_25_34
    ,total_pop_age_35_44
    ,total_pop_age_45_54
    ,total_pop_age_55_64
    ,total_pop_age_65_74
    ,total_pop_age_75_84
    ,total_pop_age_85_over
    ,total_pop_aboriginal
    ,total_pop_torres_strait_islander
    ,total_pop_birthplace_australia
    ,total_pop_birthplace_elsewhere
    ,total_pop_home_language_english_only
    ,total_pop_home_language_other
    ,total_pop_australia_citizen
    ,total_pop_age_0_4_in_education
    ,total_pop_age_5_14_in_education
    ,total_pop_age_15_19_in_education
    ,total_pop_age_20_24_in_education
    ,total_pop_age_25_over_in_education
    ,total_pop_completed_year_12
    ,total_pop_completed_year_11
    ,total_pop_completed_year_10
    ,total_pop_completed_year_9
    ,total_pop_completed_year_8_below
    ,total_pop_private_dwelling
    ,total_pop_other_dwelling
    )
SELECT d.lga_id
	,tot_p_p
	,tot_p_m
	,tot_p_f
	,age_0_4_yr_p
	,age_5_14_yr_p
	,age_15_19_yr_p
	,age_20_24_yr_p
	,age_25_34_yr_p
	,age_35_44_yr_p
	,age_45_54_yr_p
	,age_55_64_yr_p
	,age_65_74_yr_p
	,age_75_84_yr_p
	,age_85ov_p
	,indigenous_p_tot_p
	,indig_psns_torres_strait_is_p
	,birthplace_australia_p
	,birthplace_elsewhere_p
	,lang_spoken_home_eng_only_p
	,lang_spoken_home_oth_lang_p
	,australian_citizen_p
	,age_psns_att_educ_inst_0_4_p
	,age_psns_att_educ_inst_5_14_p
	,age_psns_att_edu_inst_15_19_p
	,age_psns_att_edu_inst_20_24_p
	,age_psns_att_edu_inst_25_ov_p
	,high_yr_schl_comp_yr_12_eq_p
	,high_yr_schl_comp_yr_11_eq_p
	,high_yr_schl_comp_yr_10_eq_p
	,high_yr_schl_comp_yr_9_eq_p
	,high_yr_schl_comp_yr_8_belw_p
	,count_psns_occ_priv_dwgs_p
	,count_persons_other_dwgs_p
FROM dwh.dim_local_government_area d
	JOIN census c
		ON d.lga_reference = c.lga_code
ON CONFLICT (lga_id)
DO UPDATE SET
total_pop = EXCLUDED.total_pop
,total_pop_male = EXCLUDED.total_pop_male
,total_pop_female = EXCLUDED.total_pop_female
,total_pop_age_0_4 = EXCLUDED.total_pop_age_0_4
,total_pop_age_5_14 = EXCLUDED.total_pop_age_5_14
,total_pop_age_15_19 = EXCLUDED.total_pop_age_15_19
,total_pop_age_20_24 = EXCLUDED.total_pop_age_20_24
,total_pop_age_25_34 = EXCLUDED.total_pop_age_25_34
,total_pop_age_35_44 = EXCLUDED.total_pop_age_35_44
,total_pop_age_45_54 = EXCLUDED.total_pop_age_45_54
,total_pop_age_55_64 = EXCLUDED.total_pop_age_55_64
,total_pop_age_65_74 = EXCLUDED.total_pop_age_65_74
,total_pop_age_75_84 = EXCLUDED.total_pop_age_75_84
,total_pop_age_85_over = EXCLUDED.total_pop_age_85_over
,total_pop_aboriginal = EXCLUDED.total_pop_aboriginal
,total_pop_torres_strait_islander = EXCLUDED.total_pop_torres_strait_islander
,total_pop_birthplace_australia = EXCLUDED.total_pop_birthplace_australia
,total_pop_birthplace_elsewhere = EXCLUDED.total_pop_birthplace_elsewhere
,total_pop_home_language_english_only = EXCLUDED.total_pop_home_language_english_only
,total_pop_home_language_other = EXCLUDED.total_pop_home_language_other
,total_pop_australia_citizen = EXCLUDED.total_pop_australia_citizen
,total_pop_age_0_4_in_education = EXCLUDED.total_pop_age_0_4_in_education
,total_pop_age_5_14_in_education = EXCLUDED.total_pop_age_5_14_in_education
,total_pop_age_15_19_in_education = EXCLUDED.total_pop_age_15_19_in_education
,total_pop_age_20_24_in_education = EXCLUDED.total_pop_age_20_24_in_education
,total_pop_age_25_over_in_education = EXCLUDED.total_pop_age_25_over_in_education
,total_pop_completed_year_12 = EXCLUDED.total_pop_completed_year_12
,total_pop_completed_year_11 = EXCLUDED.total_pop_completed_year_11
,total_pop_completed_year_10 = EXCLUDED.total_pop_completed_year_10
,total_pop_completed_year_9 = EXCLUDED.total_pop_completed_year_9
,total_pop_completed_year_8_below = EXCLUDED.total_pop_completed_year_8_below
,total_pop_private_dwelling = EXCLUDED.total_pop_private_dwelling
,total_pop_other_dwelling = EXCLUDED.total_pop_other_dwelling
;