INSERT INTO dwh.dim_local_government_area (
    lga_reference
    ,lga_name
    ,state_name
)
SELECT lga_code_2016
	,TRIM(SPLIT_PART(lga_name_2016,'(',1)) as lga_name_2016
	,state_name_2016
FROM staging.lga_2016_nsw
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
ON CONFLICT (lga_reference)
DO UPDATE SET
lga_name = EXCLUDED.lga_name
,state_name = EXCLUDED.state_name
;