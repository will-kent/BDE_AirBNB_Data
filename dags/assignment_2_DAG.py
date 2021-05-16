import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from datetime import datetime, timedelta
from code.extract.extract_data import get_airbnb_data, get_csv_data
from code.data_warehouse.transform_data import transform_dim_date
from code.mappings.mapping_tables import write_mappings_file

#########################################################
#
#   Load Environment Variables
#
#########################################################

postgres_conn_name = "postgres_airflow"
airbnb_stage = "staging.listings"
lga_stage = "staging.abs_lga_2016_nsw"
g01_stage = "staging.abs_2016_census_g01_nsw_lga"
g02_stage = "staging.abs_2016_census_g02_nsw_lga"
hn_map_stage = "staging.host_neighbourhood_mapping"
listing_cols = ["id", "listing_url", "scrape_id", "last_scraped", "name", "host_id"
    , "host_name", "host_since", "host_is_superhost", "host_neighbourhood", "host_listings_count"
    , "host_total_listings_count", "neighbourhood_cleansed", "property_type", "room_type", "accommodates"
    , "price", "has_availability", "availability_30", "number_of_reviews", "review_scores_rating"
    , "reviews_per_month"]
lga_cols = ["LGA_CODE_2016", "LGA_NAME_2016", "STATE_NAME_2016"]
g01_cols = ["LGA_CODE_2016","Tot_P_M","Tot_P_F","Tot_P_P","Age_0_4_yr_P","Age_5_14_yr_P","Age_15_19_yr_P"
    , "Age_20_24_yr_P","Age_25_34_yr_P","Age_35_44_yr_P","Age_45_54_yr_P","Age_55_64_yr_P","Age_65_74_yr_P"
    , "Age_75_84_yr_P","Age_85ov_P","Counted_Census_Night_home_P","Count_Census_Nt_Ewhere_Aust_P"
    , "Indigenous_psns_Aboriginal_P","Indig_psns_Torres_Strait_Is_P","Indig_Bth_Abor_Torres_St_Is_P"
    , "Indigenous_P_Tot_P","Birthplace_Australia_P","Birthplace_Elsewhere_P","Lang_spoken_home_Eng_only_P"
    , "Lang_spoken_home_Oth_Lang_P","Australian_citizen_P","Age_psns_att_educ_inst_0_4_P"
    , "Age_psns_att_educ_inst_5_14_P","Age_psns_att_edu_inst_15_19_P","Age_psns_att_edu_inst_20_24_P"
    , "Age_psns_att_edu_inst_25_ov_P","High_yr_schl_comp_Yr_12_eq_P","High_yr_schl_comp_Yr_11_eq_P"
    , "High_yr_schl_comp_Yr_10_eq_P","High_yr_schl_comp_Yr_9_eq_P","High_yr_schl_comp_Yr_8_belw_P"
    , "High_yr_schl_comp_D_n_g_sch_P","Count_psns_occ_priv_dwgs_P","Count_Persons_other_dwgs_P"]
g02_cols = ["LGA_CODE_2016","Median_age_persons","Median_mortgage_repay_monthly"
    , "Median_tot_prsnl_inc_weekly","Median_rent_weekly","Median_tot_fam_inc_weekly"
    , "Average_num_psns_per_bedroom","Median_tot_hhd_inc_weekly","Average_household_size"]
hn_map_cols = ["host_neighbourhood", "neighbourhood_cleansed"]
filename = "listings.csv.gz"
lga_filename = "LGA_2016_NSW.csv"
g01_filename = "2016Census_G01_NSW_LGA.csv"
g02_filename = "2016Census_G02_NSW_LGA.csv"
hn_map_filename = "host_neighbourhood_mapping.csv"
dim_date_table = "dwh.dim_date"
dim_date_start = "2020-01-01"
dim_date_end = "2021-12-31"
dim_date_key = "date"
hnm_select_query = "SELECT * FROM staging.host_neighbourhood_mapping"


########################################################
#
#   DAG Settings
#
#########################################################

from airflow import DAG

dag_default_args = {
    'owner': 'William Kent',
    'start_date': datetime(2021, 4, 1),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id="Assignment_2_Airbnb_Data_Pipeline",
    default_args=dag_default_args,
    schedule_interval="@monthly",
    catchup=False,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################
from airflow.hooks.postgres_hook import PostgresHook

pg_hook = PostgresHook(postgres_conn_name)


def get_airbnb_data_from_gzip(hook, execution_date, file_suffix, airbnb_cols, airbnb_stage):
    get_airbnb_data(hook, execution_date, file_suffix, airbnb_cols, airbnb_stage)


def get_abs_2016_data(hook, file, cols, table, folder):
    get_csv_data(hook, file, cols, table, folder)


def get_mapping_data(hook, file, cols, table, folder):
    get_csv_data(hook, file, cols, table, folder)


def get_dim_date(hook, table, key, start, end):
    transform_dim_date(hook, table, key, start, end)


def write_host_neighbourhood_mapping(hook, hnm_query, hnm_cols, hnm_file_name, folder_name):
    write_mappings_file(hook, hnm_query, hnm_cols, hnm_file_name, folder_name)

#########################################################
#
#   DAG Operator Setup
#
#########################################################

from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

# Make sure staging schema exists
create_staging_schema = PostgresOperator(
    task_id="create_staging_schema",
    sql=["/sql/staging/create_schema.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

# Drop staging tables to clear out data before run
drop_listings_table = PostgresOperator(
    task_id="drop_listings_table",
    sql=["/sql/staging/drop_listings_table.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

drop_lga_table = PostgresOperator(
    task_id="drop_lga_table",
    sql=["/sql/staging/drop_lga_table.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

drop_g01_table = PostgresOperator(
    task_id="drop_g01_table",
    sql=["/sql/staging/drop_g01_table.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

drop_g02_table = PostgresOperator(
    task_id="drop_g02_table",
    sql=["/sql/staging/drop_g02_table.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

drop_host_neighbourhood_mapping_table = PostgresOperator(
    task_id="drop_host_neighbourhood_mapping_table",
    sql=["/sql/staging/drop_host_neighbourhood_mapping_table.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

# Create staging tables to extract new data into
create_listings_table = PostgresOperator(
    task_id="create_listings_table",
    sql=["/sql/staging/create_listings_table.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

create_lga_table = PostgresOperator(
    task_id="create_lga_table",
    sql=["/sql/staging/create_lga_table.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

create_g01_table = PostgresOperator(
    task_id="create_g01_table",
    sql=["/sql/staging/create_g01_table.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

create_g02_table = PostgresOperator(
    task_id="create_g02_table",
    sql=["/sql/staging/create_g02_table.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

create_host_neighbourhood_mapping_table = PostgresOperator(
    task_id="create_host_neighbourhood_mapping_table",
    sql=["/sql/staging/create_host_neighbourhood_mapping_table.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

# Extract new data
extract_monthly_airbnb_listings_data = PythonOperator(
    task_id="extract_monthly_airbnb_listings_data",
    python_callable=get_airbnb_data_from_gzip,
    op_kwargs={"hook": pg_hook,
               "execution_date": "{{ ds }}",
               "file_suffix": filename,
               "airbnb_cols": listing_cols,
               "airbnb_stage": airbnb_stage},
    provide_context=True,
    dag=dag
)

extract_lga_data = PythonOperator(
    task_id="extract_lga_data",
    python_callable=get_abs_2016_data,
    op_kwargs={"hook": pg_hook,
               "file": lga_filename,
               "cols": lga_cols,
               "table": lga_stage,
               "folder": "ABS"},
    provide_context=True,
    dag=dag
)

extract_g01_data = PythonOperator(
    task_id="extract_g01_data",
    python_callable=get_abs_2016_data,
    op_kwargs={"hook": pg_hook,
               "file": g01_filename,
               "cols": g01_cols,
               "table": g01_stage,
               "folder": "ABS"},
    provide_context=True,
    dag=dag
)

extract_g02_data = PythonOperator(
    task_id="extract_g02_data",
    python_callable=get_abs_2016_data,
    op_kwargs={"hook": pg_hook,
               "file": g02_filename,
               "cols": g02_cols,
               "table": g02_stage,
               "folder": "ABS"},
    provide_context=True,
    dag=dag
)


extract_host_neighbourhood_mapping_data = PythonOperator(
    task_id="extract_host_neighbourhood_mapping_data",
    python_callable=get_mapping_data,
    op_kwargs={"hook": pg_hook,
               "file": hn_map_filename,
               "cols": hn_map_cols,
               "table": hn_map_stage,
               "folder": "Mappings"},
    provide_context=True,
    dag=dag
)

# If new host neighbourhoods are fond add to mapping table
add_missing_host_neighbourhoods = PostgresOperator(
    task_id="add_missing_host_neighbourhoods",
    sql=["/sql/staging/add_missing_host_neighbourhoods.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

# Create dwh schema if it doesn't exist
create_dwh_schema = PostgresOperator(
    task_id="create_dwh_schema",
    sql=["/sql/data_warehouse/create_schema.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

# Create dwh tables if they don't already exist
create_dwh_tables = PostgresOperator(
    task_id="create_dwh_tables",
    sql=["/sql/data_warehouse/create_dwh_tables.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

# Create default values in the dim tables to handle missing values
create_dim_defaults = PostgresOperator(
    task_id="create_dim_defaults",
    sql=["/sql/data_warehouse/dim_default_values.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

# First populate dimensions
populate_dim_date = PythonOperator(
    task_id="populate_dim_date",
    python_callable=get_dim_date,
    op_kwargs={"hook": pg_hook,
               "table": dim_date_table,
               "key": dim_date_key,
               "start": dim_date_start,
               "end": dim_date_end},
    provide_context=True,
    dag=dag
)

populate_dim_neighbourhood = PostgresOperator(
    task_id="populate_dim_neighbourhood",
    sql=["/sql/data_warehouse/transform_dim_neighbourhood.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

populate_dim_accommodation = PostgresOperator(
    task_id="populate_dim_accommodation",
    sql=["/sql/data_warehouse/transform_dim_accommodation.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

populate_dim_host = PostgresOperator(
    task_id="populate_dim_host",
    sql=["/sql/data_warehouse/transform_dim_host.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

populate_dim_local_government_area = PostgresOperator(
    task_id="populate_dim_local_government_area",
    sql=["/sql/data_warehouse/transform_dim_local_government_area.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

# Then populate fact tables
populate_fact_airbnb_listings = PostgresOperator(
    task_id="populate_fact_airbnb_listings",
    sql=["/sql/data_warehouse/transform_fact_airbnb_listings.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

populate_fact_lga_demographics = PostgresOperator(
    task_id="populate_fact_lga_demographics",
    sql=["/sql/data_warehouse/transform_fact_lga_demographics.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

# Create mart schema if it doesn't already exist
create_mart_schema = PostgresOperator(
    task_id="create_mart_schema",
    sql=["/sql/mart/create_schema.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

# Create mart tables if they don't already exist
create_mart_tables = PostgresOperator(
    task_id="create_mart_tables",
    sql=["/sql/mart/create_mart_tables.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

# Run mart aggregations and populate tables
run_monthly_neighbourhood_stats = PostgresOperator(
    task_id="run_monthly_neighbourhood_stats",
    sql=["/sql/mart/monthly_neighbourhood_stats.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

run_monthly_accommodation_type_stats = PostgresOperator(
    task_id="run_monthly_accommodation_type_stats",
    sql=["/sql/mart/monthly_accommodation_type_stats.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

run_monthly_host_neighbourhood_stats = PostgresOperator(
    task_id="run_monthly_host_neighbourhood_stats",
    sql=["/sql/mart/monthly_host_neighbourhood_stats.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

# Write host neighbourhood mapping file to csv to allow update of missing mappings
write_host_neighbourhood_mapping = PythonOperator(
    task_id="write_host_neighbourhood_mapping",
    python_callable=write_host_neighbourhood_mapping,
    op_kwargs={"hook": pg_hook,
               "hnm_query": hnm_select_query,
               "hnm_cols": hn_map_cols,
               "hnm_file_name": hn_map_filename,
               "folder_name": "Mappings"},
    provide_context=True,
    dag=dag
)

create_staging_schema >> drop_listings_table >> create_listings_table >> \
extract_monthly_airbnb_listings_data >> add_missing_host_neighbourhoods >> create_dwh_schema >> \
create_dwh_tables >> create_dim_defaults >> populate_dim_date >> populate_fact_airbnb_listings >> \
create_mart_schema >> create_mart_tables >> run_monthly_neighbourhood_stats >> \
write_host_neighbourhood_mapping

create_staging_schema >> drop_lga_table >> create_lga_table >> extract_lga_data >> add_missing_host_neighbourhoods
create_staging_schema >> drop_g01_table >> create_g01_table >> extract_g01_data >> add_missing_host_neighbourhoods
create_staging_schema >> drop_g02_table >> create_g02_table >> extract_g02_data >> add_missing_host_neighbourhoods
create_staging_schema >> drop_host_neighbourhood_mapping_table >> create_host_neighbourhood_mapping_table >> \
extract_host_neighbourhood_mapping_data >> add_missing_host_neighbourhoods

create_dim_defaults >> populate_dim_neighbourhood >> populate_fact_airbnb_listings
create_dim_defaults >> populate_dim_accommodation >> populate_fact_airbnb_listings
create_dim_defaults >> populate_dim_host >> populate_fact_airbnb_listings
create_dim_defaults >> populate_dim_local_government_area >> populate_fact_lga_demographics >> create_mart_schema

populate_dim_local_government_area >> populate_fact_airbnb_listings

create_mart_tables >> run_monthly_accommodation_type_stats >> write_host_neighbourhood_mapping
create_mart_tables >> run_monthly_host_neighbourhood_stats >> write_host_neighbourhood_mapping