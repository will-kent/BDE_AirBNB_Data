import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from datetime import datetime, timedelta
from code.extract.extract_data import get_airbnb_data, get_lga_data, get_g01_data

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
listing_cols = ["id", "listing_url", "scrape_id", "last_scraped", "name", "host_id"
    , "host_name", "host_since", "host_neighbourhood", "host_listings_count", "host_total_listings_count"
    , "neighbourhood_cleansed", "property_type", "room_type", "accommodates", "price", "has_availability"
    , "availability_30", "review_scores_rating", "reviews_per_month"]
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
filename = "listings.csv.gz"
lga_filename = "LGA_2016_NSW.csv"
g01_filename = "2016Census_G01_NSW_LGA.csv"
g02_filename = "2016Census_G02_NSW_LGA.csv"



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

# Change abs extracts to use single function
def get_lga_2016_data(hook, file, lga_cols, lga_table):
    get_lga_data(hook, file, lga_cols, lga_table)


def get_g01_2016_data(hook, file, g01_cols, g01_table):
    get_g01_data(hook, file, g01_cols, g01_table)


def get_g02_2016_data(hook, file, g02_cols, g02_table):
    get_g01_data(hook, file, g02_cols, g02_table)


#########################################################
#
#   DAG Operator Setup
#
#########################################################

from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

create_staging_schema = PostgresOperator(
    task_id="create_staging_schema",
    sql=["/sql/staging/create_schema.sql"],
    postgres_conn_id=postgres_conn_name,
    autocommit=True,
    dag=dag
)

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
    python_callable=get_lga_2016_data,
    op_kwargs={"hook": pg_hook,
               "file": lga_filename,
               "lga_cols": lga_cols,
               "lga_table": lga_stage},
    provide_context=True,
    dag=dag
)

extract_g01_data = PythonOperator(
    task_id="extract_g01_data",
    python_callable=get_g01_2016_data,
    op_kwargs={"hook": pg_hook,
               "file": g01_filename,
               "g01_cols": g01_cols,
               "g01_table": g01_stage},
    provide_context=True,
    dag=dag
)

extract_g02_data = PythonOperator(
    task_id="extract_g02_data",
    python_callable=get_g02_2016_data,
    op_kwargs={"hook": pg_hook,
               "file": g02_filename,
               "g02_cols": g02_cols,
               "g02_table": g02_stage},
    provide_context=True,
    dag=dag
)


create_staging_schema >> drop_listings_table >> create_listings_table >> \
extract_monthly_airbnb_listings_data
create_staging_schema >> drop_lga_table >> create_lga_table >> extract_lga_data
create_staging_schema >> drop_g01_table >> create_g01_table >> extract_g01_data
create_staging_schema >> drop_g02_table >> create_g02_table >> extract_g02_data