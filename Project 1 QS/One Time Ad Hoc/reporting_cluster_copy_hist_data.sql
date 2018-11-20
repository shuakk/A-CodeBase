CREATE TABLE quicksight.account_type_edition_daily
(
	computation_date DATE ENCODE runlength,
	account_id VARCHAR(100) ENCODE lzo,
	start_iso_week_date DATE ENCODE runlength,
	end_iso_week_date DATE ENCODE runlength,
	num_of_day_in_month SMALLINT ENCODE runlength,
	join_date DATE ENCODE lzo,
	last_login_date TIMESTAMP ENCODE lzo,
	payer_account_id VARCHAR(32) ENCODE lzo,
	company_name VARCHAR(256) ENCODE lzo,
	internal VARCHAR(32) ENCODE lzo,
	account_type VARCHAR(13) ENCODE lzo,
	edition VARCHAR(10) ENCODE lzo,
	dw_create_date timestamp ENCODE runlength,
	dw_updated_date timestamp ENCODE runlength,
	primary key(computation_date, account_id)
)
diststyle even
sortkey (computation_date, account_id)
;


copy quicksight.account_type_edition_daily
from 's3://db_services.aws.amazon.com/quicksight/data_model/account_type_edition_daily_pull/Backfill/' 
iam_role 'arn:aws:iam::015982553965:role/mySpectrumRole'
region 'us-east-1'
delimiter '|'
ESCAPE REMOVEQUOTES gzip BLANKSASNULL EMPTYASNULL ACCEPTINVCHARS ' ' MAXERROR 10;
; 

create table quicksight.account_type_edition_daily_stg as select * from quicksight.account_type_edition_daily limit 1;

CREATE TABLE quicksight.events_daily
(
	computation_date DATE ENCODE runlength,
	account_id VARCHAR(100) ENCODE lzo,
	start_iso_week_date DATE ENCODE runlength,
	end_iso_week_date DATE ENCODE runlength,
	num_of_day_in_month SMALLINT ENCODE runlength,
	payer_account_id VARCHAR(32) ENCODE lzo,
	company_name VARCHAR(256) ENCODE lzo,
	internal VARCHAR(32) ENCODE lzo,
	join_date DATE ENCODE lzo,
	last_login_date TIMESTAMP ENCODE lzo,
	account_type VARCHAR(13) ENCODE lzo,
	edition VARCHAR(10) ENCODE lzo,
	active_account BOOLEAN,
	count_active_readers BIGINT ENCODE lzo,
	count_active_authors BIGINT ENCODE lzo,
	count_users_unsubscribed BIGINT ENCODE lzo,
	count_users_activated BIGINT ENCODE lzo,
	count_users_invited BIGINT ENCODE lzo,
	count_login_event BIGINT ENCODE lzo,
	dataset_created BIGINT ENCODE lzo,
	count_analysis_created BIGINT ENCODE lzo,
	--count_analysis_created_v1_wbr_db_feature_usage_met BIGINT ENCODE lzo,
	--count_analysis_created_v2_wbr_db_user_activity BIGINT ENCODE lzo,
	count_dashboard_created BIGINT ENCODE lzo,
	count_analysis_deleted BIGINT ENCODE lzo,
	count_dashboard_deleted BIGINT ENCODE lzo,
	dashboards_viewed BIGINT ENCODE lzo,
	dashboards_updated BIGINT ENCODE lzo,
	datasourcegroups_created BIGINT ENCODE lzo,
	count_spice BIGINT ENCODE lzo,
	count_s3_dataset BIGINT ENCODE lzo,
	count_redshift_dataset BIGINT ENCODE lzo,
	count_rds_dataset BIGINT ENCODE lzo,
	count_athena_dataset BIGINT ENCODE lzo,
	count_xlsx_dataset BIGINT ENCODE lzo,
	count_csv_dataset BIGINT ENCODE lzo,
	count_flat BIGINT ENCODE lzo,
	count_log BIGINT ENCODE lzo,
	count_new_authors BIGINT ENCODE lzo,
	dw_create_date timestamp ENCODE runlength,
	dw_update_date timestamp ENCODE runlength,
	primary key (computation_date, account_id)

)
diststyle even
sortkey (computation_date, account_id)
;

copy quicksight.events_daily
from 's3://db_services.aws.amazon.com/quicksight/data_model/events_daily_pull/Backfill/' 
iam_role 'arn:aws:iam::015982553965:role/mySpectrumRole'
region 'us-east-1'
delimiter '|'
ESCAPE REMOVEQUOTES gzip BLANKSASNULL EMPTYASNULL ACCEPTINVCHARS ' ' MAXERROR 10;
; 

create table quicksight.events_daily_stg as select * from quicksight.events_daily limit 1;


CREATE TABLE quicksight.revenue_daily
(
	computation_date DATE ENCODE runlength,
	account_id VARCHAR(100) ENCODE lzo,
	start_iso_week_date DATE ENCODE runlength,
	end_iso_week_date DATE ENCODE runlength,
	num_of_day_in_month SMALLINT ENCODE runlength,
	payer_account_id VARCHAR(32) ENCODE lzo,
	company_name VARCHAR(256) ENCODE lzo,
	internal VARCHAR(32) ENCODE lzo,
	join_date DATE ENCODE lzo,
	last_login_date TIMESTAMP ENCODE lzo,
	account_type VARCHAR(13) ENCODE lzo,
	edition VARCHAR(10) ENCODE lzo,
	gross_revenue NUMERIC(38, 22) ENCODE lzo,
	revenue NUMERIC(38, 30) ENCODE lzo,
	annual_sub_revenue NUMERIC(38, 30) ENCODE lzo,
	monthly_sub_revenue NUMERIC(38, 30) ENCODE lzo,
	spice_revenue NUMERIC(38, 30) ENCODE lzo,
	reader_revenue NUMERIC(38, 30) ENCODE lzo,
	count_provisioned_author NUMERIC(38,30),
	count_provisioned_paid_authors_with_annual_subscription NUMERIC(38,30),
	count_provisioned_paid_authors_with_monthly_subscription NUMERIC(38,30),
	count_provisioned_free_users NUMERIC(38,30),
	count_provisioned_trial_users NUMERIC(38,30),
	count_provisioned_reader_seesions NUMERIC(38,30) ENCODE lzo,
	spice_gb NUMERIC(38,30),
	count_provisioned_reader_interactive_sessions NUMERIC(38,30) ENCODE lzo,
	count_provisioned_reader_email_sessions NUMERIC(38, 30) ENCODE lzo,
	provisioned_readers BIGINT ENCODE lzo,
	dw_create_date timestamp ENCODE runlength,
	dw_update_date timestamp ENCODE runlength,
	net_revenue NUMERIC(38, 30) ENCODE lzo,
	primary key(computation_date, account_id)
)
diststyle even
sortkey(computation_date, account_id)
;

copy quicksight.revenue_daily
from 's3://db_services.aws.amazon.com/quicksight/data_model/revenue_daily_pull/Backfill/' 
iam_role 'arn:aws:iam::015982553965:role/mySpectrumRole'
region 'us-east-1'
delimiter '|'
ESCAPE REMOVEQUOTES gzip BLANKSASNULL EMPTYASNULL ACCEPTINVCHARS ' ' MAXERROR 10;
; 

create table quicksight.revenue_daily_stg as select * from quicksight.revenue_daily limit 1;

CREATE TABLE quicksight.count_user_rolling_days
(
	computation_date DATE ENCODE runlength,
	account_id VARCHAR(100) ENCODE lzo,
	start_iso_week_date DATE ENCODE runlength,
	end_iso_week_date DATE ENCODE runlength,
	num_of_day_in_month SMALLINT ENCODE runlength,
	payer_account_id VARCHAR(32) ENCODE lzo,
	company_name VARCHAR(256) ENCODE lzo,
	internal VARCHAR(32) ENCODE lzo,
	join_date DATE ENCODE lzo,
	last_login_date TIMESTAMP ENCODE lzo,
	account_type VARCHAR(13) ENCODE lzo,
	edition VARCHAR(10) ENCODE lzo,
	l30_count_reader BIGINT ENCODE lzo,
	l7_count_reader BIGINT ENCODE lzo,
	l30_period_first_event_date DATE ENCODE lzo,
	l30_period_last_event_date DATE ENCODE lzo,
	l30_count_active_reader BIGINT ENCODE lzo,
	l30_count_active_authors BIGINT ENCODE lzo,
	l30_total_login_event BIGINT ENCODE lzo,
	l30_count_active_accounts BIGINT ENCODE lzo,
	l7_period_first_event_date DATE ENCODE lzo,
	l7_period_last_event_date DATE ENCODE lzo,
	l7_count_active_reader BIGINT ENCODE lzo,
	l7_count_active_authors BIGINT ENCODE lzo,
	l7_total_login_event BIGINT ENCODE lzo,
	l7_count_active_accounts BIGINT ENCODE lzo,
	dw_create_date timestamp ENCODE runlength,
  dw_update_date timestamp encode runlength,
	primary key(computation_date, account_id)
)
diststyle even
sortkey (computation_date, account_id)
;

copy quicksight.count_user_rolling_days
from 's3://db_services.aws.amazon.com/quicksight/data_model/user_rolling_period_pull/Backfill/' 
iam_role 'arn:aws:iam::015982553965:role/mySpectrumRole'
region 'us-east-1'
delimiter '|'
ESCAPE REMOVEQUOTES gzip BLANKSASNULL EMPTYASNULL ACCEPTINVCHARS ' ' MAXERROR 10;
; 

create table quicksight.count_user_rolling_days_stg as select * from quicksight.count_user_rolling_days limit 1;


CREATE TABLE quicksight.events_regional_daily
(
	computation_date DATE ENCODE runlength,
	account_id VARCHAR(100) ENCODE lzo,
	region VARCHAR(100) ENCODE runlength,
	start_iso_week_date DATE ENCODE runlength,
	end_iso_week_date DATE ENCODE runlength,
	num_of_day_in_month SMALLINT ENCODE runlength,
	payer_account_id VARCHAR(32) ENCODE lzo,
	company_name VARCHAR(256) ENCODE lzo,
	internal VARCHAR(32) ENCODE lzo,
	join_date DATE ENCODE lzo,
	last_login_date TIMESTAMP ENCODE lzo,
	account_type VARCHAR(13) ENCODE lzo,
	edition VARCHAR(10) ENCODE lzo,
	active_account BOOLEAN,
	count_active_readers BIGINT ENCODE lzo,
	count_active_authors BIGINT ENCODE lzo,
	count_users_unsubscribed BIGINT ENCODE lzo,
	count_users_activated BIGINT ENCODE lzo,
	count_users_invited BIGINT ENCODE lzo,
	count_login_event BIGINT ENCODE lzo,
	dataset_created BIGINT ENCODE lzo,
	count_analysis_created BIGINT ENCODE lzo,
	count_dashboard_created BIGINT ENCODE lzo,
	count_analysis_deleted BIGINT ENCODE lzo,
	count_dashboard_deleted BIGINT ENCODE lzo,
	dashboards_viewed BIGINT ENCODE lzo,
	dashboards_updated BIGINT ENCODE lzo,
	datasourcegroups_created BIGINT ENCODE lzo,
	count_spice BIGINT ENCODE lzo,
	count_s3_dataset BIGINT ENCODE lzo,
	count_redshift_dataset BIGINT ENCODE lzo,
	count_rds_dataset BIGINT ENCODE lzo,
	count_athena_dataset BIGINT ENCODE lzo,
	count_xlsx_dataset BIGINT ENCODE lzo,
	count_csv_dataset BIGINT ENCODE lzo,
	count_flat BIGINT ENCODE lzo,
	count_log BIGINT ENCODE lzo,
	count_new_authors BIGINT ENCODE lzo,
	l30_count_active_reader BIGINT ENCODE lzo,
	l30_count_active_authors BIGINT ENCODE lzo,
	l30_count_active_accounts BIGINT ENCODE lzo,
	l7_count_active_reader BIGINT ENCODE lzo,
	l7_count_active_authors BIGINT ENCODE lzo,
	l7_count_active_accounts BIGINT ENCODE lzo,
	dw_create_date TIMESTAMP ENCODE runlength,
	dw_update_date TIMESTAMP ENCODE runlength,
	primary key (computation_date, account_id, region)
)
diststyle even
sortkey (computation_date, account_id, region)
;

copy quicksight.events_regional_daily
from 's3://db_services.aws.amazon.com/quicksight/data_model/events_regional_daily_pull/Backfill/' 
iam_role 'arn:aws:iam::015982553965:role/mySpectrumRole'
region 'us-east-1'
delimiter '|'
ESCAPE REMOVEQUOTES gzip BLANKSASNULL EMPTYASNULL ACCEPTINVCHARS ' ' MAXERROR 10;
; 

create table quicksight.events_regional_daily_stg as select * from quicksight.events_regional_daily limit 1;