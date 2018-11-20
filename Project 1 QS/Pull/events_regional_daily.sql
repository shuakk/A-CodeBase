/*
- For daily ETL, only pull last full month data and this month data

Drop Table if exists quicksight.events_regional_daily;

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

--drop table if exists sandbox.quicksight_data_model_events_regional_daily_su_test;

*/


with cte_date_dm as
(
	select
		date AS calendar_date,
		start_iso_week_date,
		end_iso_week_date,
		month_days_no as num_of_day_in_month
	from public.date_dm
	where date >= date_add('month', -1, date_trunc('month', '${ETL_DATE}'::date))
	and date <= date('${ETL_DATE}')

), cte_events_daily as
(
	select
		date(event_day) as computation_date,
		region,
		account_id,
		sum(case when event_name = 'UserLogin' then 1 else 0 end) as active_account_flag,

		/*Active Account Author Reader*/
		count(distinct case when user_data ilike '%%"role":"READER"%%' and event_name = 'DashboardView' then user_id end) as count_active_readers,
		count(distinct case when event_name = 'UserLogin' then user_id end) as count_login_users, /*count_user_login*/
		count(case when event_name = 'UserLogin' then event_name end) AS count_login_event,
		count(distinct case when event_name = 'UserLogin' then user_id end) - count(distinct case when user_data ilike '%%"role":"READER"%%' and event_name = 'DashboardView' then user_id end) as count_active_authors,
		count(distinct case when event_name = 'Unsubscribe' then user_id end) AS count_users_unsubscribed,
	    count(distinct case when event_name = 'UserActivated' then user_id end) AS count_users_activated,
	    count(distinct case when event_name = 'UserInvite' then user_id end) AS count_users_invited,


		/*Dataset Created two version to test*/
		sum(case when event_name = 'PreparedDataSourceCreated'
					and json_extract_path_text(user_data, 'accelerated') = 'true' and user_data !~~ '%%SAMPLE%%' then 1 else 0 end)
		+
		sum(case when event_name = 'DataSourceGroupCreated'
				and (json_extract_path_text(user_data, 'dataSourceGroupType') = 'REDSHIFT' or json_extract_path_text(user_data, 'dataSourceGroupType') = 'EXTERNAL') then 1 else 0 end)
		+
		sum(case when event_name = 'DataSourceGroupCreated'
				and json_extract_path_text(user_data, 'dataSourceGroupType') = 'RDS' then 1 else 0 end)
		+
		sum(case when event_name = 'DataSourceGroupCreated' and json_extract_path_text(user_data, 'dataSourceGroupType') = 'ATHENA' then 1 else 0 end) AS dataset_created,

		/*Feature Created or Used*/
		sum(case when event_name = 'AnalysisCreated' or event_name = 'AnalysisCreatedFromDashboard' then 1 else 0 end) AS count_analysis_created,
		sum(case when event_name = 'DashboardCreated' then 1 else 0 end) AS count_dashboard_created,
		sum(case when event_name = 'AnalysisDeleted' then 1 else 0 end) AS count_analysis_deleted,
		sum(case when event_name = 'DashboardDeleted' then 1 else 0 end) AS count_dashboard_deleted,
		sum(case when event_name = 'DashboardView' then 1 ELSE 0 END) AS dashboards_viewed,
	  sum(case when event_name = 'DashboardUpdated' then 1 ELSE 0 END) AS dashboards_updated,
	  sum(case when event_name = 'DataSourceGroupCreated' then 1 ELSE 0 END) AS datasourcegroups_created,

		sum(case when event_name = 'PreparedDataSourceCreated' and json_extract_path_text(user_data, 'accelerated') = 'true' and user_data !~~ '%%SAMPLE%%' then 1 else 0 end) AS count_spice,
		sum(case when event_name='DataSourceGroupCreated' and json_extract_path_text(user_data, 'dataSourceGroupType')='S3CONNECTION' then 1 else 0 end) as count_s3_dataset,
		sum(case when event_name = 'DataSourceGroupCreated' and (json_extract_path_text(user_data, 'dataSourceGroupType') = 'REDSHIFT' or json_extract_path_text(user_data, 'dataSourceGroupType') = 'EXTERNAL') then 1 else 0 end) AS count_redshift_dataset,
		sum(case when event_name = 'DataSourceGroupCreated' and json_extract_path_text(user_data, 'dataSourceGroupType') = 'RDS' then 1 else 0 end) AS count_rds_dataset,
		sum(case when event_name = 'DataSourceGroupCreated' and json_extract_path_text(user_data, 'dataSourceGroupType') = 'ATHENA' then 1 else 0 end) AS count_athena_dataset,
		sum(case when event_name = 'DataSourceGroupCreated' and json_extract_path_text(user_data, 'dataSourceGroupType') = 'XLSX' then 1 else 0 end) AS count_xlsx_dataset,
		sum(case when event_name = 'DataSourceGroupCreated' and json_extract_path_text(user_data, 'dataSourceGroupType') = 'CSV' then 1 else 0 end) AS count_csv_dataset,
		sum(case when event_name = 'DataSourceGroupCreated' and (json_extract_path_text(user_data, 'dataSourceGroupType') = 'CSV' or json_extract_path_text(user_data, 'dataSourceGroupType') = 'TSV') then 1 else 0 end) AS count_flat,
		sum(case when event_name = 'DataSourceGroupCreated' and (json_extract_path_text(user_data, 'dataSourceGroupType') = 'ELF' or json_extract_path_text(user_data, 'dataSourceGroupType') = 'CLF') then 1 else 0 end) AS count_log

	from  quicksight.v_raw_events_regionalized
	where 1 = 1
	and date(event_day) >= date_add('month', -1, date_trunc('month', '${ETL_DATE}'::date))
	and date(event_day) <= date('${ETL_DATE}')
	group by 1,2,3
), cte_new_author as
(
	select
		events.first_login_date,
		events.region,
		events.account_id,
		count(distinct events.user_id) as count_new_authors
	from
	(
		select
			account_id,
			user_id,
			region,
			min(date(event_day)) as first_login_date
		from quicksight.v_raw_events_regionalized
		where event_name = 'UserLogin'
		group by 1,2,3
	) as events
	left join
	(
		select
			account_id,
			user_id,
			region,
			date(event_day) as dashboard_view_date
		from quicksight.v_raw_events_regionalized
		where user_data ilike '%%"role":"READER"%%' and event_name = 'DashboardView' group by 1,2,3,4
	) as exclude_reader
	on events.account_id = exclude_reader.account_id
	and events.first_login_date = exclude_reader.dashboard_view_date
	and events.user_id = exclude_reader.user_id
	and events.region = exclude_reader.region
	where exclude_reader.user_id is null
	group by 1,2,3
)
/*rolling day below*/
 , cte_events_rolling as
(
	select distinct
		events.account_id,
		region,
		date(event_day) as computation_date,
		events.user_id,
		user_data,
		event_name
	from  quicksight.v_raw_events_regionalized as events
	where 1 = 1
	/*
	calender date will only go back one full month, so the event table only need to go back two full month
	for rolling 30 days
	*/
	and date(event_day) >= date_add('month', -2 , date_trunc('month', '${ETL_DATE}'::date))
	and date(event_day) <= date('${ETL_DATE}')
	and ((event_name = 'UserLogin') or ( user_data ilike '%%"role":"READER"%%' and event_name = 'DashboardView'))
), cte_l30_stg as
(
	select
		calendar_date,
		computation_date,
		account_id,
		region,
		user_id, user_data, event_name
	from cte_date_dm as d
	left join cte_events_rolling as events
	on	computation_date >= date_add('day', -29, d.calendar_date)
	and computation_date <= d.calendar_date
), cte_l30_count as
(
	select
		calendar_date, account_id, region,
		count(distinct case when user_data ilike '%%"role":"READER"%%' and event_name = 'DashboardView' then user_id end) as count_active_readers,
		count(distinct case when event_name = 'UserLogin' then user_id end) - count(distinct case when user_data ilike '%%"role":"READER"%%' and event_name = 'DashboardView' then user_id end) as count_active_authors,
		count(distinct case when event_name = 'UserLogin' then account_id end) AS count_active_account
	from cte_l30_stg
	group by 1,2,3
), cte_l7_stg as
(
	select
		calendar_date,
		computation_date,
		account_id,
		region,
		user_id, user_data, event_name
	from cte_date_dm as d
	left join cte_events_rolling as events
	on	computation_date >= date_add('day', -6, d.calendar_date)
	and computation_date <= d.calendar_date
), cte_l7_count as
(
	select
		calendar_date, account_id,
		region,
		count(distinct case when user_data ilike '%%"role":"READER"%%' and event_name = 'DashboardView' then user_id end) as count_active_readers,
		count(distinct case when event_name = 'UserLogin' then user_id end) - count(distinct case when user_data ilike '%%"role":"READER"%%' and event_name = 'DashboardView' then user_id end) as count_active_authors,
		count(distinct case when event_name = 'UserLogin' then account_id end) AS count_active_account
	from cte_l7_stg
	group by 1,2,3
)
select
	coalesce(account_dm.computation_date, l30.calendar_date, events.computation_date, new_author.first_login_date) as computation_date,
	coalesce(account_dm.account_id, l30.account_id, events.account_id, new_author.account_id) as account_id,
	coalesce(l30.region, events.region, new_author.region, 'Unknown') as region,

	account_dm.start_iso_week_date,
	account_dm.end_iso_week_date,
	account_dm.num_of_day_in_month,

	account_dm.payer_account_id,
	account_dm.company_name,
	account_dm.internal,
    account_dm.join_date,
	account_dm.last_login_date,
	account_dm.account_type,
	account_dm.edition,

	case when events.active_account_flag > 0 then True else False end as active_account,
	events.count_active_readers,
	events.count_active_authors,
	events.count_users_unsubscribed,
	events.count_users_activated,
	events.count_users_invited,
	events.count_login_event,
	events.dataset_created,
	events.count_analysis_created,
	events.count_dashboard_created,
	events.count_analysis_deleted,
	events.count_dashboard_deleted,
	events.dashboards_viewed,
	events.dashboards_updated,
	events.datasourcegroups_created,
	events.count_spice,
	events.count_s3_dataset,
	events.count_redshift_dataset,
	events.count_rds_dataset,
	events.count_athena_dataset,
	events.count_xlsx_dataset,
	events.count_csv_dataset,
	events.count_flat,
	events.count_log,
	new_author.count_new_authors,

	/*l30_reader.l30_count_reader, l7_reader.l7_count_reader,*/
	l30.count_active_readers as l30_count_active_reader, l30.count_active_authors as l30_count_active_authors, l30.count_active_account as l30_count_active_accounts,
	l7.count_active_readers as l7_count_active_reader, l7.count_active_authors as l7_count_active_authors, l7.count_active_account as l7_count_active_accounts,

	'${ETL_DATE}' as dw_create_date,
	'${ETL_DATE}' as dw_update_date

from quicksight.account_type_edition_daily as account_dm
left join
	cte_l30_count as l30
on 	account_dm.computation_date = l30.calendar_date
and account_dm.account_id = l30.account_id

left join
	cte_l7_count as l7
on  account_dm.computation_date = l7.calendar_date
and l30.account_id = l7.account_id
and l30.region = l7.region

full join
	cte_events_daily as events
on	account_dm.computation_date = events.computation_date
and account_dm.account_id = events.account_id
and l30.region = events.region

full join
	cte_new_author as new_author
on 	account_dm.computation_date = new_author.first_login_date
and account_dm.account_id = new_author.account_id
and events.region = new_author.region

where 1 = 1
and coalesce(account_dm.account_id, l30.account_id, events.account_id, new_author.account_id) is not null
and date_trunc('month',coalesce(account_dm.computation_date, l30.calendar_date, events.computation_date, new_author.first_login_date)) >= date_add('month', -1, date_trunc('month', '${ETL_DATE}'::date))
and coalesce(account_dm.computation_date, l30.calendar_date, events.computation_date, new_author.first_login_date) <=  date('${ETL_DATE}')
;
