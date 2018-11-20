
update quicksight.revenue_daily
set net_revenue = t1.net_revenue, gross_revenue = t1.gross_revenue, revenue = t1.revenue, count_provisioned_reader_interactive_sessions = NULL
from
(
select
		computation_date,
		account_id,
		payer_account_id,
		pgdate_part('day', last_day(computation_date)) as number_days_in_computation_month,

		/*Revenue section*/
		sum(case when metrics_name = 'QS-Overall' then gross_revenue else 0 end) as gross_revenue,
		sum(case when metrics_name = 'QS-Overall' then gross_revenue* (1 - estimated_negative_revenue_share) else 0 end) as revenue,
		sum(case when metrics_name = 'QS-User-Enterprise-Annual' or metrics_name = 'QS-User-Standard-Annual' then gross_revenue * (1 - estimated_negative_revenue_share) else 0 end) as annual_sub_revenue,
		sum(case when metrics_name = 'QS-User-Enterprise-Month' or metrics_name = 'QS-User-Standard-Month' then gross_revenue * (1 - estimated_negative_revenue_share) else 0 end) as monthly_sub_revenue,
		sum(case when metrics_name = 'SPICE' then gross_revenue* (1 - estimated_negative_revenue_share) else 0 end) as spice_revenue,
		sum(case when metrics_name = 'QS-ReaderOverall' or metrics_name = 'QS-ReaderReportSession' then gross_revenue * (1 - estimated_negative_revenue_share) else 0 end) as reader_revenue,

		/*Convert usage into count*/
		--sum(case when metrics_name = 'TotalEnterprise' or metrics_name = 'TotalStandard' then usage else 0 end * pgdate_part('day', last_day(computation_date))) as count_provisioned_author,
		sum(case when metrics_name = 'PaidAnnual' then usage else 0 end * pgdate_part('day', last_day(computation_date)))  as count_provisioned_paid_authors_with_annual_subscription,
		sum(case when metrics_name = 'PaidMonthly' then usage else 0 end * pgdate_part('day', last_day(computation_date)))  as count_provisioned_paid_authors_with_monthly_subscription,

		sum(case
				when metrics_name = 'FreeTier' and computation_date >= '2017-02-01' then usage * pgdate_part('day', last_day(computation_date))
				when metrics_name = 'FreeTier' and computation_date < '2017-02-01' then usage / 24.0
				else 0 end)  as count_provisioned_free_users,
		sum(case
				when metrics_name = 'FreeTrial' and computation_date >= '2017-02-01' then usage * pgdate_part('day', last_day(computation_date))
				when metrics_name = 'FreeTrial' and computation_date < '2017-02-01' then usage / 24.0
				else 0 end)  as count_provisioned_trial_users,

		sum(case when metrics_name = 'QS-ReaderOverall' then usage else 0 end) as count_provisioned_reader_seesions,
		sum(case when metrics_name = 'QS-ReaderReportSession' then usage end) as count_provisioned_reader_email_sessions,
		sum(case when metrics_name = 'SPICE' then usage else 0 end * pgdate_part('day', last_day(computation_date)))  as spice_gb,
		sum(case when metrics_name = 'QS-ReaderOverall' then usage else 0 end) /*+ credit which is not exists */ -  sum(case when metrics_name = 'QS-ReaderReportSession' then usage end) as count_provisioned_reader_interactive_sessions,

		/*New added column*/
		sum(case when metrics_name = 'QS-Overall' then gross_revenue + discounts + crf else 0 end) as net_revenue

	from  bimetadata_rpt.dbs_business_metrics
	where product_code = 'AmazonQuickSight'
	group by 1,2,3, 4
) as t1
where revenue_daily.account_id = t1.account_id and revenue_daily.computation_date = t1.computation_date;




update quicksight.events_daily
set count_analysis_created = t1.count_analysis_created
from
(
select
		date(event_day) as computation_date,
		account_id as account_id,
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
	group by 1,2
) as t1
where events_daily.account_id = t1.account_id and events_daily.computation_date = t1.computation_date;



update quicksight.events_regional_daily
set count_analysis_created = t1.count_analysis_created, dw_update_date = t1.dw_update_date
from
(
select
		date(event_day) as computation_date,
		account_id as account_id,
		region,
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
		sum(case when event_name = 'DataSourceGroupCreated' and (json_extract_path_text(user_data, 'dataSourceGroupType') = 'ELF' or json_extract_path_text(user_data, 'dataSourceGroupType') = 'CLF') then 1 else 0 end) AS count_log,

		getdate() as dw_update_date

	from  quicksight.v_raw_events_regionalized
	where 1 = 1
	group by 1,2,3
) as t1
where events_regional_daily.account_id = t1.account_id and events_regional_daily.computation_date = t1.computation_date and events_regional_daily.region = t1.region;




update quicksight.account_type_edition_daily
set account_type = t1.account_type, dw_update_date = t1.dw_update_date
from
(
with cte_date_dm as
(
	select
		date AS calendar_date,
		start_iso_week_date,
		end_iso_week_date,
		month_days_no as num_of_day_in_month
	from public.date_dm
	--where date >= date_add('month', -1, date_trunc('month', '$ETL_DATE'::date))
	--and date <=  '$ETL_DATE'
), cte_account_id as
(
	select distinct account_id from quicksight.v_raw_events_regionalized
	union
	select distinct account_id from bimetadata_rpt.dbs_business_metrics where product_code = 'AmazonQuickSight'
	union
	select distinct account_id from bimetadata_rpt.dbs_business_metrics_daily_account where product_code = 'AmazonQuickSight'
), cte_account_dm_latest as
(
	select
		acct_id.account_id,
		payer_account_id,
		company_name,
		internal
	from cte_account_id as acct_id
	inner join
		bimetadata_rpt.account_details_dm as acct_detail
	on acct_id.account_id = acct_detail.account_id
), cte_sign_up AS
(
	SELECT account_id,
       MIN(computation_date) AS join_date
	FROM bimetadata_rpt.dbs_business_metrics_daily_account
	WHERE 1 = 1
	AND   product_code = 'AmazonQuickSight'
	GROUP BY 1
), cte_bimetadata_daily_account_type_edition as
(
	SELECT
		computation_date,
		start_iso_week_date AS week_start_date,
		end_iso_week_date AS week_end_date,
		account_id,
		SUM(CASE WHEN metrics_name = 'FreeTier' THEN 1 ELSE 0 END) AS free_tier,
		SUM(CASE WHEN metrics_name = 'FreeTrial' THEN 1 ELSE 0 END) AS free_trial,
		SUM(CASE WHEN metrics_name = 'PaidStandard' OR metrics_name = 'PaidEnterprise' THEN 1 ELSE 0 END) AS paid,
		SUM(CASE WHEN metrics_name = 'TotalStandard' THEN 1 ELSE 0 END) AS total_std,
		SUM(CASE WHEN metrics_name = 'TotalEnterprise' THEN 1 ELSE 0 END) AS total_ent
	FROM bimetadata_rpt.dbs_business_metrics_daily_account  b
	WHERE b.product_code = 'AmazonQuickSight'
	AND   b.metrics_name in ('FreeTier', 'FreeTrial', 'PaidStandard', 'PaidEnterprise', 'TotalStandard', 'TotalEnterprise')
	AND   CEIL(b.daily_usage* pgdate_part('day', last_day(b.computation_date))) > 0
	--and computation_date <=  '$ETL_DATE'
	GROUP BY 1, 2, 3, 4
), cte_account_last_login as
(
	SELECT
		account_id,
        MAX(event_day) AS last_login_date
	from quicksight.v_raw_events_regionalized
	where event_name = 'UserLogin'
	group by 1
)
SELECT
	date.calendar_date as computation_date,
	account_dm_latest.account_id as account_id,

	CASE
		WHEN free_tier > 0 AND free_trial = 0 AND paid = 0 AND join_date < date_add ('month',-2,DATE_TRUNC('month', week_end_date)) THEN 'free'
		WHEN (free_tier > 0  AND free_trial > 0 AND paid = 0) THEN 'trial'
		WHEN (free_tier > 0 AND join_date >= date_add ('month',-2,DATE_TRUNC('month',week_end_date)) AND free_trial = 0 AND paid = 0) THEN 'free in trial'
		WHEN (free_tier > 0 AND free_trial > 0 AND paid > 0) THEN 'paid in trial'
		WHEN free_trial = 0 AND paid > 0 THEN 'paid'
		ELSE NULL
	END AS account_type,

	getdate() as dw_update_date
FROM
	cte_date_dm as date
cross join
	cte_account_dm_latest as account_dm_latest
left join
	cte_account_last_login as last_login
on  account_dm_latest.account_id = last_login.account_id
left join
	cte_sign_up as sign_up
on	account_dm_latest.account_id = sign_up.account_id
left join
	cte_bimetadata_daily_account_type_edition as daily_account_type_edition
on  date.calendar_date = daily_account_type_edition.computation_date
and account_dm_latest.account_id = daily_account_type_edition.account_id
--where date.calendar_date >= date_add('month', -1, date_trunc('month', '$ETL_DATE'::date))
--and date.calendar_date <= '$ETL_DATE'
) as t1
where account_type_edition_daily.account_id = t1.account_id and account_type_edition_daily.computation_date = t1.computation_date;


delete quicksight.account_type_edition_daily
where account_id in (select account_id from bimetadata_rpt.account_details_dm where account_status_code = 'Suspended');
