/*
-- For daily ETL, only pull last full month data and this month data
-- Miss credit session to caluclate the interactive session
-- Ent/Std Free/Trial will be different since Ent/Std come from account type edition logicc instead of revenue metrics name

Drop Table if Exists quicksight.revenue_daily;

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


--drop table if exists sandbox.quicksight_data_model_revenue_daily_su_test;

*/


with cte_revenue_usage as
(
	select
		computation_date,
		account_id,
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
	and computation_date >= date_add('month', -1, date_trunc('month', '${ETL_DATE}'::date))
	and computation_date <= date('${ETL_DATE}')
	group by 1,2,3
), cte_prov_reader as
(
	select
		date(request_day) as computation_date,
		account_id,
		count(distinct usage_resource) as provisioned_readers
	from awsdw_dm_metering.d_daily_metering_sum_current met
	where product_code='AmazonQuickSight'
	and usage_type like '%%Reader%%'
	and  date(request_day) >= date_add('month', -1 , date_trunc('month', '${ETL_DATE}'::date))
	and date(request_day) <= date('${ETL_DATE}')
	group by 1,2
 )
select
	account_dm.computation_date,
	account_dm.account_id,

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

	revenue.gross_revenue,
	revenue.revenue,
	revenue.annual_sub_revenue,
	revenue.monthly_sub_revenue,
	revenue.spice_revenue,
	revenue.reader_revenue,

	/*revenue.count_provisioned_author,*/
	revenue.count_provisioned_paid_authors_with_annual_subscription + revenue.count_provisioned_paid_authors_with_monthly_subscription + revenue.count_provisioned_free_users + revenue.count_provisioned_trial_users as count_provisioned_author,
	revenue.count_provisioned_paid_authors_with_annual_subscription as count_provisioned_paid_authors_with_annual_subscription,
	revenue.count_provisioned_paid_authors_with_monthly_subscription as count_provisioned_paid_authors_with_monthly_subscription,
	revenue.count_provisioned_free_users as count_provisioned_free_users,
	revenue.count_provisioned_trial_users,

	revenue.count_provisioned_reader_seesions,
	revenue.spice_gb as spice_gb,
	/*revenue.count_provisioned_reader_interactive_sessions,*/
	NULL as count_provisioned_reader_interactive_sessions,
	revenue.count_provisioned_reader_email_sessions,

	reader.provisioned_readers,
	'${ETL_DATE}' as dw_create_date,
	'${ETL_DATE}' as dw_update_date,

	/*New added column*/
	revenue.net_revenue
from quicksight.account_type_edition_daily as account_dm
left join
	cte_revenue_usage as revenue
on	account_dm.computation_date = revenue.computation_date
and account_dm.account_id = revenue.account_id

left join
	cte_prov_reader as reader
on	account_dm.computation_date = reader.computation_date
and account_dm.account_id = reader.account_id
where 1 = 1
	and date_trunc('month',account_dm.computation_date) >= date_add('month', -1, date_trunc('month', '${ETL_DATE}'::date))
	and account_dm.computation_date <= date('${ETL_DATE}');
