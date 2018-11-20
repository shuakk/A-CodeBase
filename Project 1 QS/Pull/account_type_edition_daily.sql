/*
This is daily table.
Last Login, Join date, Internal, Payer Account ID and Company name will be identical for given account cross all time
Account Type and Edition will change on daily basis.

Drop table if exists quicksight.account_type_edition_daily

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
	dw_update_date timestamp ENCODE runlength,
	primary key(computation_date, account_id)
)
diststyle even
sortkey (computation_date, account_id)
;

--drop table if exists sandbox.quicksight_data_model_account_type_edition_daily_su_test;

*/

with cte_date_dm as
(
	select
		date AS calendar_date,
		start_iso_week_date,
		end_iso_week_date,
		month_days_no as num_of_day_in_month
	from public.date_dm
	where date >= date_add('month', -1, date_trunc('month', '$ETL_DATE'::date))
	and date <=  '$ETL_DATE'
), cte_account_id as
(
	select distinct account_id from quicksight.v_raw_events_regionalized where account_id not in
	(
		select account_id
		from bimetadata_rpt.account_details_dm
		where account_status_code = 'Suspended' or fraud_enforcement_status like '%Suspended' or fraud_enforcement_status like '%Terminated')
	union
	select distinct account_id from bimetadata_rpt.dbs_business_metrics where product_code = 'AmazonQuickSight'
	/*
	union
	select distinct account_id from bimetadata_rpt.dbs_business_metrics_daily_account where product_code = 'AmazonQuickSight'
	*/
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
	and computation_date <=  '$ETL_DATE'
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
	date.start_iso_week_date,
	date.end_iso_week_date,
	date.num_of_day_in_month,

	sign_up.join_date,
	last_login.last_login_date,


	account_dm_latest.payer_account_id,
	account_dm_latest.company_name,
	account_dm_latest.internal,


	CASE
		WHEN free_tier > 0 AND free_trial = 0 AND paid = 0 AND join_date < date_add ('month',-2,DATE_TRUNC('month', week_end_date)) THEN 'free'
		WHEN (free_tier > 0  AND free_trial > 0 AND paid = 0) THEN 'trial'
		WHEN (free_tier > 0 AND join_date >= date_add ('month',-2,DATE_TRUNC('month',week_end_date)) AND free_trial = 0 AND paid = 0) THEN 'free in trial'
		WHEN (free_tier > 0 AND free_trial > 0 AND paid > 0) THEN 'paid in trial'
		WHEN free_trial = 0 AND paid > 0 THEN 'paid'
		ELSE NULL
	END AS account_type,

	CASE
		WHEN total_ent::INTEGER> 0 THEN 'enterprise'
		WHEN total_std::INTEGER> 0 THEN 'standard'
		ELSE NULL
	END AS edition,

	'$ETL_DATE' as dw_create_date,
	'$ETL_DATE' as dw_update_date
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
where date.calendar_date >= date_add('month', -1, date_trunc('month', '$ETL_DATE'::date))
and date.calendar_date <= '$ETL_DATE';
