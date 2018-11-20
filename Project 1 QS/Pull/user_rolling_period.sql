/*
-- For daily ETL, only pull last full month data and this month data

Drop Table if Exists quicksight.count_user_rolling_days;

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

--drop table if exists sandbox.quicksight_data_model_count_user_rolling_su_test;
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
	select distinct
		events.account_id,
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
), cte_metering_daily as
(
	select distinct
		date(request_day) as computation_date,
		account_id,
		usage_resource
	from awsdw_dm_metering.d_daily_metering_sum_current met
	where product_code='AmazonQuickSight'
	and usage_type like '%%Reader%%'
	/*
	calender date will only go back one full month, so the metering table only need to go back two full month
	for rolling 30 days
	*/
	and date(request_day) >= date_add('month', -2 , date_trunc('month', '${ETL_DATE}'::date))
	and date(request_day) <= date('${ETL_DATE}')
), cte_l30_stg as
(
	select
		calendar_date,
		computation_date,
		account_id, user_id, user_data, event_name
	from cte_date_dm as d
	left join cte_events_daily as events
	on	computation_date >= date_add('day', -29, d.calendar_date)
	and computation_date <= d.calendar_date
), cte_l30_count as
(
	select
		calendar_date, account_id,
		min(computation_date) as l30_period_first_event_date, max(computation_date) as l30_period_last_event_date,
		count(distinct case when user_data ilike '%%"role":"READER"%%' and event_name = 'DashboardView' then user_id end) as count_active_readers,
		count(distinct case when event_name = 'UserLogin' then user_id end) - count(distinct case when user_data ilike '%%"role":"READER"%%' and event_name = 'DashboardView' then user_id end) as count_active_authors,
	  count(case when event_name = 'UserLogin' then event_name end) AS count_login_event,
		count(distinct case when event_name = 'UserLogin' then account_id end) AS count_active_account
	from cte_l30_stg
	group by 1,2
), cte_l7_stg as
(
	select
		calendar_date,
		computation_date,
		account_id, user_id, user_data, event_name
	from cte_date_dm as d
	left join cte_events_daily as events
	on	computation_date >= date_add('day', -6, d.calendar_date)
	and computation_date <= d.calendar_date
), cte_l7_count as
(
	select
		calendar_date, account_id,
		min(computation_date) as l7_period_first_event_date, max(computation_date) as l7_period_last_event_date,
		count(distinct case when user_data ilike '%%"role":"READER"%%' and event_name = 'DashboardView' then user_id end) as count_active_readers,
		count(distinct case when event_name = 'UserLogin' then user_id end) - count(distinct case when user_data ilike '%%"role":"READER"%%' and event_name = 'DashboardView' then user_id end) as count_active_authors,
	  count(case when event_name = 'UserLogin' then event_name end) AS count_login_event,
		count(distinct case when event_name = 'UserLogin' then account_id end) AS count_active_account
	from cte_l7_stg
	group by 1,2
), cte_l30_reader_count as
(
	select
		calendar_date,
		account_id,
		count(distinct usage_resource) as l30_count_reader
	from cte_date_dm as d
	left join cte_metering_daily as metering
	on	computation_date >= date_add('day', -29, d.calendar_date)
	and computation_date <= d.calendar_date
	group by 1,2
), cte_l7_reader_count as
(
	select
		calendar_date,
		account_id,
		count(distinct usage_resource) as l7_count_reader
	from cte_date_dm as d
	left join cte_metering_daily as metering
	on	computation_date >= date_add('day', -6, d.calendar_date)
	and computation_date <= d.calendar_date
	group by 1,2
)
select
	coalesce(account_dm.computation_date, l30.calendar_date, l30_reader.calendar_date) as computation_date,
	coalesce(account_dm.account_id, l30.account_id, l30_reader.account_id) as account_id,

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

	l30_reader.l30_count_reader, l7_reader.l7_count_reader,
	l30.l30_period_first_event_date, l30.l30_period_last_event_date, l30.count_active_readers as l30_count_active_reader, l30.count_active_authors as l30_count_active_authors, l30.count_login_event as l30_total_login_event, l30.count_active_account as l30_count_active_accounts,
	l7.l7_period_first_event_date, l7.l7_period_last_event_date, l7.count_active_readers as l7_count_active_reader, l7.count_active_authors as l7_count_active_authors, l7.count_login_event as l7_total_login_event, l7.count_active_account as l7_count_active_accounts,

	'${ETL_DATE}' as dw_create_date,
	'${ETL_DATE}' as dw_update_date
from quicksight.account_type_edition_daily as account_dm
left join
	cte_l30_count as l30
on	account_dm.computation_date = l30.calendar_date
and account_dm.account_id = l30.account_id
left join
	cte_l7_count as l7
on  l30.calendar_date = l7.calendar_date
and l30.account_id = l7.account_id

full join
	cte_l30_reader_count as l30_reader
on  account_dm.computation_date = l30_reader.calendar_date
and l30.account_id = l30_reader.account_id
left join
	cte_l7_reader_count as l7_reader
on  l30_reader.calendar_date = l7_reader.calendar_date
and l30_reader.account_id = l7_reader.account_id
where 1 = 1
and coalesce(account_dm.account_id, l30.account_id, l30_reader.account_id) is not null
and date_trunc('month',coalesce(account_dm.computation_date, l30.calendar_date, l30_reader.calendar_date)) >= date_add('month', -1, date_trunc('month', '${ETL_DATE}'::date))
and coalesce(account_dm.computation_date, l30.calendar_date, l30_reader.calendar_date) <= date('${ETL_DATE}');
