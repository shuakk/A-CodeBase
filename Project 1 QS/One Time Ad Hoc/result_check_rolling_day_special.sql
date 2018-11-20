with v_active_account_stats as (
SELECT
	a.calendar_date,
	a.account_count AS "active accounts (30 days)",
	a.user_count AS "active users (30 days)",
	b.daily_account_counts AS "daily active accounts",
	b.daily_user_counts AS "daily active users"
FROM
	(
	SELECT
		b.calendar_date, count(DISTINCT thirty.account_id) AS account_count, count(DISTINCT thirty.user_id) AS user_count
    FROM
		(
		SELECT
			date(v_raw_events_regionalized_external_only.event_day) as event_day,
			v_raw_events_regionalized_external_only.account_id,
			v_raw_events_regionalized_external_only.user_id,
			v_raw_events_regionalized_external_only.event_name
    FROM quicksight.v_raw_events_regionalized_external_only
    WHERE v_raw_events_regionalized_external_only.event_name::text = 'UserLogin'::character varying::text
		AND date(v_raw_events_regionalized_external_only.event_day) >= ((( SELECT reporting_dates.dbr_start_date
                       FROM reporting_dates)) - 45)
		AND date(v_raw_events_regionalized_external_only.event_day) <= (( SELECT reporting_dates.dbr_start_date
                       FROM reporting_dates))
		) thirty
  	JOIN
		(
		SELECT
			date_dm.date AS calendar_date
    FROM date_dm
    WHERE date_dm.date >= (( SELECT reporting_dates.dbr_start_date - 13
                       FROM reporting_dates))
		AND date_dm.date <= (( SELECT reporting_dates.dbr_start_date
                       FROM reporting_dates))
		) b
	ON thirty.event_day >= (b.calendar_date - 29)
	AND thirty.event_day <= b.calendar_date
 	GROUP BY b.calendar_date
	) a

JOIN
	(
	SELECT
		date_trunc('day'::character varying::text, v_raw_events_regionalized_external_only.event_day) AS activity_day,
		count(DISTINCT v_raw_events_regionalized_external_only.account_id) AS daily_account_counts,
		count(DISTINCT v_raw_events_regionalized_external_only.user_id) AS daily_user_counts
 	FROM quicksight.v_raw_events_regionalized_external_only
  WHERE v_raw_events_regionalized_external_only.event_name::text = 'UserLogin'::character varying::text 
	AND date_trunc('day'::character varying::text, v_raw_events_regionalized_external_only.event_day) >= (( SELECT reporting_dates.dbr_start_date - 13
               FROM reporting_dates))
	AND date_trunc('day'::character varying::text, v_raw_events_regionalized_external_only.event_day) <= (( SELECT reporting_dates.dbr_start_date
               FROM reporting_dates))
    GROUP BY date_trunc('day'::character varying::text, v_raw_events_regionalized_external_only.event_day)
	) b
ON a.calendar_date::timestamp without time zone = b.activity_day
), dates as
(
	select distinct date as computation_date
	from date_dm 
	where date between '2018-11-05'-13 and date(getdate())
),
reader_actives as
(
	select date(event_day) as computation_Date
	, user_id
	from  quicksight.v_raw_events_regionalized_external_only 
	where event_name='DashboardView' 
	and user_data like '%%"role":"READER"%%'
),
reader_daily as
(
	select 
	date(event_day) as computation_Date
	, count(distinct user_id) as daily_active
	from  quicksight.v_raw_events_regionalized_external_only 
	where event_name='DashboardView' 
	and user_data like '%%"role":"READER"%%'
	and date(event_day) between '2018-11-05'-13 and date(getdate())
	group by 1
)
select to_char(d.computation_date,'yyyy-mm-dd') as date
, "active accounts (30 days)"
, "active users (30 days)" as "active authors (30 days)"
, count(distinct user_id) as "active readers (30 days)"
, "daily active accounts"
,"daily active users" as "daily active authors"
, coalesce(da.daily_Active,0) as "daily active readers"
from dates d
left join reader_actives r on r.computation_date between d.computation_date-29 and d.computation_date
left join reader_daily da on da.computation_date=d.computation_date
left join v_active_account_stats v on d.computation_date=v.calendar_date
group by d.computation_Date, 2, 3, 5, 6, 7
order by d.computation_date desc;



















with para_date as (
select date(getdate()) -2 as para_date
), v_regional_account_stats as (
SELECT 
	r.region_code_aws, 
	new_account_count.account_count, 
	new_user_count.user_count, 
	daily_actives.daily_active_users, 
	daily_actives.daily_active_accounts, 
	thirty_active.active_account AS thirty_account, 
	thirty_active.active_user AS thirty_user
FROM region_dm r

FULL JOIN 
	( 
	SELECT 
		derived_table1.computation_date, 
		derived_table1."region", 
		derived_table1.internal, 
		count(DISTINCT derived_table1.account_id) AS account_count
    FROM 
	 	( 
		SELECT 
			quicksight_account_daily.account_id, 
			quicksight_account_daily."region", 
			quicksight_account_daily.internal, 
			min(quicksight_account_daily.computation_date) AS computation_date
        FROM quicksight.quicksight_account_daily
        GROUP BY quicksight_account_daily.account_id, quicksight_account_daily.internal, quicksight_account_daily."region"
        HAVING min(quicksight_account_daily.computation_date) >= (( SELECT reporting_dates.dbr_start_date
                           FROM reporting_dates))
		) derived_table1
    WHERE derived_table1.internal::text = 'external'::character varying::text
    GROUP BY derived_table1.computation_date, derived_table1.internal, derived_table1."region"
	) new_account_count 
ON r.region_code_aws::text = new_account_count."region"::text

FULL JOIN 
	( 
	SELECT 
		derived_table1.computation_date, 
		derived_table1.internal, 
		derived_table1."region", 
		count(DISTINCT derived_table1.account_id::text || derived_table1.user_id::text) AS user_count
     FROM 
	 	(
		SELECT 
			a.account_id, 
			a.user_id, b.internal, 
			a."region", 
			min(date_trunc('day'::character varying::text, a.event_day)) AS computation_date
        FROM quicksight.v_raw_events_regionalized_external_only a
        JOIN bimetadata_rpt.account_details_dm b 
		ON a.account_id::text = b.account_id::text
        WHERE a.event_name::text = 'UserLogin'::character varying::text
        GROUP BY a.account_id, a.user_id, b.internal, a."region"
       	HAVING min(date_trunc('day'::character varying::text, a.event_day)) = (( SELECT reporting_dates.dbr_start_date
                 FROM reporting_dates))::timestamp without time zone
		) derived_table1
    GROUP BY derived_table1.computation_date, derived_table1.internal, derived_table1."region"
	) new_user_count 
ON r.region_code_aws::text = new_user_count."region"::text

FULL JOIN 
	( 
	SELECT 
		b.activity_day, 
		b."region", 
		b.daily_account_counts AS daily_active_accounts, 
		b.daily_user_counts AS daily_active_users
   	FROM 
		( 
		SELECT 
			date_trunc('day'::character varying::text, v_raw_events_regionalized_external_only.event_day) AS activity_day, 
			v_raw_events_regionalized_external_only."region", 
			count(DISTINCT v_raw_events_regionalized_external_only.account_id) AS daily_account_counts, 
			count(DISTINCT v_raw_events_regionalized_external_only.user_id) AS daily_user_counts
        FROM quicksight.v_raw_events_regionalized_external_only
        WHERE v_raw_events_regionalized_external_only.event_name::text = 'UserLogin'::character varying::text 
		AND date_trunc('day'::character varying::text, v_raw_events_regionalized_external_only.event_day) = (( SELECT reporting_dates.dbr_start_date
                   FROM reporting_dates))::timestamp without time zone
          
		GROUP BY date_trunc('day'::character varying::text, v_raw_events_regionalized_external_only.event_day), v_raw_events_regionalized_external_only."region"
		) b
	) daily_actives 
ON r.region_code_aws::text = daily_actives."region"::text
   
FULL JOIN 
	( 
	SELECT 
		v_raw_events_regionalized_external_only."region", 
		count(DISTINCT v_raw_events_regionalized_external_only.account_id) AS active_account, 
		count(DISTINCT v_raw_events_regionalized_external_only.user_id) AS active_user
   	FROM quicksight.v_raw_events_regionalized_external_only
  	WHERE date(v_raw_events_regionalized_external_only.event_day) >= ((( SELECT reporting_dates.dbr_start_date
           FROM reporting_dates)) - 29) 
	AND date(v_raw_events_regionalized_external_only.event_day) <= (( SELECT reporting_dates.dbr_start_date
           FROM reporting_dates)) 
	AND v_raw_events_regionalized_external_only.event_name::text = 'UserLogin'::character varying::text
  	GROUP BY v_raw_events_regionalized_external_only."region"
	) thirty_active 
ON r.region_code_aws::text = thirty_active."region"::text
WHERE r.region_code_aws::text = 'CMH'::text 
OR r.region_code_aws::text = 'DUB'::text 
OR r.region_code_aws::text = 'IAD'::text 
OR r.region_code_aws::text = 'PDX'::text 
OR r.region_code_aws::text = 'SIN'::text 
OR r.region_code_aws::text = 'SYD'::text 
OR r.region_code_aws::text = 'NRT'::text
), reader as
(
    select 
	region
	, count(distinct user_id) as daily_active
	from  quicksight.v_raw_events_regionalized_external_only 
	where event_name='DashboardView' 
	and user_data like '%%"role":"READER"%%'
	and date(event_day) = (select para_date from para_date)
	group by 1
),
thirty_reader as
(
	select 
	region
	, count(distinct user_id) as thirty_day
	from  quicksight.v_raw_events_regionalized_external_only 
	where event_name='DashboardView' 
	and user_data like '%%"role":"READER"%%'
	and date(event_day) between (select para_date from para_date) -29 and (select para_date from para_date) - 1
	group by 1
)
select region_code_aws as "Region"
, coalesce(account_count, 0) as "New Accounts"
, coalesce(user_count,0) as "New Authors"
, coalesce(daily_active_accounts,0) as "Active Accounts"
, coalesce(daily_active_users,0) as "Active Authors"
, coalesce(r.daily_active,0) as "Active Readers"
, coalesce(thirty_account,0) as "30-day Active Accounts"
, coalesce(thirty_user,0) as "30-day Active Authors"
, coalesce(t.thirty_day,0) as "30 day Active Readers"
from v_regional_account_stats a
left join reader r on a.region_code_aws=r.region
left join thirty_reader t on t.region=a.region_code_aws
order by 1;


