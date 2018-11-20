delete quicksight.account_type_edition_daily
where account_id in
    (
    select account_id
		from bimetadata_rpt.account_details_dm
		where account_status_code = 'Suspended'
    or fraud_enforcement_status like '%Suspended'
    or fraud_enforcement_status like '%Terminated'
    );


update quicksight.events_daily
set count_new_authors = t1.count_new_authors, dw_update_date = getdate()
from
     (
      select
        events.first_login_date,
        events.account_id,
        count(distinct events.user_id) as count_new_authors
      from
      (
        select
          account_id,
          user_id,
          min(date(event_day)) as first_login_date
        from quicksight.v_raw_events_regionalized
        where event_name = 'UserLogin'
        group by 1,2
      ) as events
      left join
      (
        select
          account_id,
          user_id,
          date(event_day) as dashboard_view_date
        from quicksight.v_raw_events_regionalized
        where user_data ilike '%%"role":"READER"%%' and event_name = 'DashboardView' group by 1,2,3
      ) as exclude_reader
      on events.account_id = exclude_reader.account_id
      and events.first_login_date = exclude_reader.dashboard_view_date
      and events.user_id = exclude_reader.user_id
      where exclude_reader.user_id is null
      group by 1,2
         ) as t1
where events_daily.computation_date = t1.first_login_date
and events_daily.account_id = t1.account_id;


update quicksight.events_regional_daily
set count_new_authors = t1.count_new_authors
, l30_count_active_authors = l30_count_active_authors - l30_count_active_reader
, l7_count_active_authors = l7_count_active_authors - l7_count_active_reader
, dw_update_date = getdate()
from
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
    ) as t1
where events_regional_daily.computation_date = t1.first_login_date
and events_regional_daily.account_id = t1.account_id
and events_regional_daily.region = t1.region;



update quicksight.count_user_rolling_days
set l30_count_active_authors = l30_count_active_authors - l30_count_active_reader
, l7_count_active_authors = l7_count_active_authors - l7_count_active_reader
,dw_update_date = getdate()
;