

select 
	computation_date, 
	sum(case when active_account then 1 end) as active_account, --matched
	sum(count_active_authors) as active_authors, -- NOT matched, user - reader 
	sum(count_active_readers) as active_readers, -- matched
	sum(count_new_authors) as new_authors
from quicksight.events_daily
where computation_date >= '2018-10-01' and internal = 'external'
group by 1
order by computation_date desc;



select 
	computation_date, 
	sum(gross_revenue) as gross_revenue, 
	sum(revenue) as revenue, --matched
	
	sum(annual_sub_revenue) as annual_sub_revenue, --matched
	sum(monthly_sub_revenue) as monthly_sub_revenue, --matched
	sum(reader_revenue) as reaader_revenue, --matched
	sum(spice_revenue) as spice_revenue, --matched
	
	
	sum(count_provisioned_free_users) as free_users, --matched
	sum(case when edition = 'standard' then count_provisioned_free_users end) as std_free, --almost matched
	sum(case when edition = 'enterprise' then count_provisioned_free_users end) as ent_free, --almost matched
	
	sum(count_provisioned_trial_users) as trial_users, --matched
	sum(case when edition = 'standard' then count_provisioned_trial_users end) as std_trial, --almost matched
	sum(case when edition = 'enterprise' then count_provisioned_trial_users end) as ent_trial, --almost matched
	
	sum(count_provisioned_author) as author, --matched
	sum(count_provisioned_paid_authors_with_annual_subscription) as annual_sub_author, --matched
	sum(count_provisioned_paid_authors_with_monthly_subscription) as monthly_sub_author, --matched
	
	sum(provisioned_readers) as reader_count, --matched
	sum(count_provisioned_reader_seesions) as reader_sessions, --matched
	sum(spice_gb) as spice_gb --matched
from quicksight.revenue_daily
where computation_date >= '2018-10-01' and internal = 'external'
group by 1
order by computation_date desc;





select 
	computation_date,
	min(l30_period_first_event_date),
	max(l30_period_last_event_date),
	--l7_period_start_date,
	sum(l30_count_active_accounts) as l30_active_account, --matched
	sum(l30_count_active_authors) as l30_active_authors, --matched
	sum(l30_count_active_reader) as l30_active_reader --matched
from quicksight.count_user_rolling_days
where computation_date >= '2018-10-01' and internal = 'external'
group by 1
order by computation_date desc;



select 
	computation_date, 
	region, 
	count(distinct case when computation_date = join_date then account_id end) as new_account, --wrong, min comp date in bimetadata_rpt.dbs_business_metrics_daily_account is different from quicksight.quicksight_account_daily
	sum(count_new_authors) as new_author, --matched
	count(distinct case when active_account then account_id end) as avtive_account, --matched
	sum(count_active_authors) as active_authors, --not matched since active author = active user - active reader 
	sum(count_active_readers) as active_readers, --matched
	sum(l30_count_active_accounts) as l30_active_account,
	sum(l30_count_active_authors) as l30_active_authors,
	sum(l30_count_active_reader) as l30_active_readers

from quicksight.events_regional_daily
where computation_date = date(getdate()) - 1 and internal = 'external'
group by 1,2;


--new account created
select 
	computation_date, 
	count(distinct case when computation_date = join_date then account_id end) as new_account, --matched
	sum(count_new_authors) as new_authors --matched
from quicksight.events_daily
where  computation_date >= '2018-10-01' and internal = 'external'
group by 1 order by computation_date desc;


--top 10 payer account by revenue
select 
	computation_date, 
	payer_account_id,  
	company_name, 
	sum(net_revenue) as revenue,--matched
	sum(count_provisioned_author) as author, --matched
	sum(spice_gb) as spice_gb --Not matched
from quicksight.revenue_daily 
where computation_date = '2018-11-15' and internal = 'external'
group by 1,2,3
--having sum(net_revenue) is not null
order by sum(net_revenue) desc nulls last
limit 10;




-- top 10 account by author count
select 
	account_id, 
	company_name,
	edition, 
	sum(count_provisioned_author) as author, --matched
	sum(provisioned_readers) as reader_count, --matched
	sum(net_revenue) as net_revenue, --matched
	sum(spice_gb) as spice_gb --almost matched different logic, provisioned spice vs enterprised spice
from quicksight.revenue_daily
where computation_date = '2018-11-15' and internal = 'external'
group by 1,2,3
having sum(count_provisioned_author) is not null
order by  sum(count_provisioned_author) desc
limit 10;



-- top 10 payer accounts by author count
select 
	payer_account_id, company_name, 
	
	--user is sum of author and reader
	sum(count_provisioned_author) as user, --matched
	sum(net_revenue) as net_revenue, --matched
	sum(spice_gb) as spice_gb --matched
from quicksight.revenue_daily
where computation_date = '2018-11-15' and internal = 'external'
group by 1,2
having sum(count_provisioned_author) is not null
order by  sum(count_provisioned_author) desc
limit 10;



--top 10 analysis created
select account_id, company_name, 
sum(count_analysis_created) --matched
from quicksight.events_daily
where computation_date = '2018-11-15' and internal = 'external'
group by 1, 2
order by sum(count_analysis_created) desc nulls last
limit 10;


--top 10 active reader count
select account_id, company_name, 
	sum(count_active_readers) as active_readers --difference due no dashboard view here 
from quicksight.events_daily
where computation_date = '2018-11-15' and internal = 'external'
group by 1,2
order by sum(count_active_readers) desc nulls last
limit 10
;








