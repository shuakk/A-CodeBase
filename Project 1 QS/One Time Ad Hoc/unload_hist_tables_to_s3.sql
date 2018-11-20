unload ('select * from quicksight.account_type_edition_daily') 
to 's3://db_services.aws.amazon.com/quicksight/data_model/account_type_edition_daily_pull/Backfill/' 
iam_role 'arn:aws:iam::015982553965:role/mySpectrumRole'
delimiter '|' ALLOWOVERWRITE ADDQUOTES ESCAPE gzip null as ''; 


unload ('select * from quicksight.events_daily') 
to 's3://db_services.aws.amazon.com/quicksight/data_model/events_daily_pull/Backfill/' 
iam_role 'arn:aws:iam::015982553965:role/mySpectrumRole'
delimiter '|' ALLOWOVERWRITE ADDQUOTES ESCAPE gzip null as ''; 



unload ('select * from quicksight.revenue_daily') 
to 's3://db_services.aws.amazon.com/quicksight/data_model/revenue_daily_pull/Backfill/' 
iam_role 'arn:aws:iam::015982553965:role/mySpectrumRole'
delimiter '|' ALLOWOVERWRITE ADDQUOTES ESCAPE gzip null as ''; 


unload ('select * from quicksight.count_user_rolling_days') 
to 's3://db_services.aws.amazon.com/quicksight/data_model/user_rolling_period_pull/Backfill/' 
iam_role 'arn:aws:iam::015982553965:role/mySpectrumRole'
delimiter '|' ALLOWOVERWRITE ADDQUOTES ESCAPE gzip null as ''; 


unload ('select * from quicksight.events_regional_daily') 
to 's3://db_services.aws.amazon.com/quicksight/data_model/events_regional_daily_pull/Backfill/' 
iam_role 'arn:aws:iam::015982553965:role/mySpectrumRole'
delimiter '|' ALLOWOVERWRITE ADDQUOTES ESCAPE gzip null as ''; 













