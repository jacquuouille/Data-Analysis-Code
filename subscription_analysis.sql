---------------
-- 0. DATA IMPORTATION 
---------------

-- 0.1. Creation of the table 

CREATE TABLE IF NOT EXISTS public.streaming_data
(
    customer_id text COLLATE pg_catalog."default" NOT NULL,
    created_date date NOT NULL,
    canceled_date text COLLATE pg_catalog."default",
    subscription_cost integer,
    subscription_interval text COLLATE pg_catalog."default",
    was_subscription_paid text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.streaming_data
    OWNER to postgres;


-- 0.2. Insertion of the data using the .csv file


----------------
-- 1. DATA ANALYSIS
----------------

-- 1.1. Distribution of Subscribers per Category
-- after quickly cleaning the data (step 0), we're going to create a category dimension refering to the status of each subscriber based on their last status in the program (1). finally, we'll gather all the created categories and aggregate them by counting the number of subscribers in each (2).
-- (see glossary for categories' definition)

-- (0)
with
data_prep as ( 
	select
		customer_id 
		, created_date
		, case 
			when to_date(canceled_date, 'YYYY-MM-DD') != '0001-01-01 BC' then to_date(canceled_date, 'YYYY-MM-DD') -- applying a date format to all observations
			else null
			end 
		as canceled_date 
		, subscription_cost
		, subscription_interval 
		, was_subscription_paid as paid
		, row_number() over(partition by customer_id order by created_date desc) as last_activity_event
		, row_number() over(partition by customer_id order by created_date) as first_activity_event
		, count(*) over(partition by customer_id) as nb_activity_event
	from 
		streaming_data
)
-- (1)
, fast_churn as ( 
	select 
		distinct 'fast_churned' as category
		, *
	from
		data_prep 
	where
		nb_activity_event = 1 
		and last_activity_event = 1
		and canceled_date is not null 
		and to_char(created_date, 'YYYY-MM') = to_char(canceled_date, 'YYYY-MM') 
)  
, churned_users as (
	select 
		distinct 'churned' as category
		, t1.*  
	from 
		data_prep t1 
	left join 
		fast_churn t2 
		on t1.customer_id = t2.customer_id
	where 
		t1.last_activity_event = 1 
		and t1.canceled_date is not null
		and t2.customer_id is null -- removing fast churn so we avoid duplicates
)  
, all_new_users as ( 
	select 
		*
	from 
		data_prep
	where 
		nb_activity_event = 1  
		and to_char(created_date, 'YYYY-MM') = '2023-09' -- subscriber who has just signed up for the subscription in the last month of the data 
)  
, new_users as ( -- removing fast_churned
	select
		distinct 'new' as category
		, t1.* 
	from 
		all_new_users t1 
	left join 
		fast_churn t2 
		on t1.customer_id = t2.customer_id
		and to_char(t1.created_date, 'YYYY-MM') = to_char(t2.canceled_date, 'YYYY-MM')
	where 
		t2.customer_id is null -- removing fast churn so we avoid duplicates
)  
, reccuring_users as (
	select
		distinct 'reccuring' as category
		, *  
	from
		data_prep 
	where
		nb_activity_event = 1
		and canceled_date is null 
		and to_char(created_date, 'YYYY-MM') != '2023-09' -- otherwise is null 
) 
, recovered_users as (
	select
		distinct 'recovered' as category
		, *  
	from 
		data_prep 
	where 
		nb_activity_event > 1 
		and last_activity_event = 1 
		and canceled_date is null  
)  
-- (2)
, subscriber_category as (
	select * from new_users 
	union 
	select * from reccuring_users 
	union 
	select * from recovered_users
	union
	select * from fast_churn 
	union 
	select * from churned_users 
	union 
	select * from recovered_users
)
select 
	category 
	, count(distinct customer_id) as accounts
from 
	subscriber_category
group by 
	1 
order by 
	1 desc


-- 1.2. Active Accounts Over Time  
-- using the temporary tables 'data_prep', 'new_users', 'reccuring_users' and 'recovered_users' created in the previous query

(...)
, active_subscribers as (
	select * from new_users 
	union 
	select * from reccuring_users 
	union 
	select * from recovered_users
)
select 
	to_char(t1.created_date, 'YYYY-MM') as period 
	, count(distinct t1.customer_id) as subscribers_joined
	, count(distinct t2.customer_id) as subscribers_joined_active
from 
	data_prep t1 
join 
	active_subscribers t2 
	on to_char(t1.created_date, 'YYYY-MM') = to_char(t2.created_date, 'YYYY-MM')
group by 
	1 
order by 
	1 
