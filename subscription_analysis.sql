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

-- 1.1. Accounts Breakdown (Subscriber-based (as of 2023-09))
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


-- 1.2. Active Accounts Over Time (from the date subscribers joined for the first time, regardless of whether they were later recovered)
-- using the temporary table 'data_prep' created in the previous query (1.1. Accounts Breakdown) 

(...)
, first_touch as ( 
	select 
		customer_id
		, min(to_char(created_date, 'YYYY-MM')) as cohort_month
	from
		data_prep
	group by
		1
)
, subscribers_based as (
	select 
		cohort_month as period
		, count(distinct customer_id) as subscribers_based
	from 
		first_touch
	group by 
		1
	order by 
		1
)
, subscribers_active as ( 
	select 
		cohort_month as period
		, count(distinct customer_id) as accounts
	from ( 
		select 
			t1.*
			, t2.cohort_month 
		from
			data_prep t1 
		left join 
			first_touch t2 
			on t1.customer_id = t2.customer_id
		-- where t1.customer_id = '116060198' 
	) a
	where 
		last_activity_event = 1 
		and canceled_date is null
	group by 
		1
)

select 
	distinct t1.period 
	, t1.accounts
	, t2.subscribers_based
from 
	subscribers_active t1 
join 
	subscribers_based t2 
	on t1.period = t2.period 
order by 
	1

	
-- 1.3. Subscriber Tenure Distribution (from the date subscribers stayed active for the last time (only the reactivation date of recovered accounts will be taken into account))
-- using the temporary tables 'data_prep', 'fast_churn', 'churned_users', 'new_users', 'reccuring_users' and 'recovered_users' created in the really first query (1.1. Accounts Breakdown)

(...)
, inactive_subscribers as ( 
	select 'inactive' as main_category, category, customer_id from fast_churn
	union 
	select 'inactive' as main_category, category, customer_id from churned_users 
)
, active_subscribers as (
	select 'active' as main_category, category, customer_id from new_users 
	union 
	select 'active' as main_category, category, customer_id from reccuring_users 
	union 
	select 'active' as main_category, category, customer_id from recovered_users
)
, all_together as (
	select * from inactive_subscribers
	union 
	select * from active_subscribers
) 
, first_touch as (
	select 
		t1.customer_id
		-- for recovered accounts, we need to take into account the date they subscribed again (tenure is from their last active subscription, and not from when they subscribed to the plan for the first time)
		, case 
			when t2.category = 'recovered' then t3.created_date else t1.created_date  
		  end as first_touch_date
		, t2.category
		, t2.main_category
	from 
		data_prep t1
	join 
		all_together t2
		on t1.customer_id = t2.customer_id
	left join 
		recovered_users t3
		on t1.customer_id = t3.customer_id
	where 
		t1.first_activity_event = 1
) 
, last_touch as (
	select 
		t1.customer_id
		, t1.canceled_date as last_touch_date 
		, t2.category
		, t2.main_category
	from 
		data_prep t1
	join 
		all_together t2
		on t1.customer_id = t2.customer_id
	where 
		t1.last_activity_event = 1
) 
, subscribers_tenure as ( 
	select 
		t1.customer_id 
		, t1.main_category
		, t1.category
		, t1.first_touch_date
		, coalesce(t2.last_touch_date, '2023-09-08') as last_touch_date
		, extract(month from age(coalesce(t2.last_touch_date, '2023-09-08'), t1.first_touch_date)) as month_tenure 
	from 
		first_touch t1 
	join 
		last_touch t2 
		on t1.customer_id = t2.customer_id 
) 
, month_tenure as ( 
-- As we assume that subscriptions always starts from the 1st day of the month and end on the last day of that same month, with renewals occuring on the 1st day of the following month, we need to change the 'month_tenure' value from 0 to 1 for reccuring accounts and subscribers who canceled in the month after signing-up.
-- e.g., reccuring subscriber '116156335' signing-up August 14, 2023 
-- e.g., churned   subscriber '123169152' signing-up December 10, 2022 and cancelling on January 1, 2023 
	select 
		* 
		, case 
			when month_tenure = 0 and to_char(first_touch_date, 'YYYY-MM') != to_char(last_touch_date, 'YYYY-MM') then 1
			else month_tenure 
			end 
		as real_month_tenure 
	from 
		subscribers_tenure
) 
select 
	distinct *
	, sum(accounts) over(order by month_tenure_bis rows between unbounded preceding and current row) as cumul_accounts
	, round(100.0*sum(accounts) over(order by month_tenure_bis rows between unbounded preceding and current row) / sum(accounts) over(), 1) as prop_cumul_accounts
from (
	select 
		real_month_tenure
		, count(distinct customer_id) as accounts
	from 
		month_tenure 
	group by 
		1  
) a


-- 1.4. Subscriber Tenure Trends by Engagement Status (from the date subscribers stayed active for the last time (only the reactivation date of recovered accounts will be taken into account)
-- using the final temporary table 'month_tenure' created in the previous query (1.3. Subscriber Tenure Distribution)

(...)
select 
	distinct *
	, sum(accounts) over(partition by main_category order by month_tenure_bis rows between unbounded preceding and current row) as cumul_accounts
	, round(100.0*sum(accounts) over(partition by main_category order by month_tenure_bis rows between unbounded preceding and current row) / sum(accounts) over(partition by main_category), 1) as prop_cumul_accounts
from (
	select 
		main_category
		, month_tenure_bis
		, count(distinct customer_id) as accounts 
	from 
		month_tenure 
	group by 
		1, 2
) a


-- 1.5. Cancelled Subscriptions Over Time (% of the running total of created subscriptions) 
-- using the final temporary table 'data_prep' created in the really first query (1.1. Accounts Breakdown)

(...)
, churned_subcriptions as ( 
	select 
		to_char(canceled_date, 'YYYY-MM') as period 
		, count(*) as churned_subscriptions 
	from
		data_prep 
	group by 
		1 
)
, created_subscriptions as ( 
	select 
		to_char(created_date, 'YYYY-MM') as period 
		, count(*) as created_subscriptions 
		, sum(count(*)) over(order by to_char(created_date, 'YYYY-MM') rows between unbounded preceding and current row) as running_total_created_subscriptions
	from
		data_prep 
	group by 
		1
) 

select 
	distinct t1.period 
	, t1.churned_subscriptions
	, t2.running_total_created_subscriptions 
	, round(100.0*t1.churned_subscriptions / t2.running_total_created_subscriptions, 1) as prop_churned_subscriptions
from 
	churned_subcriptions t1 
join 
	created_subscriptions t2 
	on t1.period = t2.period 
order by 
	1


-- 1.6. Payment Behaviours of Churned Accounts 
-- using the final temporary tables 'data_prep', 'fast_churn' and 'churned_users' created in the really first query (1.1. Accounts Breakdown)

(...)
, churned_all_together as ( 
	select * from fast_churned 
	union 
	select * from churned_users  
) 
select 
	distinct *
	, round(100.0*accounts / sum(accounts) over(partition by category), 2) as accounts_cat 
from ( 
	select 
		category
		, paid
		, count(distinct customer_id) as accounts
	from 
		churned_all_together
	group by
		1, 2 
) a
order by 
	1
-- Nearly all subscribers who have churned have paid their subscription, wheareas more than 30% of fast-churned did not, suggesting that their lack of payment could be the reason for their cancellation	
	
	
