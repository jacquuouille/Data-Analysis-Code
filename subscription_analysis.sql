---------------
-- 0. DATA IMPORTATION 
---------------

--
-- 0.1. Creation of the table using the public dataset (https://mavenanalytics.io/data-playground?order=date_added%2Cdesc&page=5&pageSize=5)

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


--
-- 0.2. Insertion of the data using the .csv file


--
-- 0.3. Creation of the materialized views 
-- Here we're going to create materialized views in order to easily use them in the code for our analysis. 

--
-- 0.3.1. Subscribers Data Materialized View (using the created table by slighlty cleaning it and adding ranking activity dimensions)
-- View: public.subscribers_data

-- DROP MATERIALIZED VIEW IF EXISTS public.subscribers_data;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.subscribers_data
TABLESPACE pg_default
AS
 SELECT 
    streaming_data.customer_id,
    streaming_data.created_date,
	-- applying a date format to all observations
	CASE
	    WHEN to_date(streaming_data.canceled_date, 'YYYY-MM-DD'::text) <> '0001-01-01 BC'::date THEN to_date(streaming_data.canceled_date, 'YYYY-MM-DD'::text)
	    ELSE NULL::date
	END AS canceled_date,
    streaming_data.subscription_cost,
    streaming_data.subscription_interval,
    streaming_data.was_subscription_paid AS paid,
    row_number() OVER (PARTITION BY streaming_data.customer_id ORDER BY streaming_data.created_date DESC) AS last_activity_event,
    row_number() OVER (PARTITION BY streaming_data.customer_id ORDER BY streaming_data.created_date) AS first_activity_event,
    count(*) OVER (PARTITION BY streaming_data.customer_id) AS nb_activity_event
  FROM 
    streaming_data
WITH DATA;

ALTER TABLE IF EXISTS public.subscribers_data
    OWNER TO postgres;


--
-- 0.3.2. Fast-Churn Materialized View (Subscriber who has canceled the subscription within the same month they signed up and has not returned)
-- View: public.fast_churn

-- DROP MATERIALIZED VIEW IF EXISTS public.fast_churn;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.fast_churn
TABLESPACE pg_default
AS
 SELECT 
    DISTINCT 'fast_churned'::text AS category,
    subscribers_data.customer_id,
    subscribers_data.created_date,
    subscribers_data.canceled_date,
    subscribers_data.subscription_cost,
    subscribers_data.subscription_interval,
    subscribers_data.paid,
    subscribers_data.last_activity_event,
    subscribers_data.first_activity_event,
    subscribers_data.nb_activity_event
  FROM 
    subscribers_data
  WHERE 
    subscribers_data.nb_activity_event = 1 
    AND subscribers_data.last_activity_event = 1 
    AND subscribers_data.canceled_date IS NOT NULL 
    AND to_char(subscribers_data.created_date::timestamp with time zone, 'YYYY-MM'::text) = to_char(subscribers_data.canceled_date::timestamp with time zone, 'YYYY-MM'::text)
WITH DATA;

ALTER TABLE IF EXISTS public.fast_churn
    OWNER TO postgres;


--
-- 0.3.3. Churned Users Materialized View (Subscriber whose account has been canceled within a specific time frame and has not returned)
-- View: public.churned_users

-- DROP MATERIALIZED VIEW IF EXISTS public.churned_users;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.churned_users
TABLESPACE pg_default
AS
 SELECT 
    DISTINCT 'churned'::text AS category,
    t1.customer_id,
    t1.created_date,
    t1.canceled_date,
    t1.subscription_cost,
    t1.subscription_interval,
    t1.paid,
    t1.last_activity_event,
    t1.first_activity_event,
    t1.nb_activity_event
  FROM 
    subscribers_data t1
  LEFT JOIN 
    fast_churn t2 
    ON t1.customer_id = t2.customer_id
  WHERE 
    t1.last_activity_event = 1 
    AND t1.canceled_date IS NOT NULL 
    AND t2.customer_id IS NULL -- removing fast churn so we avoid duplicates
WITH DATA;

ALTER TABLE IF EXISTS public.churned_users
    OWNER TO postgres;


--
-- 0.3.4. New Users Materialized View (Subscriber who has just signed up for the subscription)
-- View: public.new_users

-- DROP MATERIALIZED VIEW IF EXISTS public.new_users;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.new_users
TABLESPACE pg_default
AS
 WITH 
 all_new_users AS (
         SELECT 
	    subscribers_data.customer_id,
            subscribers_data.created_date,
            subscribers_data.canceled_date,
            subscribers_data.subscription_cost,
            subscribers_data.subscription_interval,
            subscribers_data.paid,
            subscribers_data.last_activity_event,
            subscribers_data.first_activity_event,
            subscribers_data.nb_activity_event
          FROM 
	    subscribers_data
          WHERE 
	     subscribers_data.nb_activity_event = 1 
	     AND to_char(subscribers_data.created_date::timestamp with time zone, 'YYYY-MM'::text) = '2023-09'::text -- subscriber who has just signed up for the subscription in the last month of the data 
        )
 SELECT 
    DISTINCT 
    'new'::text AS category,
    t1.customer_id,
    t1.created_date,
    t1.canceled_date,
    t1.subscription_cost,
    t1.subscription_interval,
    t1.paid,
    t1.last_activity_event,
    t1.first_activity_event,
    t1.nb_activity_event
   FROM 
     all_new_users t1
   LEFT JOIN 
     fast_churn t2 
     ON t1.customer_id = t2.customer_id 
     AND to_char(t1.created_date::timestamp with time zone, 'YYYY-MM'::text) = to_char(t2.canceled_date::timestamp with time zone, 'YYYY-MM'::text)
  WHERE 
     t2.customer_id IS NULL -- removing fast churn so we avoid duplicates
WITH DATA;

ALTER TABLE IF EXISTS public.new_users
    OWNER TO postgres;


--
-- 0.3.5. Reccuring Users Materialized View (Subscriber who has remained continuously active with the subscription, without any interruptions)
-- View: public.reccuring_users

-- DROP MATERIALIZED VIEW IF EXISTS public.reccuring_users;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.reccuring_users
TABLESPACE pg_default
AS
 SELECT 
    DISTINCT 'reccuring'::text AS category,
    subscribers_data.customer_id,
    subscribers_data.created_date,
    subscribers_data.canceled_date,
    subscribers_data.subscription_cost,
    subscribers_data.subscription_interval,
    subscribers_data.paid,
    subscribers_data.last_activity_event,
    subscribers_data.first_activity_event,
    subscribers_data.nb_activity_event
   FROM 
    subscribers_data
  WHERE 
     subscribers_data.nb_activity_event = 1 
     AND subscribers_data.canceled_date IS NULL 
     AND to_char(subscribers_data.created_date::timestamp with time zone, 'YYYY-MM'::text) <> '2023-09'::text
WITH DATA;

ALTER TABLE IF EXISTS public.reccuring_users
    OWNER TO postgres;


--
-- 0.3.6. Recovered Users Materialized View (Subscriber who previously stopped engaging with the subscription, but has returned)
-- View: public.recovered_users

-- DROP MATERIALIZED VIEW IF EXISTS public.recovered_users;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.recovered_users
TABLESPACE pg_default
AS
 SELECT 
    DISTINCT 'recovered'::text AS category,
    subscribers_data.customer_id,
    subscribers_data.created_date,
    subscribers_data.canceled_date,
    subscribers_data.subscription_cost,
    subscribers_data.subscription_interval,
    subscribers_data.paid,
    subscribers_data.last_activity_event,
    subscribers_data.first_activity_event,
    subscribers_data.nb_activity_event
   FROM 
     subscribers_data
  WHERE 
     subscribers_data.nb_activity_event > 1 
     AND subscribers_data.last_activity_event = 1
     AND subscribers_data.canceled_date IS NULL
WITH DATA;

ALTER TABLE IF EXISTS public.recovered_users
    OWNER TO postgres;



----------------
-- 1. DATA ANALYSIS
----------------

--
-- 1.1. Accounts Breakdown (Subscriber-based (as of 2023-09)) 

with
subscriber_category as (
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


--
-- 1.2. Active Accounts Over Time (from the date subscribers joined for the first time, regardless of whether they were later recovered) 

with
first_touch as ( 
	select 
		customer_id
		, min(to_char(created_date, 'YYYY-MM')) as cohort_month
	from
		subscribers_data
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
			subscribers_data t1 
		left join 
			first_touch t2 
			on t1.customer_id = t2.customer_id 
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


--
-- 1.3. Subscriber Tenure Distribution (from the date subscribers stayed active for the last time (only the reactivation date of recovered accounts will be taken into account))

with
inactive_subscribers as ( 
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
		subscribers_data t1
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
		subscribers_data t1
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
		, coalesce(t2.last_touch_date, '2023-09-30') as last_touch_date -- using the last date of the data set as the last touch
		, extract(month from age(coalesce(t2.last_touch_date, '2023-09-30'), t1.first_touch_date)) as month_tenure 
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
	, sum(accounts) over(order by real_month_tenure rows between unbounded preceding and current row) as cumul_accounts
	, round(100.0*sum(accounts) over(order by real_month_tenure rows between unbounded preceding and current row) / sum(accounts) over(), 1) as prop_cumul_accounts
from (
	select 
		real_month_tenure
		, count(distinct customer_id) as accounts
	from 
		month_tenure 
	group by 
		1  
) a


--
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


--
-- 1.5. Payment Behaviours of Churned Accounts 

with
churned_all_together as ( 
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


--
-- 1.6. Cohort Analysis (including Fast-Churn)

with 
calendar_month as (
    -- generate a series of months for each subscriber based on their created_date to their canceled_date; using '2023-09-30' as canceled_date for active accounts as it's the last date we have in the dataset
    select
        	t1.customer_id
        	, generate_series(t1.created_date::date, coalesce(t1.canceled_date, '2023-09-30')::date,'1 month'::interval)::date as month_date 
    from 
        	subscribers_data t1 
) 
, cohort_buckets as ( 
	select 
		customer_id
		, cast(date_trunc('month', min(created_date)) as date) as cohort_month
	from
		subscribers_data
	group by
		1
)  
, churn_buckets as ( 
	select 
		customer_id 
		, max(canceled_date) as last_touch 
	from
		subscribers_data
	where 
		last_activity_event = 1
	group by
		1
) 
, user_details as ( 
	select 
		distinct t1.customer_id 
		, cast(date_trunc('month', t1.month_date) as date) as month_date
		, cast(date_trunc('month', t2.cohort_month) as date) as cohort_month
		, cast(date_trunc('month', t3.last_touch_real) as date) as last_month
		, case 
			when cast(date_trunc('month', t1.month_date) as date) = cast(date_trunc('month', t3.last_touch) as date) then 'churn'
			else 'active'
			end 
		  as user_type 
	from 
		calendar_month t1 
	left join 
		cohort_buckets t2 
		on t1.customer_id = t2.customer_id 
	left join 
		churn_buckets t3
		on t1.customer_id = t3.customer_id 
) 
, customer_activity as (
	select 
		t1.*
		-- as date_diff() function doesn't exist in PostgreSQL, we have to proceed diffently as below:
		, (extract(year from t1.month_date) - extract(year from t2.cohort_month)) * 12 -- calculates the difference in years * converts the year difference into months
		  + (extract(month from t1.month_date) - extract(month from t2.cohort_month)) as month_retained -- adds the difference in months.
	from 
		user_details t1
	join 
		cohort_buckets t2 
		on t1.customer_id = t2.customer_id 
	where 
		user_type != 'churn'
) 
, cohort_size as (
	select 
		cohort_month 
		, count(distinct customer_id) as nb_users
	from 
		cohort_buckets 
	group by 
		1
)
, retention as (
	select 
		t2.cohort_month 
		, t1.month_retained
		, count(distinct t1.customer_id) as nb_users 
	from 
		customer_activity t1 
	left join 
		cohort_buckets t2 
		on t1.customer_id = t2.customer_id
	group by 
		1, 2
)
select
	t1.cohort_month
	, t1.month_retained 
	, t2.nb_users as total_accounts
	, t1.nb_users as retained_accounts
	, t2.nb_users - t1.nb_users as churned_accounts
	, round(100.0 * t1.nb_users/t2.nb_users, 1) as retention_pct
from 
	retention t1 
left join 
	cohort_size t2
	on t1.cohort_month = t2.cohort_month 
order by 
	1, 3
