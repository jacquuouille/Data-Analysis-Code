-- Booking Analysis - SQL Code 
-- Summary: 
   -- 0. Creating the master table
   -- 1. Data Analysis 
    -- 1.1. Bookings Statistics per Ticket Type
    -- 1.2. Bookings per Ticket Type over time 
    -- 1.3. Average Ticket Price over time: is that a promotion period?  
    -- 1.4. Off-Peak Bookings over time 
    -- 1.5. Distribution of Tickets Booked by Purchase Type
    -- 1.6. Distribution of bookings per days in advance
    -- 1.7. Top departure station
    -- 1.8. Top arrival station
    -- 1.9. Heatmap of the Average Daily Bookings by Day of Purchase and Time of Purchase
    -- 1.10. Distribution of the number of Delayed Reasons per Journey Status
    -- 1.11. Distribution of the number of delayed trains per Delay Duration
    -- 1.12. Distribution of the number of Refunded Tickets per Journey Status
    -- 1.13. Distribution of the number of Bookings per Journey Status 


---------------
-- 0. THE MASTER TABLE
---------------

--
-- 0. Creating the master table, from the railway data table (imported via the Python script), and 1 external dataset 'city_arrival' (geolocalisation points details). 

CREATE MATERIALIZED VIEW railway_tickets
AS
with 
departure_cities as ( 
	select 
		distinct departure_station  
		, case 
			when departure_station like '%Birmingham%' then 'Birmingham'
			when departure_station like '%Bristol%' then 'Bristol'
			when departure_station like '%Edinburgh%' then 'Edinburgh'
			when departure_station like '%Liverpool%' then 'Liverpool'
			when departure_station like '%London%' then 'London'
			when departure_station like '%Manchester%' then 'Manchester'
			when departure_station like '%Reading%' then 'Reading'
			else departure_station 
			end as city_departure
	from 
		railway 
)
, arrival_cities as ( 
	select 
		distinct arrival_destination  
		, case 
			when arrival_destination like '%Birmingham%' then 'Birmingham'
			when arrival_destination like '%Bristol%' then 'Bristol' 
			when arrival_destination like '%Cardiff%' then 'Cardiff'  
			when arrival_destination like '%Edinburgh%' then 'Edinburgh'
			when arrival_destination like '%Liverpool%' then 'Liverpool'
			when arrival_destination like '%London%' then 'London'
			when arrival_destination like '%Manchester%' then 'Manchester' 
			when arrival_destination like '%Reading%' then 'Reading'
			else arrival_destination 
			end as city_arrival
	from
		railway 
)	
, departure_city_geo as ( 
	select 
		distinct t1.departure_station
		, t1.city_departure
		, t2.latitude as latitude_departure
		, t2.longitude as longitude_departure
	from 
		departure_cities t1 
	left join 
		city_dataset t2 
		on t1.city_departure = t2.city
)
, arrival_city_geo as ( 
	select 
		distinct t1.arrival_destination
		, t1.city_arrival
		, t2.latitude as latitude_arrival
		, t2.longitude as longitude_arrival
	from 
		arrival_cities t1 
	left join 
		city_dataset t2 
		on t1.city_arrival = t2.city
)
select 
	t1.transaction_id
	, t1.purchase_date
	, t1.purchase_time
	, t1.purchase_type
	, t1.payment_method
	, t1.railcard
	, t1.ticket_class
	, t1.ticket_type
	, t1.price
	, t1.departure_station
	, t2.city_departure
	, t2.latitude_departure
	, t2.longitude_departure
	, t1.arrival_destination
	, t3.city_arrival
	, t3.latitude_arrival
	, t3.longitude_arrival
	, t1.journey_date 
	, t1.departure_time
	, t1.arrival_time 
	, t1.actual_arrival_time
	, t1.journey_status 
	, t1.delay_reason 
	, t1.refund_request 
from 
	railway t1
left join 
	departure_city_geo t2 
	on t1.departure_station = t2.departure_station
left join 
	arrival_city_geo t3 
	on t1.arrival_destination = t3.arrival_destination
order by 
	1
WITH NO DATA;

REFRESH MATERIALIZED VIEW railway_tickets


----------------
-- 1. DATA ANALYSIS
----------------

----
-- 1.1. Bookings Statistics per Ticket Type
with 
bookings as ( 
	select 
		ticket_type    
		, count(distinct transaction_id) as bookings  -- 1 row = 1 booking (count(*) = count(distinct transaction_id))
		, count(distinct transaction_id) / count(distinct purchase_date) as avg_daily_bookings
	from
		railway_bookings
	group by  
		1
)
select 
	* 
	, round(
		100.0*sum(bookings) over(partition by ticket_type) / sum(bookings) over(), 1
	  ) as prop_bookings
from 
	bookings 
order by 
	4 desc
-- More than 50% of all bookings are for Advance Tickets (55,5%), while nearly 30% of tickets booked are for Off-Peak. Anytime tickets are primarily purchased by customers without Railcard; in other words, which means that the majority of ticketd (83.1%) have been bought by Railcard holders, copmpared to just 16.9% by non-holders.
-- Each day, on average 138 Advance tickets are being booked, 72 Off-Set ones and 58 Anything ones.


----
-- 1.2. Bookings per Ticket Type over time 
select 
	ticket_type   
	, purchase_date
	, sum(count(transaction_id)) over(partition by ticket_type, purchase_date) as bookings -- 1 row = 1 booking (count(*) = count(distinct transaction_id))
	, sum(count(transaction_id)) over(order by purchase_date rows between unbounded preceding and current row) as running_bookings
from
	railway_bookings
group by 
	1, 2 
order by 
	1, 2
-- a. There appears to be a peak in Advance Tickets bookings In February 2024, suggesting at a glance a potential promotion period for this ticket type.
-- Interestingly, bookings keeps decreasing over the month, while the overall number of trips booked within this period for the following month remains stable.
-- Since the average ticket price had remained consistent during this time, we may assume that the goal of such of a promotion is to encourage earlier booking 
-- (see ad-hoc analysis)	

-- b. It looks like the booking of Off-Peak tickets are seasonal 


----
-- 1.3. Average Ticket Price over time: is that a promotion period?  
select 
	purchase_date 
	, round(
		avg(price), 1
	  ) as avg_price
from
	railway_bookings
group by 
	1 
order by 
	1 
-- It looks like the average ticket price has slightly decreased during that period, which may be due to lower bookings in that time.
-- Was it a promotional period for non-holders of the National Card?


----
-- 1.4. Off-Peak Bookings over time 
select 
	purchase_date 
	, date_part('isodow', purchase_date) as date -- week starting Monday ('1' = Monday)
	, count(distinct transaction_id)
from
	railway_bookings
where 
	ticket_type = 'Off-Peak'
group by 
	1, 2
order by 
	1, 2
-- It looks like the booking of Off-Peak tickets are seasonal, with high volume of tickets booked on weekends (Friday, Saturday, Sunday)  
-- There is a peak in Off-Peak ticket 2024-03-23, looks pretty isolated phenomenon 
 

----
-- 1.5. Distribution of Tickets Booked by Purchase Type
select 
	distinct purchase_type    
	, count(transaction_id) over(partition by purchase_type) as revenue
	, round(
		100.0*count(transaction_id) over(partition by purchase_type) / count(transaction_id) over(), 1
	  ) as prop_revenue
from
	railway_bookings 
order by 
	2 desc
-- Most of the tickets are being booked online (59%) than at the station (41%).  


-- 
-- 1.6. Distribution of bookings per days in advance
with 
booking_metrics as ( 
	select 
		distinct days_in_advance 
		, round(
			avg(price) over(partition by days_in_advance), 1
		  ) as avg_price
		, count(transaction_id) over(partition by days_in_advance) as bookings -- 1 row = 1 booking (count(*) = count(distinct transaction_id))
		, round(
			100.0*count(transaction_id) over(partition by days_in_advance) / count(transaction_id) over(), 1
		  ) as prop_bookings
	from ( 
		select 
			* 
			, journey_date - purchase_date as days_in_advance
		from
			railway_bookings
	) a 
)
select 
	*
	, sum(prop_bookings) over(order by days_in_advance rows between unbounded preceding and current row) as running_prop_bookings
from 
	booking_metrics 
order by 
	1
-- Almost 9 tickets out of 10 have being booked 1 day before departure or on the same day


--
-- 1.7. Top departure station
select 
	departure_station
	, count(distinct transaction_id) as bookings -- 1 row = 1 booking (count(*) = count(distinct transaction_id))
	, count(distinct transaction_id) / count(distinct purchase_date) as avg_daily_bookings 
from
	railway_bookings
group by 
	1 
order by 
	3 desc
limit 5


-- 
-- 1.8. Top arrival station
select 
	arrival_destination
	, count(distinct transaction_id) as bookings -- 1 row = 1 booking (count(*) = count(distinct transaction_id))
	, count(distinct transaction_id) / count(distinct purchase_date) as avg_daily_bookings 
from
	railway_bookings
group by 
	1 
order by 
	3 desc
limit 5


--
-- 1.9. Heatmap of the Average Daily Bookings by Day of Purchase and Time of Purchase
-- creating pivot table
select 
	hour_day
	, round(coalesce(avg(case when week_day = 1 then bookings else null end), 0), 0) as "1"
	, round(coalesce(avg(case when week_day = 2 then bookings else null end), 0), 0) as "2"
	, round(coalesce(avg(case when week_day = 3 then bookings else null end), 0), 0) as "3"
	, round(coalesce(avg(case when week_day = 4 then bookings else null end), 0), 0) as "4"
	, round(coalesce(avg(case when week_day = 5 then bookings else null end), 0), 0) as "5"
	, round(coalesce(avg(case when week_day = 6 then bookings else null end), 0), 0) as "6"
	, round(coalesce(avg(case when week_day = 7 then bookings else null end), 0), 0) as "7"
from (  
	select 
		purchase_date 
		, extract(hour from purchase_time) as hour_day
		, date_part('isodow', purchase_date) as week_day -- week starting Monday ('1' = Monday)
		, count(distinct transaction_id) as bookings
	from
		railway_bookings 
    -- e.g., with Anytime tickets
    where 
		ticket_type = 'Anytime'
	group by 
		1, 2, 3  
) a 
group by 
	1
order by 
	1 
-- The booking pattern differs according to the ticket type:
-- a. Advance Tickets: tickets mostly purchased during the morning between 8am and 9am, as well as the end of the day between 5pm and 8pm, regardless it's during the week or weekend
-- b. Off-Peaks: tickets mostly got purchased during the weekend, early in the morning (5am -7am), and in the afternoon (2pm-6pm)
-- c. Anytime Tickets: barely booked on weekends, probably for works during office hours


-- 
-- 1.10. Distribution of the number of Delayed Reasons per Journey Status
select 
	distinct journey_status
	, delay_reason 
	, count(*) over(partition by journey_status, delay_reason) as freq -- 1 row = 1 booking (count(*) = count(distinct transaction_id))
	, round(
		100.0*count(*) over(partition by journey_status, delay_reason) / count(*) over(partition by journey_status), 1
	  ) as prop_reason
from
	railway_bookings 
where
	journey_status != 'On Time'
order by 
	1, 4 desc


-- 
-- 1.11. Distribution of the number of delayed trains per Delay Duration
with 
delay_categories as ( 
	select 
		case 
			when delayed between '00:00:00' and '00:29:00' then '1. less than 30min'
			when delayed between '00:30:00' and '00:59:00' then '2. less than 1h'
			when delayed between '01:00:00' and '01:59:00' then '3. less than 2h'
			when delayed > '02:00:00' then '4. more than 2h'
			else null 
			end as delay_cat
		, count(*) as delayed_trains 
	from ( 
		-- Assuming that delays are happening on the same day
		select 
			distinct transaction_id  
			, arrival_time 
			, actual_arrival_time
			, actual_arrival_time - arrival_time as delayed
		from
			railway_bookings 
		where
			journey_status = 'Delayed' -- no delay for om-time tickets; tickets that have been cancelled don't have any actual arrival time
	) a
	group by 
		1
) 
select 
	distinct * 
	, round(
		100.0*sum(delayed_trains) over(partition by delay_cat) / sum(delayed_trains) over(), 1
	  ) as prop
from 
	delay_categories
order by 
	1


-- 
-- 1.12. Distribution of the number of Refunded Tickets per Journey Status
select 
	distinct journey_status 
	, refund_request 
	, count(*) over(partition by journey_status, refund_request) as freq
	, round(
		100.0*count(*) over(partition by journey_status, refund_request) / count(*) over(partition by journey_status), 1
	  ) as prop
from
	railway_bookings
where 
	journey_status != 'On Time' 
order by 
	1, 2
-- 30% of cancelled tickets have been requested by the passenger and refunded to him, less than 25% for the delayed tickets


-- 
-- 1.13. Distribution of the number of Bookings per Journey Status 
select 
	journey_date 
	, count(distinct transaction_id) as bookings -- 1 row = 1 booking (count(*) = count(distinct transaction_id))
        , count(distinct case when journey_status = 'On Time' then transaction_id else null end) as ontime_journeys
	, count(distinct case when journey_status = 'Delayed' then transaction_id else null end) as delayed_journeys
	, count(distinct case when journey_status = 'Cancelled' then transaction_id else null end) as cancelled_journeys
from
	railway_bookings 
group by 
	1
order by 
	1
