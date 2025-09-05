-- Sentiment Analysis - SQL Code (PostgreSQL)
-- Summary: 
   -- 1. Creating the master table for customer experience  
   -- 2. Creating the master table for customer sentiment 



---------------
-- 1. CUSTOMER EXPERIENCE MASTER TABLE  
---------------

CREATE MATERIALIZED VIEW customer_reviews
AS
  with 
  split_reviews as (
  	select 
  		* 
      -- splitting the array in multiple parts using the separator ',<p', and turning them into a set of rows  
  		, unnest(string_to_array(raw_review_text, ',<p')) as part 
  	from 
  		hotel_reviews 
  
  )
  , extracted_reviews as ( 
  	select 
  		* 
      -- extracts specific attributes from the part text.
  		, regexp_matches(part, '<svg[^>]*aria-label="([^"]+)"', 'g') as sentiment
  	 	, regexp_matches(part, '<span itemprop="reviewBody">([^<]+)</span>', 'g') AS sentiment_review
  	from 
  		split_reviews
  ) 
  
  select 
  	index 
  	, cast(reviewed_at as date) as review_date
  	, reviewed_by as reviewer
  	, nationality
  	, hotel_name
  	, review_title 
  	, rating
  	, review_text
  	, regexp_replace(array_to_string(sentiment, ' '), '[\"{}]', 'g') as sentiment
  	, regexp_replace(array_to_string(sentiment_review, ' '), '[\"{}]', 'g') as sentiment_review
  from 
  	extracted_reviews
  order by 
  	1, 2
WITH NO DATA;



---------------
-- 2. CUSTOMER SENTIMENT MASTER TABLE  
---------------
-- Using the 'customer_sentiment' dataset created in Python (see "sentiment_analysis.py" Python script)

CREATE MATERIALIZED VIEW sentiment_analysis
AS
  select 
  	index 
  	, review_date_clean as review_date 
  	, reviewer
  	, nationality 
  	, hotel_name 
  	, review_title 
  	, rating 
  	, review_text 
  	, tb_polarity 
  	, tb_subjectivity 
  	, vader_compound
  	, vader_pos 
  	, vader_neg 
  	, vader_neu 
  from ( 
  	select 
  		* 
  		, row_number() over(partition by index) as counter 
  		, to_date(review_date, 'YYYY-MM-DD') as review_date_clean
  	from
  		customer_sentiment 
  	where 
  		review_date ~ '^\d{4}-\d{2}-\d{2}$' -- ensures the string is exactly in YYYY-MM-DD format
  ) a
  where 
  	counter = 1
WITH NO DATA;
