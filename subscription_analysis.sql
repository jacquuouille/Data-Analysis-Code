/* DATA IMPORTATION */

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
