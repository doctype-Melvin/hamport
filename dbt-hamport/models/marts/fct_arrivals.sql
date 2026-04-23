{{ config(materialized='table')}}

with staging as (
    select * from {{ref('stg_arrivals')}}
)

select
    md5(concat(flight_id, planned_time::text)) as arrival_key, -- unique identifier
    flight_id,
    airline,
    origin_airport,
    planned_time,
    actual_time,
    delay_minutes,
    flight_status,

    -- enrichment from temporal data
    extract(hour from planned_time) as planned_hour,
    to_char(planned_time, 'Day') as day_of_week

from staging