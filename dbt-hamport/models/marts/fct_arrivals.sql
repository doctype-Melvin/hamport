{{ config(materialized='table')}}

with staging as (
    select * from {{ref('stg_arrivals')}}
),

final as (
    select
        md5(concat(flight_id, planned_time::text)) as arrival_key, -- unique identifier
        flight_id,
        cancelled,
        airline,
        origin_airport,
        planned_time,
        actual_time,
        delay_minutes,
        flight_status,

        -- enrichment from temporal data
        extract(hour from planned_time) as planned_hour,
        to_char(planned_time, 'Day') as day_of_week,

        case
            when flight_status = 'Data Stale' then 'Data Stale'
            when delay_minutes > 15 then 'Late'
            when delay_minutes > 0 then 'Minor Delay'
            when cancelled then 'Cancelled'
            else 'On Time'
        end as punctual

    from staging
)

select * from final