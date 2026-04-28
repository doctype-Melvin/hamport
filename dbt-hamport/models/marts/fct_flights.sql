{{
    config(
        materialized='incremental',
        unique_key='flight_pk',
        on_schema_change='append_new_columns'
    )
}}

with arrivals as (
    select
        *,
        'Arrival' as direction
    from {{ ref('stg_arrivals') }}
),

departures as (
    select
        *,
        'Departure' as direction
    from {{ ref('stg_departures') }}
),

unioned as (
    select * from arrivals
    union all
    select * from departures
),

final as (
    select
        md5(concat(flight_id, planned_time::text, direction)) as flight_key,
        flight_id,
        airport_location,
        direction,
        airline,
        planned_time,
        actual_time,
        flight_status,
        cancelled,
        delay_minutes,
        extract(hour from planned_time) as planned_hour
    from unioned
)

select * from final