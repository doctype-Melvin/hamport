{{
    config(
        materialized='incremental',
        unique_key='flight_key',
        on_schema_change='append_new_columns',
        incremental_strategy='merge'
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
        flight_pk as flight_key,
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

/* 
final table only tracks flights with 
a planned_time and acutal_time or planned flights.
*/

select 
* 
from final
where planned_time >= '2026-04-28'::timestamptz
and (
    flight_status in ('Cancelled', 'Completed')
    or (flight_status = 'Unknown' and planned_time > now())
    )

{% if is_incremental() %}
    and planned_time >= (select max(planned_time) - interval '3 days' from {{ this }} )
{% endif %}