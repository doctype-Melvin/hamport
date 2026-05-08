with flights_weather_data as (
    select
    *
    from {{ ref("int_flights_weather_join") }}
),

airlines as (
    select
    *
    from {{ ref("airlines")}}
),

joined as (
    select
    f.planned_time::date as date,
    f.flight_id,
    f.direction,
    f.airport_location,
    f.airline,
    coalesce(a.group, f.airline) as airline_group,
    f.flight_status,
    to_char(f.planned_time, 'HH24:MI') as time_planned,
    to_char(f.actual_time, 'HH24:MI') as time_actual,
    round(f.delay_minutes, 1) as minutes_delay,
    f.planned_hour,
    f.condition,
    f.visibility_m,
    f.temperature,
    f.precipitation,
    f.wind_speed
    from flights_weather_data f
    left join airlines a on f.airline = a.airline
),

final as (
    select
        *
    from joined
)

select * from final