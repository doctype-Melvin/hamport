with flights_data as (
    select
    *
    from {{ ref("fct_flights") }}
),

airlines as (
    select
    *
    from {{ ref("airlines")}}
),

weather_data as (
    select
    *
    from {{ ref("fct_weather_history")}}
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
    w.condition,
    w.visibility_m,
    w.temperature,
    w.precipitation,
    w.wind_speed
    from flights_data f
    left join airlines a on f.airline = a.airline
    left join weather_data w on date_trunc('hour', f.planned_time) = w.weather_at
),

final as (
    select
        *
    from joined
)

select * from final