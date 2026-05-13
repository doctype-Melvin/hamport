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
    {# f.flight_status, #}
    {{ get_flight_status('f.actual_time', 'f.cancelled')}} as flight_status,
    f.cancelled,
    case when f.cancelled = 'TRUE' then 1 else 0 end as cancelled_fl,
    to_char(f.planned_time, 'HH24:MI') as time_planned,
    to_char(f.actual_time, 'HH24:MI') as time_actual,
    round({{ get_delay_minutes('f.actual_time', 'f.planned_time', 'f.cancelled')}} , 1) as minutes_delay,
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