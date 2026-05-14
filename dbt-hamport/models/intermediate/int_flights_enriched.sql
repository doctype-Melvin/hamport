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

locations as (
    select * from {{ ref("airport_mapping") }}
),

joined as (
    select
    f.planned_time::date as date,
    f.flight_id,
    f.direction,
    l.standardized_location as airport_location,
    f.airline,
    coalesce(a.group, f.airline) as airline_group,
    {{ get_flight_status('f.actual_time', 'f.cancelled')}} as flight_status,
    f.cancelled,
    case when f.cancelled = 'TRUE' then 1 else 0 end as cancelled_fl,
    round({{ get_delay_minutes('f.actual_time', 'f.planned_time', 'f.cancelled')}} , 1) as minutes_delay,
    f.planned_hour,
    f.condition,
    f.visibility_m,
    f.temperature,
    f.precipitation,
    f.wind_speed
    from flights_weather_data f
    left join airlines a on f.airline = a.airline
    left join locations l on f.airport_location = l.airport_location
),

final as (
    select
    *
    from joined
)

select * from final