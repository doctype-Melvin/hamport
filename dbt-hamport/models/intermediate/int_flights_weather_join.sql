with flights as (
    select * from {{ ref('fct_flights')}}
),

weather as (
    select * from {{ ref('fct_weather_history') }}
),

joined as (
    select
        f.flight_id,
        f.airline,
        f.airport_location,
        extract(HOUR FROM f.planned_time) as planned_hour,
        f.planned_time,
        f.actual_time,
        f.delay_minutes,
        f.direction,
        w.condition,
        w.visibility_m,
        w.temperature,
        w.precipitation,
        w.wind_speed,
        f.flight_status
    from flights f
    left join weather w
        on date_trunc('hour', f.planned_time) = w.weather_at
),

final as (
    select * from joined
)

select * from final