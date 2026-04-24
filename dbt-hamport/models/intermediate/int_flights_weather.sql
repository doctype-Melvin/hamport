with flights as (
    select * from {{ ref('fct_flights')}}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

joined as (
    select
        f.flight_id,
        f.airline,
        f.delay_minutes,
        f.flight_status,
        f.planned_time,
        f.actual_time,
        w.temperature,
        w.precipitation,
        w.wind_speed,
        w.condition
    from flights f
    left join weather w
        on date_trunc('hour', f.planned_time) = w.weather_at
),

final as (
    select * from joined
)

select * from final