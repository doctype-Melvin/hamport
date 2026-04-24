with flights as (
    select * from {{ ref('fct_flights')}}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

joined as (
    select
        f.planned_time,
        f.flight_id,
        f.airline,
        f.actual_time,
        f.delay_minutes,
        w.condition
        w.temperature,
        w.precipitation,
        w.wind_speed,
        f.flight_status,
    from flights f
    left join weather w
        on date_trunc('hour', f.planned_time) = w.weather_at
),

final as (
    select * from joined
)

select * from final