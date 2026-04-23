with flights as (
    select * from {{ ref('fct_flights')}}
    where flight_status not in ('Data Stale', 'Cancelled')
    and delay_minutes < 180
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
        w.temperature,
        w.precipitation,
        w.wind_speed,

        case
            when w.precipitation > 0 then 'Rainy'
            else 'Clear'
        end as weather_condition
    from flights f
    left join weather w
        on date_trunc('hour', f.planned_time) = w.weather_at
),

final as (
    select
        planned_time::date as flight_date,
        weather_condition,
        count(*) as flight_count,
        round(avg(delay_minutes)::numeric, 2) as avg_delay
    from joined
    group by 1, 2
    order by 1 desc
)

select * from final