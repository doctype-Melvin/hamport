with flights as (
    select * from {{ ref('fct_flights')}}
    where flight_status in ('Completed', 'Cancelled')
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
        w.condition
    from flights f
    left join weather w
        on date_trunc('hour', f.planned_time) = w.weather_at
),

final as (
    select
        planned_time::date as flight_date,
        lower(condition),
        count(*) as total_flights,
        count(case when flight_status = 'Cancelled' then 1 end) as cancelled_flights,
        count(case when flight_status = 'Completed' then 1 end) as completed_flights,
        round(avg(case when flight_status = 'Completed' then delay_minutes end)::numeric, 2) as avg_delay
    from joined
    group by 1, 2
    order by 1 desc
)

select * from final