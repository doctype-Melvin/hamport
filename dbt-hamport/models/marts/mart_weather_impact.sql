with weather_groups as (
    select
        planned_time::date as flight_date,
        lower(condition) as weather,
        count(*) as total_records,
        count(case when flight_status = 'Completed' then 1 end) as tracked_flights,
        count(case when flight_status = 'Unknown' then 1 end) as untracked_flights,
        count(case when flight_status = 'Cancelled' then 1 end) as cancelled_flights,
        round(avg(case when flight_status = 'Completed' then delay_minutes end)::numeric, 2) as avg_delay_minutes,
        round(sum(case when flight_status = 'Completed' then delay_minutes end)::numeric, 0) as total_delay
    from {{ ref('int_flights_weather_join')}}
    group by 1, 2
),

final as (
    select
        flight_date,
        weather,
        sum(total_records) over (partition by flight_date) as total_flights,
        tracked_flights,
        untracked_flights,
        cancelled_flights,
        avg_delay_minutes,
        sum(total_delay) over (partition by flight_date) as total_delay_minutes
    from weather_groups
)

select * from final
order by flight_date desc, weather asc