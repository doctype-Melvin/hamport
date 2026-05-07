with flights_by_hour as (
    select
        planned_time::DATE as date,
        extract(HOUR FROM planned_time) as hour_of_day,
        condition,
        direction,
        count(*) as number_of_flights,
        count(case when flight_status = 'Completed' then 1 end) as tracked_flights,
        count(case when flight_status = 'Cancelled' then 1 end) as untracked_flights,
        round(avg(case when flight_status = 'Completed' then delay_minutes end), 2) as avg_hourly_delay,
        round(sum(case when flight_status = 'Completed' then delay_minutes end), 0) as total_delay
    from {{ ref('int_flights_weather_join') }}
    group by 1, 2, 3, 4
),

final as (
    select
        date,
        hour_of_day,
        direction,
        condition,
        sum(number_of_flights) over (partition by date, hour_of_day, direction) as total_flights,
        tracked_flights,
        untracked_flights,
        sum(total_delay) over (partition by date, hour_of_day, direction) as total_hourly_delay,
        avg_hourly_delay
    from flights_by_hour
    order by 1 desc, 2, 3
)

select * from final