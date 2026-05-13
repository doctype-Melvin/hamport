with weather_groups as (
    select
        date,
        lower(condition) as weather,
        count(*) as total_records,
        count(case when flight_status = 'Completed' then 1 end) as tracked_flights,
        count(case when flight_status = 'Unknown' then 1 end) as untracked_flights,
        count(case when flight_status = 'Cancelled' then 1 end) as cancelled_flights,
        round(avg(case when flight_status = 'Completed' then minutes_delay end)::numeric, 2) as avg_delay_minutes,
        round(sum(case when flight_status = 'Completed' then minutes_delay end)::numeric, 0) as total_delay
    from {{ ref('int_flights_enriched')}}
    group by 1, 2
),

final as (
    select
        date,
        weather,
        sum(total_records) over (partition by date) as total_flights,
        tracked_flights,
        untracked_flights,
        cancelled_flights,
        avg_delay_minutes,
        sum(total_delay) over (partition by date) as total_delay_minutes
    from weather_groups
)

select * from final
order by date desc, weather asc