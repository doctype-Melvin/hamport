with final as (
    select
        planned_time::date as flight_date,
        lower(condition) as weather,
        count(*) as total_flights,
        count(case when flight_status = 'Cancelled' then 1 end) as cancelled_flights,
        count(case when flight_status = 'Completed' then 1 end) as completed_flights,
        round(avg(case when flight_status = 'Completed' then delay_minutes end)::numeric, 2) as avg_delay
    from {{ ref('int_flights_weather')}}
    where flight_status in ('Completed', 'Cancelled')
    group by 1, 2
)

select * from final