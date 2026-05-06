with final as (
    select
        planned_time::DATE as date,
        extract(HOUR FROM planned_time) as hour_of_day,
        condition,
        count(*) as number_of_flights,
        round(avg(delay_minutes), 2) as avg_delay
    from {{ ref('int_flights_weather_join') }}
    where flight_status in ('Completed', 'Cancelled')
    group by 1, 2, 3
    order by 1 desc, 2 asc
)

select * from final