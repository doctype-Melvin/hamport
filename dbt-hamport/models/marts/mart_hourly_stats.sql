with final as (
    select
        date_trunc('hour', planned_time) as "Hour of Day",
        condition as "Condition",
        count(*) as "Number of Flights",
        avg(delay_minutes) as "AVG Delay"
    from {{ ref('int_flights_weather_join') }}
    where flight_status in ('Completed', 'Cancelled')
    group by 1, 2
    order by 1 desc
)

select * from final