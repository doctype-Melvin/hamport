with final as (
    select
        date_trunc('hour', planned_time) as hour_at,
        condition,
        count(*) as total_flights,
        avg(delay_minutes) as avg_delay
    from {{ ref('int_flights_weather') }}
    where flight_status in ('Completed', 'Cancelled')
    group by 1, 2
    order by 1 desc
)

select * from final