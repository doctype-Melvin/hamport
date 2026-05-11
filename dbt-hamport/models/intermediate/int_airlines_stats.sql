with flights_by_airline as (
    select
        date,
        direction,
        airline_group,
        count(*) as number_of_flights,
        count(case when flight_status = 'Completed' then 1 end) as tracked_flights,
        count(case when flight_status = 'Cancelled' then 1 end) as cancelled_flights,
        round(avg(case when flight_status = 'Completed' then minutes_delay end), 2) as avg_hourly_delay,
        round(sum(case when flight_status = 'Completed' then minutes_delay end), 0) as total_delay
    from {{ ref('int_flights_enriched') }}
    group by 1, 2, 3
    order by 1
),

final as (
    select
        *
    from flights_by_airline
)

select * from final