with daily_counts as (
    select
        date,
        planned_hour,
        direction,
        count(*) as total_daily_flights,
        avg(minutes_delay) as avg_daily_delay
    from {{ ref('int_flights_enriched') }}
    group by 1, 2, 3
),

final as (
    select
        planned_hour,
        direction,
        round(avg(total_daily_flights), 0) as avg_hourly_flights,
        round(avg(avg_daily_delay), 0) as avg_hourly_delay
    from daily_counts
    group by 1, 2
)

select * from final order by planned_hour