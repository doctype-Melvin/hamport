with final as (
    select
        direction,
        count(*) as scheduled_flights,
        count(actual_time) as tracked_flights,
        round(100 * count(actual_time) / count(*)) as completeness_pct
    from {{ ref("fct_flights") }}
    where date_trunc('day', planned_time) < date_trunc('day', now())
    group by 1
)

select * from final