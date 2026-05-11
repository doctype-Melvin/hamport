with final as (
    select
    *
    from {{ ref("int_airport_hourly_stats")}}
)

select * from final