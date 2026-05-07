with flights_data as (
    select
        airline,
        direction,
        count(*) as flight_count
    from {{ ref("fct_flights") }}
    group by 1, 2
),

final as (
    select
        *
    from flights_data
)

select * from final