with final as (
    select
        airport_location,
        count(*) as flights_volume,
        round(avg(minutes_delay), 0) as avg_arrival_delay,
        round(100 * count(*) / sum(count(*)) over (), 1) as volume_share
    from {{ ref("int_flights_enriched") }}
    where direction = 'Arrival'
    group by 1
)

select * from final