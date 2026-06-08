with airport_shares as (
  select
    airport_location,
    direction,
    count(*) as flights_direction, -- flights per direction by location
    round(100 * count(*) / sum(count(*)) over (), 2) as share_airport_direction -- share of location
  from analytics.mart_flights
  where flight_status != 'Cancelled'
  group by 1, 2
  order by 4 desc
)

select
  *,
  sum(flights_direction) over (partition by airport_location) as relation_total_flights,
  sum(share_airport_direction) over (partition by airport_location) as relation_total_share -- share of all flights by location
from airport_shares
order by
  relation_total_share desc