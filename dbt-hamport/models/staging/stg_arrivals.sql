with source as (
    select * from {{ source('raw_data', 'raw_arrivals')}}
),

cleaned as (
    select
        "flightnumber"::varchar as flight_id,
        "airlineName"::varchar as airline,
        "originAirportLongName"::varchar as origin_airport,
        cancelled,
        regexp_replace("plannedArrivalTime", '\[.*\]', '')::timestamptz as planned_time,
        regexp_replace("expectedArrivalTime", '\[.*\]', '')::timestamptz as actual_time
    from source
),

final as (
    select 
        *,
        case
            when cancelled is true then null
            when actual_time is null then 0
            else extract(epoch from (planned_time - actual_time)) / 60
            end as delay_minutes,
        
        case
            when cancelled is true then 'Cancelled'
            when actual_time is null then 'In-Flight'
            else 'Arrived'
    from cleaned
)

select * from final