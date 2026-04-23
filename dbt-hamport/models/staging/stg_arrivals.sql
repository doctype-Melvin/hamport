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
            when actual_time is null and planned_time > now() then 0 -- future scheduled flight
            when actual_time is null and planned_time <= now() then -- delayed flight not landed yet
                extract(epoch from (now() - planned_time)) / 60
            else extract(epoch from (actual_time - planned_time)) / 60 -- flight arrived
        end as delay_minutes,
        
        case
            when cancelled is true then 'Cancelled'
            when actual_time is null and (now() - planned_time) > interval '6 hours' then 'Data Stale'
            when actual_time is null and planned_time > now() then 'Scheduled'
            when actual_time is null and planned_time <= now() then 'En Route / Delayed'
            else 'Arrived'
        end as flight_status
        
    from cleaned
)

select * from final