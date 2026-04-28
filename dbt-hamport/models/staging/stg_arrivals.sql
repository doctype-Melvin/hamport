with source as (
    select 
        md5(cast(concat(flightnumber,'-', "plannedArrivalTime") as text)) as flight_pk,
        * 
    from {{ source('raw_data', 'raw_arrivals')}}
),

cleaned as (
    select
        "flightnumber"::varchar as flight_id,
        "airlineName"::varchar as airline,
        "originAirportLongName"::varchar as airport_location,
        cancelled,
        regexp_replace("plannedArrivalTime", '\[.*\]', '')::timestamptz as planned_time,
        regexp_replace("expectedArrivalTime", '\[.*\]', '')::timestamptz as actual_time
    from source
),

final as (
    select
        *,
        {{ get_flight_status('actual_time', 'planned_time', 'cancelled')}} as flight_status,
        {{ get_delay_minutes('actual_time', 'planned_time', 'cancelled')}} as delay_minutes
    from cleaned
)

select * from final