with source as (
    select * from {{ source('raw_data', 'raw_arrivals')}}
),

renamed as (
    select
        "flightnumber"::varchar as flight_id,
        "airlineName"::varchar as airline,
        "plannedArrivalTime"::varchar as planned_time,
        "expectedArrivalTime"::varchar as actual_time,
        "originAirportLongName"::varchar as origin_airport
    from source
)

select * from renamed