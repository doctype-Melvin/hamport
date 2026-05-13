with source as (
    select 
        md5(cast(concat(trim(flightnumber),'-', "plannedArrivalTime", '-arrival') as text)) as flight_pk,
        * 
    from {{ source('raw_data', 'raw_arrivals')}}
),


cleaned as (
    select
        flight_pk,
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
        *
    from cleaned
)

select * from final