with source as (
    select * from {{ source('raw_data', 'raw_departures') }}
),

cleaned as (
    select
        "flightnumber"::varchar as flight_id,
        "airlineName"::varchar as airline,
        "destinationAirportLongNameInt"::varchar as airport_location,
        cancelled,
        regexp_replace("plannedDepartureTime", '\[.*\]', '')::timestamptz as planned_time,
        regexp_replace("expectedDepartureTime", '\[.*\]', '')::timestamptz as actual_time
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