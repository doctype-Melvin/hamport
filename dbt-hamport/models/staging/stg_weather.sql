with source as (
    select * from {{ source('raw_data', 'raw_weather') }}
),

final as (
    select
        time::timestamptz as weather_at,
        temperature_2m::float as temperature,
        precipitation::float as precipitation,
        wind_speed_10m::float as wind_speed,
        weather_code::int as condition_code
    from source
)

select * from final