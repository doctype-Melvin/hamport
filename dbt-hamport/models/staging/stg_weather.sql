with source as (
    select
    md5(cast(concat(time, 'Hamburg') as text)) as weather_pk,
    * 
    from {{ source('raw_data', 'raw_weather') }}
),

final as (
    select
        weather_pk,
        time::timestamptz as weather_at,
        visibility::float as visibility_m,
        temperature_2m::float as temperature,
        precipitation::float,
        wind_speed_10m::float as wind_speed,
        weather_code
    from source s
)

select * from final