with source as (
    select
    md5(cast(concat(time, 'Hamburg') as text)) as weather_pk,
    * 
    from {{ source('raw_data', 'raw_weather') }}
),

conditions as (
    select * from {{ ref('weather_codes')}}
),

final as (
    select
        s.weather_pk,
        s.time::timestamptz as weather_at,
        c.description::varchar as condition,
        s.visibility::float as visibility_m,
        s.temperature_2m::float as temperature,
        s.precipitation::float as precipitation,
        s.wind_speed_10m::float as wind_speed
    from source s
    left join conditions c
        on s.weather_code = c.code
)

select * from final