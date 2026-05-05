with weather_data as (
    select
    * 
    from {{ ref('stg_weather') }}
),

codes as (
    select * from {{ ref('weather_codes')}}
),

final as (
    select
        w.weather_pk,
        w.weather_at::timestamptz,
        c.description::varchar as condition,
        w.visibility_m::float,
        w.temperature::float,
        w.precipitation::float,
        w.wind_speed::float
    from weather_data w
    left join codes c
        on w.weather_code = c.code
)

select * from final