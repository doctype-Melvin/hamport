{{ 
    config(
        unique_key='weather_pk',
        materialized='incremental',
        incremental_strategy='merge'
    ) 
}}

with final as (
    select * from {{ ref('int_weather_codes_join') }}
)

select * from final

{% if is_incremental() %}
    where weather_at > (select max(weather_at) - interval '6 hours' from {{ this }})
{% endif %}