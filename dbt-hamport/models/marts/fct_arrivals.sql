{{ config(materialized='table')}}

with staging as (
    select * from {{ref('stg_arrivals')}}
)