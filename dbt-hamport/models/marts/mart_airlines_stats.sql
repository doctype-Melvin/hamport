with final as (
    select * from {{ ref("int_airlines_stats")}}
)

select * from final