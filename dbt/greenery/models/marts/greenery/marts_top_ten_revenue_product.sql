{{
    config(
        materialized='table',
    )
}}
with fct_order_items as (

    select * from {{ ref('fct_order_items') }}

),

products as (

    select * from {{ ref('stg_greenery__products') }}

),

product_revenue as (

    select
        product_id,
        sum(item_revenue) as gross_revenue
    from fct_order_items
    group by product_id

)

select
    pr.product_id,
    p.product_name,
    pr.gross_revenue,
    row_number() over (order by pr.gross_revenue desc) as revenue_rank
from product_revenue pr
left join products p on pr.product_id = p.product_id
order by pr.gross_revenue desc
limit 10
