
{{
    config(
        materialized='table',
    )
}}
with items as (

    select * from {{ ref('stg_greenery__order_items') }}

),

orders as (

    select * from {{ ref('stg_greenery__orders') }}

),


products as (
		select * from {{ ref('stg_greenery__products') }}
)

select
    items.order_id as order_id,
    items.product_id as product_id,
    orders.user_id as user_id,
    orders.promo_id as promo_id,
    orders.address_id as address_id,
    orders.ordered_at as ordered_at,
    items.quantity as quantity,
    items.quantity * products.price as item_revenue,
    orders.order_cost as order_cost
from items
inner join orders on items.order_id = orders.order_id
inner join products on items.product_id = products.product_id
