with 

source as (

    select * from {{ source('greenery', 'orders') }}

)

select
    order_id,
    user_id,
    promo_id,
    address_id,
    created_at as ordered_at,
    estimated_delivery_at,
    delivered_at,
    order_cost,
    shipping_cost,
    order_total,
    tracking_id,
    shipping_service,
    status
from source
