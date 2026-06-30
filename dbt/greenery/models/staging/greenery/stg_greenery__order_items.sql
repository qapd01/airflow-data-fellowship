with source as (

    select * from {{ source('greenery', 'order_items') }}

)

select
    order_id,
    product_id,
    quantity
from source
