with source as (

    select * from {{ source('greenery', 'products') }}

)

select
    product_id,
    name as product_name,
    price,
    inventory
from source
