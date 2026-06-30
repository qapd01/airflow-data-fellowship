with source as (

    select * from {{ source('greenery', 'promos') }}

)

select
    promo_id,
    discount,
    status
from source
