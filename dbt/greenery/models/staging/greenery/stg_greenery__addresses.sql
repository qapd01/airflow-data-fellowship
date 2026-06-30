with source as (

    select * from {{ source('greenery', 'addresses') }}

)

select
    address_id,
    address,
    zipcode,
    state,
    country
from source
