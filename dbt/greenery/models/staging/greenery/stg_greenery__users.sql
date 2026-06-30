with source as (

    select * from {{ source('greenery', 'users') }}

)

select
    user_id,
    first_name,
    last_name,
    email,
    phone_number,
    address_id,
    created_at,
    updated_at
from source
