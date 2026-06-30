select
    address_id,
    address,
    zipcode,
    state,
    country
from {{ ref('stg_greenery__addresses') }}
