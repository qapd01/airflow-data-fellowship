{{ config(materialized='table') }}

select
    user_id,
    first_name,
    last_name,
    email,
    phone_number,
    address_id,
    created_at
from {{ ref('stg_greenery__users') }}
