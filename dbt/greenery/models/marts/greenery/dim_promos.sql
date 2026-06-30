{{ config(materialized='table') }}

select
    promo_id,
    discount,
    status
from {{ ref('stg_greenery__promos') }}
