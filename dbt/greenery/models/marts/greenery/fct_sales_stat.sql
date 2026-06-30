select
    o.address_id as address_id,
    a.state as state,
    sum(o.order_cost) as order_cost
from {{ ref('stg_greenery__orders') }} as o
inner join {{ ref('stg_greenery__addresses') }} as a 
on o.address_id = a.address_id
group by o.address_id, a.state