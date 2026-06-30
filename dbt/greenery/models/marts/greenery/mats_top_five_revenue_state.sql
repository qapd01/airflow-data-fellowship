select
    da.state,
    fo.product_id,
    sum(fo.item_revenue) as total
from {{ ref ('fct_order_items') }} fo
inner join {{ ref ('dim_addresses') }} da
using (address_id)
group by da.state, fo.product_id
order by total desc