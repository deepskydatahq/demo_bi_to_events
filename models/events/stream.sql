with base as (
    select
    ordered_at as ts,
    "order submitted" as event_name,
    customer_id as customer,
    order_id as event_id,
    location_id,
    order_total as revenue,
    order_cost,
    tax_paid,
    is_food_order,
    is_drink_order
    from {{ ref("orders")}}
),

get_order_items_in_one_row as (
    select
    order_id, 
    ARRAY_AGG(items) as items
    from {{ ref("order_items")}} items
    group by 1
),

extend_order_items as (
    select
    base.ts,
    base.event_name,
    base.event_id,
    base.customer,
    JSON_OBJECT(
        'order_id',base.event_id,
        'revenue',base.revenue,
        'tax_paid',base.tax_paid,
        'order_cost',base.order_cost,
        'location_id',base.location_id,
        'is_food_order',base.is_food_order,
        'is_drink_order',base.is_drink_order,
        'items',items.items
        ) as properties
    from base
    left join get_order_items_in_one_row items on base.event_id = items.order_id
    
)

select * from extend_order_items
where ts < CURRENT_TIMESTAMP()