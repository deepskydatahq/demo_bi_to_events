with base as (
    select 
    product_id,
    product_name,
    product_type,
    product_price
    from {{ ref("products")}}
)

select * from base