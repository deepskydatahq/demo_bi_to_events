with base as (
    select
    location_id,
    location_name
    from {{ref("locations")}}
)
select * from base