with makes as (

    select
        vehicle_make_id     as vehicle_make_key,
        vehicle_make_id,
        vehicle_make_name,
        false               as is_unknown_member

    from {{ ref('stg_vehicle_makes') }}

),

unknown_member as (

    select {{ var('unknown_key') }}, {{ var('unknown_key') }}, 'Unknown', true

)

select * from makes
union all
select * from unknown_member