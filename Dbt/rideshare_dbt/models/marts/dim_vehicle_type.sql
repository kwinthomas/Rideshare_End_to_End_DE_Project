with vehicle_types as (

    select
        vehicle_type_id     as vehicle_type_key,
        vehicle_type_id,
        vehicle_type_name,
        vehicle_type_description,
        base_rate,
        rate_per_mile,
        rate_per_minute,
        case vehicle_type_name
            when 'UberPOOL'     then 1
            when 'UberX'        then 2
            when 'Uber Comfort' then 3
            when 'UberXL'       then 4
            when 'Uber Black'   then 5
            else 99
        end                 as tier_rank,
        false               as is_unknown_member

    from {{ ref('stg_vehicle_types') }}

),

unknown_member as (

    select
        {{ var('unknown_key') }}, {{ var('unknown_key') }},
        'Unknown', 'Unknown',
        cast(null as double), cast(null as double), cast(null as double),
        99, true

)

select * from vehicle_types
union all
select * from unknown_member