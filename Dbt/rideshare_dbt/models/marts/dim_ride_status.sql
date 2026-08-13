with statuses as (

    select
        ride_status_id      as ride_status_key,
        ride_status_id,
        ride_status_name,
        is_completed,
        false               as is_unknown_member

    from {{ ref('stg_ride_statuses') }}

),

unknown_member as (

    select
        {{ var('unknown_key') }}, {{ var('unknown_key') }}, 'Unknown',
        cast(null as boolean), true

)

select * from statuses
union all
select * from unknown_member