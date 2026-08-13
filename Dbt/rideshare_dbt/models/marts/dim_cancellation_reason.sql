with reasons as (

    select
        cancellation_reason_id  as cancellation_reason_key,
        cancellation_reason_id,
        cancellation_reason_name,
        is_not_cancelled,
        not is_not_cancelled    as is_actual_cancellation,
        false                   as is_unknown_member

    from {{ ref('stg_cancellation_reasons') }}

),

unknown_member as (

    select
        {{ var('unknown_key') }}, {{ var('unknown_key') }}, 'Unknown',
        false, false, true

)

select * from reasons
union all
select * from unknown_member