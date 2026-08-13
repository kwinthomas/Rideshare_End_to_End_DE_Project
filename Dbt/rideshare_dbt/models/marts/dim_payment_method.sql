with methods as (

    select
        payment_method_id   as payment_method_key,
        payment_method_id,
        payment_method_name,
        is_card,
        requires_auth,
        false               as is_unknown_member

    from {{ ref('stg_payment_methods') }}

),

unknown_member as (

    select
        {{ var('unknown_key') }}, {{ var('unknown_key') }}, 'Unknown',
        cast(null as boolean), cast(null as boolean), true

)

select * from methods
union all
select * from unknown_member