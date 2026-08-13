with cities as (

    select
        city_id             as city_key,
        city_id,
        city_name,
        state_code,
        region_name,
        false               as is_unknown_member

    from {{ ref('stg_cities') }}

),

unknown_member as (

    select
        {{ var('unknown_key') }}    as city_key,
        {{ var('unknown_key') }}    as city_id,
        'Unknown'                   as city_name,
        'Unknown'                   as state_code,
        'Unknown'                   as region_name,
        true                        as is_unknown_member

)

select * from cities
union all
select * from unknown_member