select
    city_id,
    city    as city_name,
    state   as state_code,
    region  as region_name

from {{ source('silver', 'ref_cities') }}