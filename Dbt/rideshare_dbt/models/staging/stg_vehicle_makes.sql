select
    vehicle_make_id,
    vehicle_make       as vehicle_make_name

from {{ source('silver', 'ref_vehicle_makes') }}