select
    vehicle_type_id,
    vehicle_type       as vehicle_type_name,
    description        as vehicle_type_description,
    base_rate,
    per_mile           as rate_per_mile,
    per_minute         as rate_per_minute

from {{ source('silver', 'ref_vehicle_types') }}