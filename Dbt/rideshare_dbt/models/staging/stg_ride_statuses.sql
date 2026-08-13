select
    ride_status_id,
    ride_status        as ride_status_name,
    is_completed

from {{ source('silver', 'ref_ride_statuses') }}