select
    cancellation_reason_id,
    cancellation_reason as cancellation_reason_name,
    cancellation_reason_id = 4 as is_not_cancelled

from {{ source('silver', 'ref_cancellation_reasons') }}