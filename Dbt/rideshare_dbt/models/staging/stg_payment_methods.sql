select
    payment_method_id,
    payment_method     as payment_method_name,
    is_card,
    requires_auth

from {{ source('silver', 'ref_payment_methods') }}