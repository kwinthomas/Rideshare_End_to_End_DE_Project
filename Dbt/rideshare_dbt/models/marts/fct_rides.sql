{{
    config(
        materialized='incremental',
        unique_key='ride_id',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='append_new_columns'
    )
}}

with rides as (

    select * from {{ ref('stg_rides') }}

    {% if is_incremental() %}
    
    where _silver_processed_at > (
        select coalesce(max(_silver_processed_at), cast('1900-01-01' as timestamp))
             - interval 1 hour
        from {{ this }}
    )
    {% endif %}

),

final as (

    select
        
        r.ride_id,
        r.confirmation_number,

        cast(r.ride_date as date)                                   as date_key,
        coalesce(pu.city_key, {{ var('unknown_key') }})             as pickup_city_key,
        coalesce(do_.city_key, {{ var('unknown_key') }})            as dropoff_city_key,
        coalesce(vt.vehicle_type_key, {{ var('unknown_key') }})     as vehicle_type_key,
        coalesce(vm.vehicle_make_key, {{ var('unknown_key') }})     as vehicle_make_key,
        coalesce(pm.payment_method_key, {{ var('unknown_key') }})   as payment_method_key,
        coalesce(rs.ride_status_key, {{ var('unknown_key') }})      as ride_status_key,
        coalesce(cr.cancellation_reason_key, {{ var('unknown_key') }}) as cancellation_reason_key,

        r.passenger_id,
        r.driver_id,
        r.vehicle_id,

        r.booking_timestamp,
        r.pickup_timestamp,
        r.dropoff_timestamp,
        r.pickup_hour,
        r.pickup_day_of_week,
        r.is_weekend,
        r.distance_miles,
        r.duration_minutes,
        r.base_fare,
        r.distance_fare,
        r.time_fare,
        r.subtotal,
        r.tip_amount,
        r.total_fare,
        1                                                           as ride_count,
        r.surge_multiplier,
        r.avg_speed_mph,
        r.total_fare / r.distance_miles as fare_per_mile,
        r.tip_amount * 100 / r.subtotal as tip_percentage,
        r.passenger_rating_given,
        r.driver_lifetime_rating,
        r.passenger_rating_given is not null                        as was_rated,
        r.is_surged,
        r.is_intercity,
        r.distance_band,
        {{ fare_band('r.total_fare') }}                             as fare_band,
        coalesce(rs.is_completed, false)                            as is_completed,
        coalesce(cr.is_actual_cancellation, false)                  as is_cancelled,
        r.pickup_latitude,
        r.pickup_longitude,
        r.dropoff_latitude,
        r.dropoff_longitude,
        r._source_system,
        r._silver_processed_at,
        current_timestamp()                                         as _gold_built_at

    from rides r
    left join {{ ref('dim_city') }} pu
        on r.pickup_city_id = pu.city_id
    left join {{ ref('dim_city') }} do_
        on r.dropoff_city_id = do_.city_id
    left join {{ ref('dim_vehicle_type') }} vt
        on r.vehicle_type_id = vt.vehicle_type_id
    left join {{ ref('dim_vehicle_make') }} vm
        on r.vehicle_make_id = vm.vehicle_make_id
    left join {{ ref('dim_payment_method') }} pm
        on r.payment_method_id = pm.payment_method_id
    left join {{ ref('dim_ride_status') }} rs
        on r.ride_status_id = rs.ride_status_id
    left join {{ ref('dim_cancellation_reason') }} cr
        on r.cancellation_reason_id = cr.cancellation_reason_id

)

select * from final
