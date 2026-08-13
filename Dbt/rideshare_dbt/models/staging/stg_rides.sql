with source as (

    select * from {{ source('silver', 'rides_enriched') }}

),

renamed as (

    select
        ride_id,
        confirmation_number,
        passenger_id,
        driver_id,
        vehicle_id,
        vehicle_type_id,
        vehicle_make_id,
        payment_method_id,
        ride_status_id,
        pickup_city_id,
        dropoff_city_id,
        cancellation_reason_id,
        booking_timestamp,
        pickup_timestamp,
        dropoff_timestamp,
        ride_date,
        pickup_hour,
        pickup_day_of_week,
        is_weekend,
        distance_miles,
        duration_minutes,
        duration_hours,
        avg_speed_mph,
        distance_band,
        is_intercity,
        base_fare,
        distance_fare,
        time_fare,
        surge_multiplier,
        is_surged,
        subtotal,
        tip_amount,
        total_fare,
        fare_per_mile,
        tip_percentage,
        rating          as passenger_rating_given,
        driver_rating   as driver_lifetime_rating,
        pickup_latitude,
        pickup_longitude,
        dropoff_latitude,
        dropoff_longitude,
        _source_system,
        _silver_processed_at

    from source

)

select * from renamed