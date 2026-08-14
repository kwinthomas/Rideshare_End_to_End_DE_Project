"""Reference (mapping) data and simulated Uber ride-confirmation events.

Imported by connection.py and seed_files.py. Not run directly.
"""

import random
import uuid
from datetime import datetime, timedelta, timezone

from faker import Faker

fake = Faker("en_US")

# Mapping data is versioned so the gold layer has a real SCD2 candidate.
# _updated_at changes when a reference row is edited; dim_location tracks it.
_MAP_EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat()

VEHICLE_TYPE_MAPPING = [
    {"vehicle_type_id": 1, "vehicle_type": "UberX", "description": "Standard", "base_rate": 2.50, "per_mile": 1.75, "per_minute": 0.35},
    {"vehicle_type_id": 2, "vehicle_type": "UberXL", "description": "Extra Large", "base_rate": 3.50, "per_mile": 2.25, "per_minute": 0.45},
    {"vehicle_type_id": 3, "vehicle_type": "UberPOOL", "description": "Shared Ride", "base_rate": 2.00, "per_mile": 1.50, "per_minute": 0.30},
    {"vehicle_type_id": 4, "vehicle_type": "Uber Comfort", "description": "Comfortable", "base_rate": 3.00, "per_mile": 2.00, "per_minute": 0.40},
    {"vehicle_type_id": 5, "vehicle_type": "Uber Black", "description": "Premium", "base_rate": 5.00, "per_mile": 3.50, "per_minute": 0.60},
]

PAYMENT_METHOD_MAPPING = [
    {"payment_method_id": 1, "payment_method": "Credit Card", "is_card": True, "requires_auth": True},
    {"payment_method_id": 2, "payment_method": "Debit Card", "is_card": True, "requires_auth": True},
    {"payment_method_id": 3, "payment_method": "Digital Wallet", "is_card": False, "requires_auth": False},
    {"payment_method_id": 4, "payment_method": "Cash", "is_card": False, "requires_auth": False},
]

RIDE_STATUS_MAPPING = [
    {"ride_status_id": 1, "ride_status": "Completed", "is_completed": True},
    {"ride_status_id": 2, "ride_status": "Cancelled", "is_completed": False},
    {"ride_status_id": 3, "ride_status": "In-progress", "is_completed": False}
]

VEHICLE_MAKE_MAPPING = [
    {"vehicle_make_id": 1, "vehicle_make": "Toyota"},
    {"vehicle_make_id": 2, "vehicle_make": "Honda"},
    {"vehicle_make_id": 3, "vehicle_make": "Ford"},
    {"vehicle_make_id": 4, "vehicle_make": "Chevrolet"},
    {"vehicle_make_id": 5, "vehicle_make": "Nissan"},
    {"vehicle_make_id": 6, "vehicle_make": "BMW"},
    {"vehicle_make_id": 7, "vehicle_make": "Mercedes"},
]

CANCELLATION_REASON_MAPPING = [
    {"cancellation_reason_id": 1, "cancellation_reason": "Driver cancelled"},
    {"cancellation_reason_id": 2, "cancellation_reason": "Passenger cancelled"},
    {"cancellation_reason_id": 3, "cancellation_reason": "No show"},
    {"cancellation_reason_id": 4, "cancellation_reason": "Not cancelled"},
]

# lat/lng anchor each city so the Power BI map is not noise.
CITY_MAPPING = [
    {"city_id": 1, "city": "New York", "state": "NY", "region": "Northeast", "lat": 40.7128, "lng": -74.0060, "city_updated_at": _MAP_EPOCH},
    {"city_id": 2, "city": "Los Angeles", "state": "CA", "region": "West", "lat": 34.0522, "lng": -118.2437, "city_updated_at": _MAP_EPOCH},
    {"city_id": 3, "city": "Chicago", "state": "IL", "region": "Midwest", "lat": 41.8781, "lng": -87.6298, "city_updated_at": _MAP_EPOCH},
    {"city_id": 4, "city": "Houston", "state": "TX", "region": "South", "lat": 29.7604, "lng": -95.3698, "city_updated_at": _MAP_EPOCH},
    {"city_id": 5, "city": "Phoenix", "state": "AZ", "region": "Southwest", "lat": 33.4484, "lng": -112.0740, "city_updated_at": _MAP_EPOCH},
    {"city_id": 6, "city": "Philadelphia", "state": "PA", "region": "Northeast", "lat": 39.9526, "lng": -75.1652, "city_updated_at": _MAP_EPOCH},
    {"city_id": 7, "city": "San Antonio", "state": "TX", "region": "South", "lat": 29.4241, "lng": -98.4936, "city_updated_at": _MAP_EPOCH},
    {"city_id": 8, "city": "San Diego", "state": "CA", "region": "West", "lat": 32.7157, "lng": -117.1611, "city_updated_at": _MAP_EPOCH},
    {"city_id": 9, "city": "Dallas", "state": "TX", "region": "South", "lat": 32.7767, "lng": -96.7970, "city_updated_at": _MAP_EPOCH},
    {"city_id": 10, "city": "San Jose", "state": "CA", "region": "West", "lat": 37.3382, "lng": -121.8863, "city_updated_at": _MAP_EPOCH},
]

CITY_BY_ID = {c["city_id"]: c for c in CITY_MAPPING}
VEHICLE_TYPE_BY_ID = {t["vehicle_type_id"]: t for t in VEHICLE_TYPE_MAPPING}

CITY_IDS = [c["city_id"] for c in CITY_MAPPING]
VEHICLE_TYPE_IDS = [t["vehicle_type_id"] for t in VEHICLE_TYPE_MAPPING]
VEHICLE_MAKE_IDS = [m["vehicle_make_id"] for m in VEHICLE_MAKE_MAPPING]
PAYMENT_METHOD_IDS = [p["payment_method_id"] for p in PAYMENT_METHOD_MAPPING]

VEHICLE_MODELS = ["Corolla", "Camry", "Civic", "Accord", "Focus", "Malibu", "Altima", "3 Series", "C-Class"]

# UberX dominates real-world mix; Black/XL are rarer.
VEHICLE_TYPE_WEIGHTS = {
    1: 48,  # UberX
    2: 12,  # UberXL
    3: 20,  # UberPOOL
    4: 12,  # Uber Comfort
    5: 6,   # Uber Black
}

VEHICLE_TYPE_IDS = [t["vehicle_type_id"] for t in VEHICLE_TYPE_MAPPING]
_VEHICLE_TYPE_WEIGHT_LIST = [VEHICLE_TYPE_WEIGHTS[i] for i in VEHICLE_TYPE_IDS]

def _jitter(value, spread=0.08):
    return round(value + random.uniform(-spread, spread), 6)


def generate_uber_ride_confirmation(booked_at=None):
    """Return one ride-confirmation dict.

    booked_at: optional datetime. Defaults to now (UTC) for live events; the
    bulk generator passes historical timestamps.
    """
    booking_time = booked_at or fake.date_time_between(start_date='-2y', end_date='now')
    pickup_time = booking_time + timedelta(minutes=random.randint(1, 10))
    duration_minutes = random.randint(5, 120)
    dropoff_time = pickup_time + timedelta(minutes=duration_minutes)

    vehicle_type_id = random.choices(VEHICLE_TYPE_IDS, weights=_VEHICLE_TYPE_WEIGHT_LIST, k=1)[0]
    rates = VEHICLE_TYPE_BY_ID[vehicle_type_id]

    distance = round(random.uniform(0.5, 50), 2)
    base_fare = rates["base_rate"]
    distance_fare = round(distance * rates["per_mile"], 2)
    time_fare = round(duration_minutes * rates["per_minute"], 2)
    surge_multiplier = round(random.uniform(1.0, 2.5), 2)
    subtotal = round((base_fare + distance_fare + time_fare) * surge_multiplier, 2)

    # Status drives everything downstream: cancelled rides earn no tip,
    # get no rating, and carry a real cancellation reason.
    is_cancelled = random.random() < 0.12
    is_inprogress = random.random() < 0.34
    if is_cancelled:
        ride_status_id = 2
        cancellation_reason_id = random.choice([1, 2, 3])
        tip = 0.0
        rating = None
        dropoff_time = None
    elif is_inprogress:
        ride_status_id = 3
        cancellation_reason_id = 4
        tip = 0.0
        rating = None
        dropoff_time = None
    else:
        ride_status_id = 1
        cancellation_reason_id = 4
        tip = round(random.choice([0, 0, 0, 1, 2, 3, 5, random.uniform(1, 20)]), 2)
        rating = random.choice([None, 3, 4, 4, 5, 5, 5])

    total_fare = round(subtotal + tip, 2)

    pickup_city_id = random.choice(CITY_IDS)
    dropoff_city_id = random.choice(CITY_IDS)
    pickup_city = CITY_BY_ID[pickup_city_id]
    dropoff_city = CITY_BY_ID[dropoff_city_id]

    return {
        # Keys
        "ride_id": str(uuid.uuid4()),
        "confirmation_number": fake.bothify("??#-####-??##").upper(),
        "passenger_id": str(uuid.uuid4()),
        "driver_id": str(uuid.uuid4()),
        "vehicle_id": str(uuid.uuid4()),
        # Foreign keys to mapping tables
        "vehicle_type_id": vehicle_type_id,
        "vehicle_make_id": random.choice(VEHICLE_MAKE_IDS),
        "payment_method_id": random.choice(PAYMENT_METHOD_IDS),
        "ride_status_id": ride_status_id,
        "pickup_city_id": pickup_city_id,
        "dropoff_city_id": dropoff_city_id,
        "cancellation_reason_id": cancellation_reason_id,
        # Passenger
        "passenger_name": fake.name(),
        "passenger_email": fake.email(),
        "passenger_phone": fake.phone_number(),
        # Driver
        "driver_name": fake.name(),
        "driver_rating": round(random.uniform(1.0, 5.0), 2),
        "driver_phone": fake.phone_number(),
        "driver_license": fake.bothify("??-???-#######").upper(),
        # Vehicle
        "vehicle_model": random.choice(VEHICLE_MODELS),
        "vehicle_color": random.choice(["Black", "White", "Gray", "Silver", "Blue", "Red"]),
        "license_plate": fake.bothify("???-####").upper(),
        # Locations
        "pickup_address": fake.street_address(),
        "pickup_latitude": _jitter(pickup_city["lat"]),
        "pickup_longitude": _jitter(pickup_city["lng"]),
        "dropoff_address": fake.street_address(),
        "dropoff_latitude": _jitter(dropoff_city["lat"]),
        "dropoff_longitude": _jitter(dropoff_city["lng"]),
        # Measures
        "distance_miles": distance,
        "duration_minutes": duration_minutes,
        "booking_timestamp": booking_time.isoformat(),
        "pickup_timestamp": pickup_time.isoformat(),
        "dropoff_timestamp": dropoff_time.isoformat() if dropoff_time else None,
        # Pricing
        "base_fare": base_fare,
        "distance_fare": distance_fare,
        "time_fare": time_fare,
        "surge_multiplier": surge_multiplier,
        "subtotal": subtotal,
        "tip_amount": tip,
        "total_fare": total_fare,
        "rating": rating,
    }


def generate_bulk_rides(n=2000, days_back=180):
    """Historical rides for the ADF initial load. Timestamps are spread
    backwards so silver has real history before the stream starts."""
    now = datetime.now(timezone.utc)
    rides = []
    for _ in range(n):
        booked_at = now - timedelta(
            days=random.randint(1, days_back),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        rides.append(generate_uber_ride_confirmation(booked_at=booked_at))
    return sorted(rides, key=lambda r: r["booking_timestamp"])
