"""Publishes ride events to Azure Event Hub (Kafka-compatible endpoint).

The producer client is created once and reused. Creating a new client per
event opens a fresh AMQP connection every time, which is slow and will throttle
you once you start sending in bulk.
"""

import json
import os
from functools import lru_cache

from azure.eventhub import EventData, EventHubProducerClient
from dotenv import load_dotenv

from data import generate_uber_ride_confirmation

load_dotenv()

CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")

if not CONNECTION_STRING or not EVENTHUB_NAME:
    raise RuntimeError(
        "EVENTHUB_CONNECTION_STRING and EVENTHUB_NAME must be set. "
    )


@lru_cache(maxsize=1)
def get_producer():
    return EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME,
    )


def send_rides(rides):
    """Send a list of ride dicts, packing them into as few batches as the
    service allows. Returns the number of events sent."""
    if not rides:
        return 0

    producer = get_producer()
    sent = 0
    batch = producer.create_batch()
    batch_count = 0

    for ride in rides:
        event = EventData(json.dumps(ride))
        try:
            batch.add(event)
            batch_count += 1
        except ValueError:
            # Batch is full: flush it and start a new one.
            print(f"Sending batch of {batch_count} events...")
            producer.send_batch(batch)
            print(f"Batch sent successfully")
            sent += batch_count
            
            # Start a new batch with the event that didn't fit.
            batch = producer.create_batch()
            batch.add(event)
            batch_count = 1

    if batch_count:
        print(f"Sending batch of {batch_count} events...")
        producer.send_batch(batch)
        print(f"Batch sent successfully")
        sent += batch_count

    return sent


def send_to_event_hub(ride_data):
    """Single-event convenience wrapper."""
    return send_rides([ride_data])


if __name__ == "__main__":
    ride = generate_uber_ride_confirmation()
    print(json.dumps(ride, indent=2))
    print(f"\nSent {send_to_event_hub(ride)} event(s) to {EVENTHUB_NAME}")
