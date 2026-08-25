import csv
import json
from pathlib import Path

from confluent_kafka import Consumer, Producer


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

INPUT_TOPIC = "processed-smart-meter-events"
OUTPUT_TOPIC = "enriched-smart-meter-events"

CONSUMER_GROUP = "event-intelligence-enrichment-python"

BASE_DIR = Path(__file__).resolve().parent.parent

EVENT_CATALOGUE_PATH = (
    BASE_DIR / "data" / "reference" / "event_catalogue.csv"
)

NETWORK_HIERARCHY_PATH = (
    BASE_DIR / "data" / "reference" / "network_hierarchy.csv"
)


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def empty_to_none(value):
    if value is None:
        return None

    value = value.strip()

    if value == "":
        return None

    return value


def to_int_or_none(value):
    value = empty_to_none(value)

    if value is None:
        return None

    return int(value)


def normalize_msn(msn):
    if msn is None:
        return None

    return str(msn).strip().upper()


# ---------------------------------------------------------
# Load reference data into memory
# ---------------------------------------------------------

def load_event_catalogue():
    lookup = {}

    with EVENT_CATALOGUE_PATH.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:
            event_id = to_int_or_none(row["event_id"])

            if event_id is None:
                continue

            lookup[event_id] = {
                "event_name": empty_to_none(row["event_name"]),
                "event_description": empty_to_none(
                    row["event_description"]
                ),
                "event_tblrefid": to_int_or_none(
                    row["event_tblrefid"]
                ),
                "event_state": empty_to_none(row["event_state"]),
                "priorityname": empty_to_none(row["priorityname"]),
                "eventclassification_name": empty_to_none(
                    row["eventclassification_name"]
                ),
            }

    return lookup


def load_network_hierarchy():
    lookup = {}

    with NETWORK_HIERARCHY_PATH.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:
            msn = normalize_msn(row["msn_normalized"])

            if msn is None:
                continue

            lookup[msn] = {
                "dtr_name": empty_to_none(row["dtr_name"]),
                "dtr_network_code": empty_to_none(
                    row["dtr_network_code"]
                ),
                "feeder_name": empty_to_none(row["feeder_name"]),
                "substation_name": empty_to_none(
                    row["substation_name"]
                ),
            }

    return lookup


# ---------------------------------------------------------
# Enrichment
# ---------------------------------------------------------

def enrich_event(event, event_catalogue, network_hierarchy):
    enriched = dict(event)

    event_id = event.get("evnt_id")
    msn = normalize_msn(event.get("msn"))

    catalogue_data = event_catalogue.get(event_id)

    if catalogue_data is None:
        catalogue_data = {
            "event_name": None,
            "event_description": None,
            "event_tblrefid": None,
            "event_state": None,
            "priorityname": None,
            "eventclassification_name": None,
        }

    hierarchy_data = network_hierarchy.get(msn)

    if hierarchy_data is None:
        hierarchy_data = {
            "dtr_name": None,
            "dtr_network_code": None,
            "feeder_name": None,
            "substation_name": None,
        }

    enriched.update(catalogue_data)
    enriched.update(hierarchy_data)

    return enriched


# ---------------------------------------------------------
# Kafka
# ---------------------------------------------------------

def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(
            f"[PRODUCED] "
            f"{msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def main():
    print("Loading reference data...")

    event_catalogue = load_event_catalogue()
    network_hierarchy = load_network_hierarchy()

    print(
        f"Loaded {len(event_catalogue)} event catalogue entries."
    )

    print(
        f"Loaded {len(network_hierarchy)} network hierarchy entries."
    )

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
        }
    )

    consumer.subscribe([INPUT_TOPIC])

    print()
    print("Enrichment service started.")
    print(f"Input : {INPUT_TOPIC}")
    print(f"Output: {OUTPUT_TOPIC}")
    print("Press Ctrl+C to stop.")
    print()

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                producer.poll(0)
                continue

            if message.error():
                print(f"[ERROR] Kafka consumer: {message.error()}")
                continue

            try:
                event = json.loads(
                    message.value().decode("utf-8")
                )

                enriched = enrich_event(
                    event,
                    event_catalogue,
                    network_hierarchy,
                )

                producer.produce(
                    OUTPUT_TOPIC,
                    key=(
                        str(enriched["msn"]).encode("utf-8")
                        if enriched.get("msn") is not None
                        else None
                    ),
                    value=json.dumps(enriched).encode("utf-8"),
                    callback=delivery_report,
                )

                producer.poll(0)

                # Wait until this message has been delivered before
                # committing the source offset.
                producer.flush()

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                print(
                    "[ENRICHED] "
                    f"msn={enriched.get('msn')} "
                    f"evnt_id={enriched.get('evnt_id')} "
                    f"event={enriched.get('event_name')} "
                    f"feeder={enriched.get('feeder_name')}"
                )

            except Exception as exc:
                print(
                    "[ERROR] Could not process message: "
                    f"{exc}"
                )

    except KeyboardInterrupt:
        print()
        print("Stopping enrichment service...")

    finally:
        producer.flush()
        consumer.close()
        print("Enrichment service stopped.")


if __name__ == "__main__":
    main()