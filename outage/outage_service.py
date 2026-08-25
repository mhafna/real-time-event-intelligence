import json
from datetime import datetime
from pathlib import Path

from confluent_kafka import Consumer, Producer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

INPUT_TOPIC = "enriched-smart-meter-events"
OUTPUT_TOPIC = "outage-incidents"

# New production-ish consumer group.
# It starts from new events on its first run, then uses committed
# offsets for all later restarts.
CONSUMER_GROUP = "outage-intelligence-service-v2"

OUTAGE_EVENT_NAMES = {"Power failure"}

STATE_FILE = Path(__file__).with_name("open_outages.json")


def normalize_msn(msn):
    if msn is None:
        return None

    msn = str(msn).strip().upper()
    return msn if msn else None


def parse_timestamp(value):
    if value is None:
        return None

    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def format_timestamp(value):
    if value is None:
        return None

    return value.isoformat(sep=" ")


def make_state_key(msn, pair_id):
    return f"{msn}|{pair_id}"


def load_state():
    if not STATE_FILE.exists():
        return {}

    try:
        with STATE_FILE.open(
            "r",
            encoding="utf-8",
        ) as file:
            state = json.load(file)

        print(
            f"Loaded {len(state)} active outage(s) "
            "from saved state."
        )

        return state

    except Exception as exc:
        raise RuntimeError(
            f"Could not load outage state: {exc}"
        ) from exc


def save_state(open_outages):
    temp_file = STATE_FILE.with_suffix(".tmp")

    with temp_file.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            open_outages,
            file,
            indent=2,
        )

    temp_file.replace(STATE_FILE)


def publish_incident(producer, incident):
    delivery = {
        "error": None,
    }

    def delivery_report(err, msg):
        delivery["error"] = err

    producer.produce(
        OUTPUT_TOPIC,
        key=incident["incident_id"].encode("utf-8"),
        value=json.dumps(incident).encode("utf-8"),
        callback=delivery_report,
    )

    remaining = producer.flush(10)

    if remaining != 0:
        raise RuntimeError(
            "Kafka incident delivery timed out."
        )

    if delivery["error"] is not None:
        raise RuntimeError(
            f"Kafka incident delivery failed: "
            f"{delivery['error']}"
        )


def handle_event(
    event,
    open_outages,
    producer,
):
    event_name = event.get("event_name")

    # Ignore unrelated enriched events.
    if event_name not in OUTAGE_EVENT_NAMES:
        return

    msn = normalize_msn(event.get("msn"))
    pair_id = event.get("event_tblrefid")
    event_state = event.get("event_state")
    event_ts = parse_timestamp(
        event.get("event_ts")
    )

    if msn is None:
        print(
            "[SKIP] "
            f"evnt_id={event.get('evnt_id')} "
            "reason=MISSING_MSN"
        )
        return

    if pair_id is None:
        print(
            "[SKIP] "
            f"msn={msn} "
            f"evnt_id={event.get('evnt_id')} "
            "reason=MISSING_PAIR_ID"
        )
        return

    if event_ts is None:
        print(
            "[SKIP] "
            f"msn={msn} "
            f"evnt_id={event.get('evnt_id')} "
            "reason=INVALID_TIMESTAMP"
        )
        return

    state_key = make_state_key(
        msn,
        pair_id,
    )

    # ---------------------------------------------------------
    # OCCURRENCE -> OPEN outage
    # ---------------------------------------------------------

    if event_state == "OCCURRENCE":

        if state_key in open_outages:
            existing = open_outages[state_key]

            print(
                "[DUPLICATE OCCURRENCE] "
                f"msn={msn} "
                f"pair_id={pair_id} "
                f"open_since="
                f"{existing['start_ts']}"
            )

            return

        incident_id = (
            f"{msn}:{pair_id}:"
            f"{event_ts.isoformat()}"
        )

        outage = {
            "incident_id": incident_id,
            "msn": msn,
            "event_name": event_name,
            "event_tblrefid": pair_id,
            "start_ts": format_timestamp(
                event_ts
            ),
            "dtr_name": event.get(
                "dtr_name"
            ),
            "dtr_network_code": event.get(
                "dtr_network_code"
            ),
            "feeder_name": event.get(
                "feeder_name"
            ),
            "substation_name": event.get(
                "substation_name"
            ),
        }

        incident = {
            "incident_id": incident_id,
            "msn": msn,
            "event_name": event_name,
            "event_tblrefid": pair_id,
            "incident_status": "OPEN",
            "start_ts": format_timestamp(
                event_ts
            ),
            "end_ts": None,
            "duration_minutes": None,
            "dtr_name": outage[
                "dtr_name"
            ],
            "dtr_network_code": outage[
                "dtr_network_code"
            ],
            "feeder_name": outage[
                "feeder_name"
            ],
            "substation_name": outage[
                "substation_name"
            ],
        }

        # Publish first so we never silently lose
        # an operational event.
        publish_incident(
            producer,
            incident,
        )

        # Then persist the active outage.
        open_outages[state_key] = outage
        save_state(open_outages)

        print(
            "[OUTAGE OPEN] "
            f"msn={msn} "
            f"pair_id={pair_id} "
            f"start={event_ts} "
            f"feeder="
            f"{outage['feeder_name']}"
        )

    # ---------------------------------------------------------
    # RESTORATION -> CLOSED outage
    # ---------------------------------------------------------

    elif event_state == "RESTORATION":

        outage = open_outages.get(
            state_key
        )

        if outage is None:
            print(
                "[RESTORATION WITHOUT OPEN] "
                f"msn={msn} "
                f"pair_id={pair_id} "
                f"restored={event_ts}"
            )

            return

        start_ts = parse_timestamp(
            outage["start_ts"]
        )

        duration_minutes = (
            event_ts - start_ts
        ).total_seconds() / 60

        incident = {
            "incident_id": outage[
                "incident_id"
            ],
            "msn": msn,
            "event_name": outage[
                "event_name"
            ],
            "event_tblrefid": pair_id,
            "incident_status": "CLOSED",
            "start_ts": outage[
                "start_ts"
            ],
            "end_ts": format_timestamp(
                event_ts
            ),
            "duration_minutes": round(
                duration_minutes,
                2,
            ),
            "dtr_name": outage[
                "dtr_name"
            ],
            "dtr_network_code": outage[
                "dtr_network_code"
            ],
            "feeder_name": outage[
                "feeder_name"
            ],
            "substation_name": outage[
                "substation_name"
            ],
        }

        publish_incident(
            producer,
            incident,
        )

        # Restoration succeeded, so remove
        # the outage from durable active state.
        del open_outages[state_key]
        save_state(open_outages)

        print(
            "[OUTAGE CLOSED] "
            f"msn={msn} "
            f"pair_id={pair_id} "
            f"duration="
            f"{duration_minutes:.1f} min "
            f"feeder="
            f"{outage['feeder_name']}"
        )


def main():
    consumer = Consumer(
        {
            "bootstrap.servers":
                KAFKA_BOOTSTRAP_SERVERS,

            "group.id":
                CONSUMER_GROUP,

            # This is a new consumer group.
            # Existing historical events have already
            # been tested, so start with new events.
            "auto.offset.reset":
                "latest",

            "enable.auto.commit":
                False,
        }
    )

    producer = Producer(
        {
            "bootstrap.servers":
                KAFKA_BOOTSTRAP_SERVERS
        }
    )

    open_outages = load_state()

    consumer.subscribe(
        [INPUT_TOPIC]
    )

    print(
        "Outage intelligence service started."
    )
    print(
        f"Input : {INPUT_TOPIC}"
    )
    print(
        f"Output: {OUTPUT_TOPIC}"
    )
    print(
        f"Active outages in state: "
        f"{len(open_outages)}"
    )
    print(
        "Kafka offsets: manual commit enabled."
    )
    print(
        "Press Ctrl+C to stop."
    )
    print()

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(
                    "[ERROR] Kafka consumer: "
                    f"{message.error()}"
                )
                continue

            try:
                event = json.loads(
                    message.value().decode(
                        "utf-8"
                    )
                )

                handle_event(
                    event,
                    open_outages,
                    producer,
                )

                # Commit ONLY after successful processing.
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

            except Exception as exc:
                print(
                    "[ERROR] Processing failed: "
                    f"{exc}"
                )

                print(
                    "Stopping service without "
                    "committing this message."
                )

                # Stop instead of moving past a
                # failed event.
                raise

    except KeyboardInterrupt:
        print()
        print(
            "Stopping outage intelligence "
            "service..."
        )

    except Exception:
        print(
            "Outage intelligence service "
            "stopped because processing failed."
        )

    finally:
        producer.flush()
        consumer.close()

        print(
            "Outage intelligence service "
            "stopped."
        )


if __name__ == "__main__":
    main()