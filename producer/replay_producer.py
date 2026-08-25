import argparse
import json
import time
from datetime import datetime
from pathlib import Path

from confluent_kafka import Producer


def get_replay_time(event, timing_field="auto"):
    """
    Decide which timestamp controls when an event is replayed.

    auto:
      - use dt when available (15-minute collection bucket)
      - otherwise fall back to ts (actual event timestamp)
    """

    if timing_field == "dt":
        dt_value = event.get("dt")

        if not dt_value:
            raise ValueError("Event does not contain dt.")

        return datetime.strptime(str(dt_value), "%Y%m%d%H%M")

    if timing_field == "ts":
        return datetime.fromisoformat(event["ts"])

    # auto mode
    dt_value = event.get("dt")

    if dt_value:
        return datetime.strptime(str(dt_value), "%Y%m%d%H%M")

    return datetime.fromisoformat(event["ts"])


def load_events(file_path, timing_field):
    """Read JSONL events and sort them by replay timestamp."""

    events = []

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()

            if line:
                events.append(json.loads(line))

    events.sort(
        key=lambda event: get_replay_time(
            event,
            timing_field=timing_field
        )
    )

    return events


def delivery_report(err, msg):
    """Show whether Kafka successfully received each event."""

    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered | topic={msg.topic()} "
            f"| partition={msg.partition()} "
            f"| offset={msg.offset()}"
        )


def emit_event(producer, topic, event):
    """
    Publish one smart-meter event to Kafka.

    The historical event timestamp is preserved.
    replayed_at records when the simulator emitted the event.
    """

    output_event = dict(event)

    output_event["replayed_at"] = (
        datetime.now()
        .astimezone()
        .isoformat(timespec="milliseconds")
    )

    producer.produce(
        topic=topic,
        key=output_event.get("msn"),
        value=json.dumps(output_event),
        callback=delivery_report
    )

    producer.poll(0)

    print(
        f"Published | msn={output_event.get('msn')} "
        f"| evnt_id={output_event.get('evnt_id')} "
        f"| source_ts={output_event.get('ts')} "
        f"| source_dt={output_event.get('dt')} "
        f"| replayed_at={output_event['replayed_at']} "
        f"| seq={output_event.get('log_seq_no')}"
    )


def replay_events(
    events,
    speed,
    producer,
    topic,
    timing_field
):
    """Replay historical events using dt buckets or source event time."""

    if speed <= 0:
        raise ValueError("Speed must be greater than 0.")

    previous_replay_time = None

    print(
        f"\nStarting replay of {len(events)} events "
        f"to Kafka topic '{topic}' "
        f"at {speed}x speed.\n"
    )

    print(
        f"Replay timing mode: {timing_field}\n"
    )

    for event in events:

        current_replay_time = get_replay_time(
            event,
            timing_field=timing_field
        )

        if previous_replay_time is not None:

            original_gap = (
                current_replay_time - previous_replay_time
            ).total_seconds()

            wait_time = max(
                original_gap / speed,
                0
            )

            if wait_time > 0:

                print(
                    f"\nHistorical gap: "
                    f"{original_gap:.0f} sec "
                    f"? waiting {wait_time:.2f} sec...\n"
                )

                time.sleep(wait_time)

        emit_event(
            producer=producer,
            topic=topic,
            event=event
        )

        previous_replay_time = current_replay_time

    print("\nWaiting for Kafka delivery confirmations...")

    producer.flush()

    print("\nReplay complete.")


def main():

    parser = argparse.ArgumentParser(
        description=(
            "Replay historical smart-meter events "
            "into Kafka as a simulated live stream."
        )
    )

    parser.add_argument(
        "--file",
        default="data/sample_events.jsonl",
        help="Path to the JSONL event file."
    )

    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Replay speed multiplier."
    )

    parser.add_argument(
        "--timing-field",
        choices=["auto", "dt", "ts"],
        default="auto",
        help=(
            "Timestamp used for replay timing. "
            "'auto' prefers dt and falls back to ts."
        )
    )

    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap server."
    )

    parser.add_argument(
        "--topic",
        default="smart-meter-events",
        help="Kafka topic receiving smart-meter events."
    )

    args = parser.parse_args()

    file_path = Path(args.file)

    if not file_path.exists():
        raise FileNotFoundError(
            f"Event file not found: {file_path}"
        )

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers
        }
    )

    events = load_events(
        file_path=file_path,
        timing_field=args.timing_field
    )

    replay_events(
        events=events,
        speed=args.speed,
        producer=producer,
        topic=args.topic,
        timing_field=args.timing_field
    )


if __name__ == "__main__":
    main()
