import json
import logging
import os
import signal
import sys
import tempfile
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv()

# -----------------------------
# Config
# -----------------------------
TOPICS = [
    "banking_server.public.customers",
    "banking_server.public.accounts",
    "banking_server.public.transactions",
]

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 50))
FLUSH_INTERVAL_SECONDS = int(os.getenv("FLUSH_INTERVAL_SECONDS", 30))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "minio-consumer-group")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw")

# -----------------------------
# Runtime state
# -----------------------------
buffer: dict[str, list[dict[str, Any]]] = {topic: [] for topic in TOPICS}
last_flush_time = time.time()
consumer: KafkaConsumer | None = None
s3 = None


# -----------------------------
# Helpers
# -----------------------------
def extract_table_name(topic: str) -> str:
    return topic.split(".")[-1]


def safe_json_value(value: Any) -> Any:
    """
    Convert values that may break Parquet serialization.
    """
    if isinstance(value, Decimal):
        return float(value)
    return value


def enrich_cdc_record(payload: dict[str, Any]) -> dict[str, Any] | None:
    """
    Handle Debezium CDC payload:
    - c, r, u -> use 'after'
    - d       -> use 'before'
    Add metadata for downstream processing.
    """
    op = payload.get("op")

    if op in {"c", "r", "u"}:
        record = payload.get("after")
    elif op == "d":
        record = payload.get("before")
    else:
        return None

    if not record or not isinstance(record, dict):
        return None

    enriched = {k: safe_json_value(v) for k, v in record.items()}
    enriched["_cdc_op"] = op
    enriched["_cdc_ts"] = payload.get("ts_ms")
    enriched["_is_deleted"] = op == "d"
    enriched["_ingested_at"] = datetime.now(timezone.utc).isoformat()

    return enriched


def get_kafka_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=KAFKA_GROUP,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=1000,
    )


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def ensure_bucket_exists(s3_client) -> None:
    existing_buckets = [b["Name"] for b in s3_client.list_buckets().get("Buckets", [])]
    if MINIO_BUCKET not in existing_buckets:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
        logger.info("Created bucket: %s", MINIO_BUCKET)
    else:
        logger.info("Bucket exists: %s", MINIO_BUCKET)


def write_to_minio(table_name: str, records: list[dict[str, Any]]) -> None:
    global s3

    if not records:
        return

    if s3 is None:
        raise RuntimeError("S3 client is not initialized.")

    df = pd.DataFrame(records)
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%H%M%S%f")

    s3_key = f"{table_name}/date={date_str}/{table_name}_{ts_str}.parquet"

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        local_path = tmp.name

    try:
        df.to_parquet(local_path, engine="fastparquet", index=False)
        s3.upload_file(local_path, MINIO_BUCKET, s3_key)
        logger.info(
            "Uploaded %s records to s3://%s/%s",
            len(records),
            MINIO_BUCKET,
            s3_key,
        )
    except Exception:
        logger.exception("Failed to upload parquet for table: %s", table_name)
        raise
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)


def flush_topic(topic: str) -> None:
    table_name = extract_table_name(topic)
    records = buffer[topic]

    if records:
        logger.info(
            "Flushing %s buffered record(s) for table: %s",
            len(records),
            table_name,
        )
        write_to_minio(table_name, records)
        buffer[topic] = []


def flush_all() -> None:
    logger.info("Flushing remaining buffered records...")
    for topic in TOPICS:
        flush_topic(topic)
    logger.info("Flush complete.")


def should_flush() -> bool:
    global last_flush_time

    if any(len(records) >= BATCH_SIZE for records in buffer.values()):
        return True

    if (time.time() - last_flush_time) >= FLUSH_INTERVAL_SECONDS:
        return True

    return False


def shutdown_handler(signum, frame) -> None:
    global consumer

    logger.warning("Received signal %s. Shutting down gracefully...", signum)
    try:
        flush_all()
    finally:
        if consumer is not None:
            consumer.close()
            logger.info("Consumer closed.")
        sys.exit(0)


def run() -> None:
    global consumer, s3, last_flush_time

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    consumer = get_kafka_consumer()
    s3 = get_s3_client()
    ensure_bucket_exists(s3)

    logger.info("Connected to Kafka. Listening for messages...")

    try:
        while True:
            message_batch = consumer.poll(timeout_ms=1000)

            for _, messages in message_batch.items():
                for message in messages:
                    topic = message.topic
                    event = message.value

                    if not isinstance(event, dict):
                        logger.warning("Skipping non-dict event from topic: %s", topic)
                        continue

                    payload = event.get("payload", {})
                    if not isinstance(payload, dict):
                        logger.warning(
                            "Skipping event with invalid payload from topic: %s",
                            topic,
                        )
                        continue

                    record = enrich_cdc_record(payload)

                    if record:
                        buffer[topic].append(record)
                        logger.info(
                            "[%s] op=%s id=%s",
                            topic,
                            record.get("_cdc_op"),
                            record.get("id", "unknown"),
                        )

            if should_flush():
                for topic in TOPICS:
                    flush_topic(topic)
                last_flush_time = time.time()

    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
    finally:
        flush_all()
        if consumer is not None:
            consumer.close()
        logger.info("Consumer stopped.")


if __name__ == "__main__":
    run()