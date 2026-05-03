import json
import logging
import os
import sys
from typing import Any

import requests
from dotenv import load_dotenv

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083/connectors")
CONNECTOR_NAME = os.getenv("DEBEZIUM_CONNECTOR_NAME", "postgres-connector")


def build_connector_config() -> dict[str, Any]:
    return {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": os.getenv("POSTGRES_HOST_DOCKER", "postgres"),
            "database.port": os.getenv("POSTGRES_PORT", "5432"),
            "database.user": os.getenv("POSTGRES_USER"),
            "database.password": os.getenv("POSTGRES_PASSWORD"),
            "database.dbname": os.getenv("POSTGRES_DB"),
            "topic.prefix": os.getenv("DEBEZIUM_TOPIC_PREFIX", "banking_server"),
            "table.include.list": "public.customers,public.accounts,public.transactions",
            "plugin.name": "pgoutput",
            "slot.name": os.getenv("DEBEZIUM_SLOT_NAME", "banking_slot"),
            "publication.autocreate.mode": "filtered",
            "tombstones.on.delete": "false",
            "decimal.handling.mode": "double",
            "snapshot.mode": os.getenv("DEBEZIUM_SNAPSHOT_MODE", "initial"),
        },
    }


def get_connector_url() -> str:
    return f"{CONNECT_URL.rstrip('/')}/{CONNECTOR_NAME}"


def connector_exists() -> bool:
    response = requests.get(get_connector_url(), timeout=10)
    return response.status_code == 200


def create_connector(config: dict[str, Any]) -> None:
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        CONNECT_URL,
        headers=headers,
        data=json.dumps(config),
        timeout=15,
    )

    if response.status_code == 201:
        logger.info("Connector '%s' created successfully.", CONNECTOR_NAME)
    elif response.status_code == 409:
        logger.warning("Connector '%s' already exists.", CONNECTOR_NAME)
    else:
        raise RuntimeError(
            f"Failed to create connector ({response.status_code}): {response.text}"
        )


def update_connector(config: dict[str, Any]) -> None:
    headers = {"Content-Type": "application/json"}
    response = requests.put(
        f"{get_connector_url()}/config",
        headers=headers,
        data=json.dumps(config["config"]),
        timeout=15,
    )

    if response.status_code in (200, 201):
        logger.info("Connector '%s' updated successfully.", CONNECTOR_NAME)
    else:
        raise RuntimeError(
            f"Failed to update connector ({response.status_code}): {response.text}"
        )


def main() -> None:
    try:
        config = build_connector_config()

        logger.info("Connecting to Kafka Connect at: %s", CONNECT_URL)
        logger.info("Connector name: %s", CONNECTOR_NAME)
        logger.info(
            "Using Postgres host for Debezium: %s",
            config["config"]["database.hostname"],
        )

        if connector_exists():
            logger.info("Connector already exists. Updating config...")
            update_connector(config)
        else:
            logger.info("Connector does not exist. Creating new connector...")
            create_connector(config)

    except requests.exceptions.RequestException:
        logger.exception("Request error while connecting to Kafka Connect")
        sys.exit(1)
    except Exception:
        logger.exception("Connector registration failed")
        sys.exit(1)


if __name__ == "__main__":
    main()