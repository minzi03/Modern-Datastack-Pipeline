import argparse
import logging
import os
import random
import sys
import time
from decimal import Decimal, ROUND_DOWN

import psycopg2
from dotenv import load_dotenv
from faker import Faker

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()

# -----------------------------
# Project configuration
# -----------------------------
NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", 10))
ACCOUNTS_PER_CUSTOMER = int(os.getenv("ACCOUNTS_PER_CUSTOMER", 2))
NUM_TRANSACTIONS = int(os.getenv("NUM_TRANSACTIONS", 50))
MAX_TXN_AMOUNT = Decimal(os.getenv("MAX_TXN_AMOUNT", "1000.00"))
CURRENCY = os.getenv("CURRENCY", "USD")

INITIAL_BALANCE_MIN = Decimal(os.getenv("INITIAL_BALANCE_MIN", "10.00"))
INITIAL_BALANCE_MAX = Decimal(os.getenv("INITIAL_BALANCE_MAX", "1000.00"))

DEFAULT_LOOP = os.getenv("DEFAULT_LOOP", "true").lower() == "true"
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", 2))
MAX_ITERATIONS = int(os.getenv("MAX_ITERATIONS", 0))  # 0 = unlimited

fake = Faker()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run fake banking data generator")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single iteration and exit",
    )
    return parser.parse_args()


def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    value = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return value.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def run_iteration(cur) -> tuple[int, int, int]:
    customers: list[int] = []

    # 1. Generate customers
    for _ in range(NUM_CUSTOMERS):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email()

        cur.execute(
            """
            INSERT INTO customers (first_name, last_name, email)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (first_name, last_name, email),
        )
        customer_id = cur.fetchone()[0]
        customers.append(customer_id)

    # 2. Generate accounts
    accounts: list[int] = []
    for customer_id in customers:
        for _ in range(ACCOUNTS_PER_CUSTOMER):
            account_type = random.choice(["SAVINGS", "CHECKING"])
            initial_balance = random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX)

            cur.execute(
                """
                INSERT INTO accounts (customer_id, account_type, balance, currency)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (customer_id, account_type, initial_balance, CURRENCY),
            )
            account_id = cur.fetchone()[0]
            accounts.append(account_id)

    # 3. Generate transactions
    txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
    for _ in range(NUM_TRANSACTIONS):
        account_id = random.choice(accounts)
        txn_type = random.choice(txn_types)
        amount = random_money(Decimal("1.00"), MAX_TXN_AMOUNT)
        related_account = None

        if txn_type == "TRANSFER" and len(accounts) > 1:
            related_account = random.choice([a for a in accounts if a != account_id])

        cur.execute(
            """
            INSERT INTO transactions (
                account_id, txn_type, amount, related_account_id, status
            )
            VALUES (%s, %s, %s, %s, 'COMPLETED')
            """,
            (account_id, txn_type, amount, related_account),
        )

    fake.unique.clear()
    return len(customers), len(accounts), NUM_TRANSACTIONS


def main() -> None:
    args = parse_args()
    loop_enabled = not args.once and DEFAULT_LOOP

    logger.info("Banking data generator starting...")
    logger.info(
        "Postgres: %s:%s db=%s",
        os.getenv("POSTGRES_HOST"),
        os.getenv("POSTGRES_PORT"),
        os.getenv("POSTGRES_DB"),
    )

    conn = None
    cur = None

    try:
        conn = get_connection()
        conn.autocommit = True
        cur = conn.cursor()

        logger.info("Connected to PostgreSQL")

        iteration = 0
        while True:
            iteration += 1
            logger.info("Iteration %s started", iteration)

            customer_count, account_count, transaction_count = run_iteration(cur)

            logger.info(
                "Generated %s customers, %s accounts, %s transactions.",
                customer_count,
                account_count,
                transaction_count,
            )
            logger.info("Iteration %s finished", iteration)

            if not loop_enabled:
                logger.info("Run-once mode enabled. Exiting cleanly.")
                break

            if MAX_ITERATIONS > 0 and iteration >= MAX_ITERATIONS:
                logger.info("Reached max iterations (%s). Exiting cleanly.", MAX_ITERATIONS)
                break

            time.sleep(SLEEP_SECONDS)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Exiting gracefully...")
    except Exception:
        logger.exception("Generator failed")
        sys.exit(1)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
        logger.info("PostgreSQL connection closed.")
        sys.exit(0)


if __name__ == "__main__":
    main()