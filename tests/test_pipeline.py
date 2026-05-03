import importlib.util
import pathlib
import sys
from decimal import Decimal
from types import SimpleNamespace

ROOT = pathlib.Path(__file__).resolve().parents[1]


class DummyKafkaConsumer:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def poll(self, timeout_ms=1000):
        return {}

    def close(self):
        return None


class DummyS3Client:
    def list_buckets(self):
        return {"Buckets": [{"Name": "raw"}]}

    def create_bucket(self, Bucket):
        return None

    def upload_file(self, Filename, Bucket, Key):
        return None


def load_consumer_module():
    """
    Load consumer/kafka_to_minio.py safely by mocking external dependencies
    initialized at import time.
    """
    module_path = ROOT / "consumer" / "kafka_to_minio.py"
    spec = importlib.util.spec_from_file_location(
        "kafka_to_minio_test_module",
        module_path,
    )
    module = importlib.util.module_from_spec(spec)

    fake_kafka_module = SimpleNamespace(KafkaConsumer=DummyKafkaConsumer)
    fake_boto3_module = SimpleNamespace(client=lambda *args, **kwargs: DummyS3Client())

    original_kafka = sys.modules.get("kafka")
    original_boto3 = sys.modules.get("boto3")

    sys.modules["kafka"] = fake_kafka_module
    sys.modules["boto3"] = fake_boto3_module

    try:
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module
    finally:
        if original_kafka is not None:
            sys.modules["kafka"] = original_kafka
        else:
            sys.modules.pop("kafka", None)

        if original_boto3 is not None:
            sys.modules["boto3"] = original_boto3
        else:
            sys.modules.pop("boto3", None)


consumer = load_consumer_module()


def test_extract_table_name():
    assert consumer.extract_table_name("banking_server.public.customers") == "customers"
    assert consumer.extract_table_name("banking_server.public.accounts") == "accounts"
    assert consumer.extract_table_name("banking_server.public.transactions") == "transactions"


def test_enrich_cdc_insert():
    payload = {
        "op": "c",
        "after": {"id": 1, "name": "Alice"},
        "ts_ms": 123456789,
    }

    result = consumer.enrich_cdc_record(payload)

    assert result is not None
    assert result["id"] == 1
    assert result["name"] == "Alice"
    assert result["_cdc_op"] == "c"
    assert result["_cdc_ts"] == 123456789
    assert result["_is_deleted"] is False
    assert "_ingested_at" in result


def test_enrich_cdc_update():
    payload = {
        "op": "u",
        "after": {"id": 2, "name": "Bob"},
        "ts_ms": 123456789,
    }

    result = consumer.enrich_cdc_record(payload)

    assert result is not None
    assert result["id"] == 2
    assert result["_cdc_op"] == "u"
    assert result["_is_deleted"] is False


def test_enrich_cdc_snapshot_read():
    payload = {
        "op": "r",
        "after": {"id": 10, "name": "Snapshot User"},
        "ts_ms": 111,
    }

    result = consumer.enrich_cdc_record(payload)

    assert result is not None
    assert result["id"] == 10
    assert result["_cdc_op"] == "r"
    assert result["_is_deleted"] is False


def test_enrich_cdc_delete():
    payload = {
        "op": "d",
        "before": {"id": 3, "name": "Charlie"},
        "ts_ms": 123456789,
    }

    result = consumer.enrich_cdc_record(payload)

    assert result is not None
    assert result["_cdc_op"] == "d"
    assert result["_cdc_ts"] == 123456789
    assert result["_is_deleted"] is True
    assert result["id"] == 3
    assert result["name"] == "Charlie"


def test_enrich_cdc_invalid_op():
    payload = {
        "op": "x",
    }

    result = consumer.enrich_cdc_record(payload)
    assert result is None


def test_enrich_cdc_missing_record():
    payload = {
        "op": "c",
        "after": None,
        "ts_ms": 123,
    }

    result = consumer.enrich_cdc_record(payload)
    assert result is None


def test_enrich_cdc_non_dict_record():
    payload = {
        "op": "c",
        "after": "not-a-dict",
        "ts_ms": 123,
    }

    result = consumer.enrich_cdc_record(payload)
    assert result is None


def test_decimal_conversion():
    payload = {
        "op": "c",
        "after": {"amount": Decimal("10.50")},
        "ts_ms": 123,
    }

    result = consumer.enrich_cdc_record(payload)

    assert result is not None
    assert isinstance(result["amount"], float)
    assert result["amount"] == 10.5