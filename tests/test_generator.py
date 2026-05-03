import importlib.util
import pathlib
import sys
from decimal import Decimal

ROOT = pathlib.Path(__file__).resolve().parents[1]


def load_generator_module():
    module_path = ROOT / "data-generator" / "faker_generator.py"
    spec = importlib.util.spec_from_file_location("faker_generator_test_module", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_random_money_within_range():
    gen = load_generator_module()

    for _ in range(100):
        value = gen.random_money(Decimal("1.00"), Decimal("1000.00"))
        assert Decimal("1.00") <= value <= Decimal("1000.00")


def test_random_money_two_decimal_places():
    gen = load_generator_module()

    for _ in range(50):
        value = gen.random_money(Decimal("10.00"), Decimal("100.00"))
        assert value == value.quantize(Decimal("0.01"))


def test_parse_args_once_flag(monkeypatch):
    gen = load_generator_module()

    monkeypatch.setattr(sys, "argv", ["faker_generator.py", "--once"])
    args = gen.parse_args()

    assert args.once is True


def test_parse_args_default(monkeypatch):
    gen = load_generator_module()

    monkeypatch.setattr(sys, "argv", ["faker_generator.py"])
    args = gen.parse_args()

    assert args.once is False