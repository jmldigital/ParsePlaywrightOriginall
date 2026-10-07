from decimal import Decimal

import pytest

from price_api.stparts import (
    SourceError,
    brand_matches,
    delivery_days,
    price_value,
    select_offer,
)


@pytest.mark.parametrize(
    "value,expected",
    [
        ("1", 1),
        ("1-2 дня", 1),
        ("2–5", 2),
        ("в наличии", 0),
        ("сегодня", 0),
        ("03.10.2026", None),
        ("24 часа", None),
        ("", None),
    ],
)
def test_delivery(value, expected):
    assert delivery_days(value) == expected


def test_price_and_minimum_with_deadline():
    assert price_value("1 234,50 ₽") == Decimal("1234.50")
    assert price_value("по запросу") is None
    rows = [
        dict(brand=b, price=p, delivery=d)
        for b, p, d in [
            ("Toyota", "500", "1"),
            ("Toyota", "100", "3"),
            ("Toyota", "250", "1–2"),
            ("BMW", "1", "1"),
            ("Toyota", "300", "в наличии"),
        ]
    ]
    result = select_offer(rows, "TOYOTA", 2)
    assert (result.price, result.delivery_days) == ("250.00", 1)
    assert select_offer(rows, "TOYOTA", 0).price == "300.00"
    assert select_offer(rows, "AMG", 2).status == "not_found"
    assert not brand_matches("Toyota /", "BMW /")
    with pytest.raises(SourceError):
        select_offer([dict(brand="Toyota", price="?", delivery="1")], "Toyota", 2)


def test_delivery_range_uses_earliest_day():
    rows = [
        dict(brand="Toyota", price="17593.40", delivery="1 - 3 дня"),
        dict(brand="Toyota", price="17000", delivery="1 - 4 дня"),
        dict(brand="Toyota", price="100", delivery="3 - 4 дня"),
    ]
    result = select_offer(rows, "Toyota", 2)
    assert (result.status, result.price, result.delivery_days) == (
        "found",
        "17000.00",
        1,
    )
    assert select_offer(rows, "Toyota", 0).status == "not_found"
