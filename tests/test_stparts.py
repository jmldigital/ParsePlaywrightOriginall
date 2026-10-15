from decimal import Decimal

import pytest

from price_api.stparts import (
    SourceError,
    brand_matches,
    delivery_days,
    price_value,
    search_urls,
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


def test_unavailable_row_is_not_an_offer():
    # The source marks a part it has no offer for with its own flag and empty cells.
    empty = dict(brand="Toyota", price="", delivery="", unavailable=True)
    assert select_offer([empty], "Toyota", 2).status == "not_found"
    # A readable row next to it still gives its price.
    readable = dict(brand="Toyota", price="500", delivery="1")
    assert select_offer([empty, readable], "Toyota", 2).price == "500.00"
    # A row that does carry text we cannot read must still refuse to pick a minimum.
    with pytest.raises(SourceError):
        select_offer(
            [dict(brand="Toyota", price="?", delivery="", unavailable=True)],
            "Toyota",
            2,
        )
    # Without the source flag an empty row stays an unknown format, as before.
    with pytest.raises(SourceError):
        select_offer([dict(brand="Toyota", price="", delivery="")], "Toyota", 2)


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


def test_search_goes_by_part_code_first_and_keeps_the_brand_path():
    primary, fallback = search_urls("Great Wall", "5402127XKQ00AF4")
    assert primary == "https://stparts.ru/search?pcode=5402127XKQ00AF4"
    assert fallback == "https://stparts.ru/search/Great%20Wall/5402127XKQ00AF4"
    assert search_urls("Toyota", "123/45")[1].endswith("/search/Toyota/123%2F45")


def test_brand_match_is_containment_of_whole_words():
    assert brand_matches("Great Wall", "Great Wall")
    assert brand_matches("great wall", "GREAT WALL")
    assert brand_matches("Victor-Reinz", "VICTOR REINZ")
    # The source labels one maker with a combined name; its parts must match too.
    assert brand_matches("KIA", "Hyundai-KIA")
    assert brand_matches("Hyundai", "Hyundai-KIA")
    assert brand_matches("Hyundai-KIA", "KIA")
    # Alignment is by whole words: a fragment of a word is not a brand.
    assert not brand_matches("kia", "Kiabi")
    # A separately named analog is a different brand, not a match.
    assert not brand_matches("haval", "Great Wall")
    assert not brand_matches("Great Wall", "WHCQ")
    assert not brand_matches("", "Great Wall")
    assert not brand_matches("Great Wall", "")


def test_offer_of_combined_brand_label_is_selected_for_its_part():
    rows = [
        dict(brand="Hyundai-KIA", price="39,60", delivery="1 - 3 дня"),
        dict(brand="Hyundai-KIA", price="46,64", delivery="4 - 6 дней"),
        dict(brand="Sat", price="10,00", delivery="1"),
    ]
    result = select_offer(rows, "KIA", 2)
    assert (result.status, result.price, result.delivery_days) == (
        "found",
        "39.60",
        1,
    )
    # A cheaper brand we share no whole word with is still not our brand.
    assert (
        select_offer(
            [dict(brand="TORK", price="148.40", delivery="1")], "Toyota", 2
        ).status
        == "not_found"
    )
