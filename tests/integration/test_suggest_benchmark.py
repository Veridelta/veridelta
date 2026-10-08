# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Hold `suggest` and `crosswalk` to noise injected on purpose, beside decoys they must leave alone.

One wide table carries a column per case. A noise column holds a difference a
rule should explain, such as rounding or padding, and expects the exact rule,
with every injected row explained. A decoy holds a real change, or noise mixed
with one, and expects no rule at all, since a rule that forgives a real change
hides it in every later run. The run is graded per column, so a failure names
the column, what was expected, and what was proposed.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any, Final

import polars as pl
import pytest

from veridelta.engine import DiffEngine
from veridelta.models import DiffConfig

if TYPE_CHECKING:
    from collections.abc import Callable

    from veridelta.models import RuleSuggestion, ValueMapProposal

pytestmark = [pytest.mark.integration, pytest.mark.slow]

ROWS: Final = 200
"""Rows in the table, keyed `0` to `ROWS - 1`."""


@dataclass(frozen=True)
class Column:
    """One column of the table, and the suggestion it should get.

    Attributes:
        name (str): The column name.
        source (list[Any]): Source values, one per row.
        target (list[Any]): Target values, one per row.
        settings (dict[str, Any] | None): The settings `suggest` should
            propose, or None for a decoy that must get no suggestion.
        explained (int): Rows the suggestion should explain.
        differing (int): Rows that differ in the column before any rule.
    """

    name: str
    source: list[Any]
    target: list[Any]
    settings: dict[str, Any] | None
    explained: int = 0
    differing: int = 0


def _rows(rng: random.Random, count: int) -> set[int]:
    return set(rng.sample(range(ROWS), count))


def _with(values: list[Any], rows: set[int], change: Callable[[Any], Any]) -> list[Any]:
    return [change(value) if index in rows else value for index, value in enumerate(values)]


def _columns(seed: int = 11) -> list[Column]:
    """Build every graded column from one seed."""
    rng = random.Random(seed)
    fares = [round(rng.uniform(5, 500), 2) for _ in range(ROWS)]
    prices = [round(rng.uniform(100, 100_000), 2) for _ in range(ROWS)]
    regions = [rng.choice(("north", "south", "east", "west")) for _ in range(ROWS)]
    statuses = [rng.choice(("open", "shipped", "closed")) for _ in range(ROWS)]
    notes = [f"order {index}" for index in range(ROWS)]
    scores = [round(rng.uniform(0, 100), 1) for _ in range(ROWS)]
    days = [date(2026, 1, 1) + timedelta(days=rng.randint(0, 364)) for _ in range(ROWS)]
    quantities = [rng.randint(1, 50) for _ in range(ROWS)]
    columns: list[Column] = []

    rows = _rows(rng, 30)
    gaps = {index: rng.choice((0.001, 0.002, 0.003, 0.004)) for index in rows}
    columns.append(
        Column(
            "fare_rounded",
            fares,
            [round(fare + gaps[i], 3) if i in rows else fare for i, fare in enumerate(fares)],
            {"absolute_tolerance": 0.005},
            30,
            30,
        )
    )
    rows = _rows(rng, 30)
    columns.append(
        Column(
            "price_scaled",
            prices,
            _with(prices, rows, lambda price: price * 1.003),
            {"relative_tolerance": 0.005},
            30,
            30,
        )
    )
    rows = _rows(rng, 25)
    columns.append(
        Column(
            "region_recased",
            regions,
            _with(regions, rows, str.upper),
            {"case_insensitive": True},
            25,
            25,
        )
    )
    rows = _rows(rng, 25)
    columns.append(
        Column(
            "region_padded",
            regions,
            _with(regions, rows, lambda region: f"  {region} "),
            {"whitespace_mode": "both"},
            25,
            25,
        )
    )
    rows = _rows(rng, 20)
    columns.append(
        Column(
            "status_recased_and_padded",
            statuses,
            _with(statuses, rows, lambda status: f" {status.upper()} "),
            {"case_insensitive": True, "whitespace_mode": "both"},
            20,
            20,
        )
    )
    rows = _rows(rng, 15)
    columns.append(
        Column(
            "note_sentinel",
            _with(notes, rows, lambda _: None),
            _with(notes, rows, lambda _: "N/A"),
            {"null_values": ["N/A"]},
            15,
            15,
        )
    )
    rows = _rows(rng, 12)
    columns.append(
        Column(
            "score_sentinel",
            _with(scores, rows, lambda _: None),
            _with(scores, rows, lambda _: -999.0),
            {"null_values": [-999.0]},
            12,
            12,
        )
    )
    columns.append(
        Column(
            "shipped_on_text",
            [day.strftime("%d/%m/%Y") for day in days],
            days,
            {"datetime_format": "%d/%m/%Y"},
            ROWS,
            ROWS,
        )
    )
    recased = _rows(rng, 10)
    edited = _rows(rng, 30) - recased
    edited = set(sorted(edited)[:4])
    columns.append(
        Column(
            "status_recased_beside_edits",
            statuses,
            [
                status.upper() if i in recased else ("cancelled" if i in edited else status)
                for i, status in enumerate(statuses)
            ],
            {"case_insensitive": True},
            10,
            14,
        )
    )

    rows = _rows(rng, 20)
    columns.append(
        Column("fare_moved_five_percent", fares, _with(fares, rows, lambda f: f * 1.05), None)
    )
    rounded = sorted(_rows(rng, 16))
    columns.append(
        Column(
            "fare_rounded_beside_one_real_change",
            fares,
            [
                fare * 1.3
                if i == rounded[0]
                else (round(fare + 0.002, 3) if i in rounded else fare)
                for i, fare in enumerate(fares)
            ],
            None,
        )
    )
    rows = _rows(rng, 10)
    columns.append(
        Column(
            "status_edited",
            statuses,
            _with(statuses, rows, lambda status: "closed" if status != "closed" else "open"),
            None,
        )
    )
    rows = _rows(rng, 8)
    columns.append(
        Column("note_sentinel_over_a_value", notes, _with(notes, rows, lambda _: "N/A"), None)
    )
    columns.append(
        Column(
            "shipped_on_text_a_day_late",
            [day.strftime("%d/%m/%Y") for day in days],
            [day + timedelta(days=1) for day in days],
            None,
        )
    )
    rows = _rows(rng, 10)
    columns.append(
        Column(
            "quantity_changed",
            quantities,
            _with(quantities, rows, lambda quantity: quantity + rng.randint(1, 3)),
            None,
        )
    )
    return columns


@dataclass(frozen=True)
class Codes:
    """A text column a value map should translate, and the entries `crosswalk` should propose.

    Attributes:
        name (str): The column name.
        source (list[str]): Source values.
        target (list[str]): Target codes.
        entries (dict[str, str]): The entries a proposal should hold, empty for
            a decoy that must get no proposal.
    """

    name: str
    source: list[str]
    target: list[str]
    entries: dict[str, str]


def _codes(seed: int = 13) -> list[Codes]:
    """Build every graded value-map column from one seed."""
    rng = random.Random(seed)
    payment = {"cash": "CSH", "card": "CRD", "voucher": "VCH"}
    paid = [rng.choice(sorted(payment)) for _ in range(ROWS)]
    channel = {"web": "W", "store": "S"}
    sold = [rng.choice(sorted(channel)) for _ in range(ROWS)]
    noisy = _rows(rng, 24)
    contact = {"phone": "P", "email": "E"}
    reached = [rng.choice(sorted(contact)) for _ in range(ROWS)]
    rare = sorted(_rows(rng, 3))
    reached = ["fax" if i in rare else value for i, value in enumerate(reached)]
    return [
        Codes("payment_coded", paid, [payment[value] for value in paid], payment),
        Codes(
            "channel_coded_with_noise",
            sold,
            [
                ("S" if value == "web" else "W") if i in noisy else channel[value]
                for i, value in enumerate(sold)
            ],
            {},
        ),
        Codes(
            "contact_coded_with_a_rare_value",
            reached,
            ["F" if value == "fax" else contact[value] for value in reached],
            contact,
        ),
    ]


COLUMNS: Final = _columns()
CODES: Final = _codes()


@pytest.fixture(scope="module")
def engine() -> DiffEngine:
    """Return one engine over the whole table, keyed on `id`."""
    source: dict[str, list[Any]] = {"id": list(range(ROWS))}
    target: dict[str, list[Any]] = {"id": list(range(ROWS))}
    every: list[Column | Codes] = [*COLUMNS, *CODES]
    for column in every:
        source[column.name] = column.source
        target[column.name] = column.target
    return DiffEngine(DiffConfig(primary_keys=["id"]), pl.LazyFrame(source), pl.LazyFrame(target))


@pytest.fixture(scope="module")
def suggestions(engine: DiffEngine) -> dict[str, RuleSuggestion]:
    """Return what `suggest` proposes, by column."""
    return {suggestion.column: suggestion for suggestion in engine.suggest_rules()}


@pytest.fixture(scope="module")
def proposals(engine: DiffEngine) -> dict[str, ValueMapProposal]:
    """Return what `crosswalk` proposes, by column."""
    return {proposal.column: proposal for proposal in engine.propose_value_maps()}


NOISE: Final = [column for column in COLUMNS if column.settings is not None]
DECOYS: Final = [column for column in COLUMNS if column.settings is None]


@pytest.mark.parametrize("column", NOISE, ids=lambda column: column.name)
def test_noise_gets_the_rule_that_explains_it(
    column: Column, suggestions: dict[str, RuleSuggestion]
) -> None:
    """Ensure each kind of noise gets its rule, explaining every injected row and no other."""
    suggestion = suggestions.get(column.name)

    assert suggestion is not None, f"{column.name}: expected {column.settings}, got nothing"
    assert suggestion.settings == column.settings
    assert (suggestion.explained, suggestion.differing) == (column.explained, column.differing)


@pytest.mark.parametrize("column", [*DECOYS, *CODES], ids=lambda column: column.name)
def test_a_real_change_gets_no_rule(
    column: Column | Codes, suggestions: dict[str, RuleSuggestion]
) -> None:
    """Ensure no rule is proposed that would forgive a real change in later runs."""
    suggestion = suggestions.get(column.name)

    assert suggestion is None, f"{column.name}: expected nothing, got {suggestion.settings}"


@pytest.mark.parametrize("codes", CODES, ids=lambda codes: codes.name)
def test_crosswalk_proposes_only_the_entries_the_data_supports(
    codes: Codes, proposals: dict[str, ValueMapProposal]
) -> None:
    """Ensure each entry needs enough agreeing rows, and a noisy mapping gets none."""
    proposal = proposals.get(codes.name)

    assert (proposal.value_map if proposal else {}) == codes.entries


def test_crosswalk_proposes_nothing_for_columns_a_map_cannot_explain(
    proposals: dict[str, ValueMapProposal],
) -> None:
    """Ensure no column outside the coded ones gets a value map."""
    coded = {codes.name for codes in CODES}

    assert {column for column in proposals if column not in coded} == set()


class TestTheDecoysHaveTeeth:
    """A decoy proves nothing unless a looser setting would fall for it."""

    def test_a_wider_share_forgives_the_five_percent_move(self, engine: DiffEngine) -> None:
        """Ensure the 5% decoy sits just past the default share, so the default is what holds."""
        loose = {s.column: s for s in engine.suggest_rules(max_share=0.1)}

        assert "fare_moved_five_percent" in loose

    def test_a_lower_confidence_maps_the_noisy_codes(self, engine: DiffEngine) -> None:
        """Ensure the noisy mapping fails only the default confidence, not every confidence."""
        loose = {p.column: p for p in engine.propose_value_maps(min_confidence=0.8)}

        assert "channel_coded_with_noise" in loose

    def test_a_lower_support_maps_the_rare_value(self, engine: DiffEngine) -> None:
        """Ensure the rare value is left out for its support alone."""
        loose = {p.column: p for p in engine.propose_value_maps(min_support=3)}

        assert loose["contact_coded_with_a_rare_value"].value_map.get("fax") == "F"
