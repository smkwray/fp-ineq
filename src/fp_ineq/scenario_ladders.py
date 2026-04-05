from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence

__all__ = [
    "FIRST_YEAR_TRLOWZ_PERIODS",
    "LadderTarget",
    "UI_LADDER_RUNGS",
    "build_relative_ladder_targets",
    "solve_linear_lever_value",
    "mean_delta_over_periods",
]


FIRST_YEAR_TRLOWZ_PERIODS = ("2026.1", "2026.2", "2026.3", "2026.4")
UI_LADDER_RUNGS = (
    ("ui-small", 0.5),
    ("ui-medium", 1.0),
    ("ui-large", 1.5),
)


@dataclass(frozen=True)
class LadderTarget:
    variant_id: str
    scale: float
    target_metric: float


def build_relative_ladder_targets(
    *,
    trial_metric: float,
    rung_specs: Sequence[tuple[str, float]] = UI_LADDER_RUNGS,
) -> list[LadderTarget]:
    if abs(float(trial_metric)) <= 1e-12:
        raise ValueError("trial_metric must be non-zero")
    return [
        LadderTarget(
            variant_id=str(variant_id),
            scale=float(scale),
            target_metric=float(trial_metric) * float(scale),
        )
        for variant_id, scale in rung_specs
    ]


def solve_linear_lever_value(
    *,
    neutral_lever: float,
    trial_lever: float,
    trial_metric: float,
    target_metric: float,
) -> float:
    lever_delta = float(trial_lever) - float(neutral_lever)
    if abs(lever_delta) <= 1e-12:
        raise ValueError("trial_lever must differ from neutral_lever")
    if abs(float(trial_metric)) <= 1e-12:
        raise ValueError("trial_metric must be non-zero")
    scale = float(target_metric) / float(trial_metric)
    return float(neutral_lever) + scale * lever_delta


def mean_delta_over_periods(
    *,
    periods: Sequence[str],
    baseline_values: Sequence[float],
    scenario_values: Sequence[float],
    target_periods: Sequence[str] = FIRST_YEAR_TRLOWZ_PERIODS,
) -> float:
    if len(periods) != len(baseline_values) or len(periods) != len(scenario_values):
        raise ValueError("periods, baseline_values, and scenario_values must have the same length")
    deltas = [
        float(scenario_values[idx]) - float(baseline_values[idx])
        for idx, period in enumerate(periods)
        if period in target_periods
    ]
    if not deltas:
        wanted = ", ".join(target_periods)
        raise ValueError(f"No target periods found in series window: {wanted}")
    return sum(deltas) / len(deltas)
