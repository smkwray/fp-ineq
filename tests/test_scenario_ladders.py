from __future__ import annotations

import pytest

from fp_ineq.scenario_ladders import (
    FIRST_YEAR_TRLOWZ_PERIODS,
    UI_LADDER_RUNGS,
    build_relative_ladder_targets,
    mean_delta_over_periods,
    solve_linear_lever_value,
)


def test_mean_delta_over_periods_uses_first_year_average_by_default() -> None:
    periods = ["2025.4", *FIRST_YEAR_TRLOWZ_PERIODS, "2027.1"]
    baseline = [0.9, 1.0, 1.1, 1.2, 1.3, 1.4]
    scenario = [0.9, 1.2, 1.2, 1.5, 1.7, 1.8]
    assert mean_delta_over_periods(
        periods=periods,
        baseline_values=baseline,
        scenario_values=scenario,
    ) == pytest.approx((0.2 + 0.1 + 0.3 + 0.4) / 4)


def test_mean_delta_over_periods_rejects_missing_target_window() -> None:
    with pytest.raises(ValueError, match="No target periods found"):
        mean_delta_over_periods(
            periods=["2028.1", "2028.2"],
            baseline_values=[1.0, 1.0],
            scenario_values=[1.1, 1.1],
        )


def test_build_relative_ladder_targets_scales_trial_metric() -> None:
    targets = build_relative_ladder_targets(trial_metric=0.24, rung_specs=UI_LADDER_RUNGS)
    assert [(item.variant_id, item.scale, item.target_metric) for item in targets] == [
        ("ui-small", 0.5, pytest.approx(0.12)),
        ("ui-medium", 1.0, pytest.approx(0.24)),
        ("ui-large", 1.5, pytest.approx(0.36)),
    ]


def test_solve_linear_lever_value_scales_from_trial_response() -> None:
    assert solve_linear_lever_value(
        neutral_lever=1.0,
        trial_lever=1.02,
        trial_metric=0.24,
        target_metric=0.12,
    ) == pytest.approx(1.01)
    assert solve_linear_lever_value(
        neutral_lever=1.0,
        trial_lever=1.02,
        trial_metric=0.24,
        target_metric=0.36,
    ) == pytest.approx(1.03)
