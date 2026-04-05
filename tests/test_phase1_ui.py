from __future__ import annotations

import pytest

from fp_ineq.phase1_ui import (
    _PHASE1_FLAT_TAIL_CHECKS,
    _apply_manifest_patches,
    _collect_ui_ladder_points,
    _derive_trlowz_series,
    _interpolate_ui_factor_for_target,
    _load_phase1_manifest,
    _movement_summary,
    _refined_ui_ladder_specs,
    _ui_ladder_specs_from_trial_delta,
)


def test_apply_manifest_patches_rewrites_stock_touchpoints() -> None:
    manifest = _load_phase1_manifest()
    stock_text = "\n".join(
        [
            "CREATE C=1;",
            "LHS UB=EXP(LUB);",
            "GENR TRGH=TRGHQ*GDPD;",
            "GENR TRSH=TRSHQ*GDPD;",
        ]
    )
    composed = _apply_manifest_patches(stock_text, manifest)
    assert "INPUT FILE=ipolicy_base.txt;" in composed
    assert "INPUT FILE=idist_identities.txt;" in composed
    assert "LHS UB=EXP(LUB)*UIFAC;" in composed
    assert "GENR TRGH=(TRGHQ+SNAPDELTAQ)*GDPD;" in composed
    assert "GENR TRSH=(TRSHQ*SSFAC)*GDPD;" in composed


def test_movement_summary_requires_macro_movement() -> None:
    results = {
        "baseline-observed": {"UB": 1.0, "YD": 2.0, "GDPR": 3.0, "UR": 4.0, "PCY": 5.0, "PIEF": 6.0, "SG": 7.0, "EXPG": 8.0},
        "ui-relief": {"UB": 1.1, "YD": 2.1, "GDPR": 3.1, "UR": 3.9, "PCY": 5.1, "PIEF": 6.0, "SG": 7.0, "EXPG": 8.0},
        "ui-shock": {"UB": 0.9, "YD": 1.9, "GDPR": 2.9, "UR": 4.1, "PCY": 4.9, "PIEF": 6.0, "SG": 7.0, "EXPG": 8.0},
    }
    summary = _movement_summary(results)
    assert summary["passes_core"] is True
    assert summary["required_moves"] == {"UB": True, "YD": True, "GDPR": True}
    assert summary["one_of_moves"]["UR"] is True


def test_flat_tail_health_check_ignores_exogenous_ub_path() -> None:
    assert "UB" not in _PHASE1_FLAT_TAIL_CHECKS
    assert _PHASE1_FLAT_TAIL_CHECKS == ("YD", "GDPR", "UR", "RS")


def test_ui_ladder_specs_scale_from_trial_delta() -> None:
    specs, calibration = _ui_ladder_specs_from_trial_delta(0.2)
    assert [(spec.variant_id, spec.ui_factor) for spec in specs] == [
        ("baseline-observed", 1.0),
        ("ui-small", pytest.approx(1.01)),
        ("ui-medium", pytest.approx(1.02)),
        ("ui-large", pytest.approx(1.03)),
    ]
    assert calibration["ui-small"]["target_mean_delta_trlowz"] == pytest.approx(0.1)
    assert calibration["ui-medium"]["target_mean_delta_trlowz"] == pytest.approx(0.2)
    assert calibration["ui-large"]["target_mean_delta_trlowz"] == pytest.approx(0.3)


def test_derive_trlowz_series_from_tracked_components() -> None:
    series = {
        "UB": [10.0, 12.0],
        "TRGH": [20.0, 22.0],
        "TRSH": [30.0, 34.0],
        "POP": [10.0, 10.0],
        "PH": [2.0, 2.0],
    }
    assert _derive_trlowz_series(series) == pytest.approx([3.0, 3.4])


def test_refined_ui_ladder_specs_interpolate_from_observed_points() -> None:
    _specs, initial_rungs = _ui_ladder_specs_from_trial_delta(0.15398358456067096)
    calibration = {
        "trial_ui_factor": 1.02,
        "trial_mean_delta_trlowz": 0.15398358456067096,
        "rungs": initial_rungs,
    }
    rung_results = {
        "ui-small": {
            "ui_factor": 1.01,
            "achieved_mean_delta_trlowz": 0.04204917876413161,
            "target_mean_delta_trlowz": 0.07699179228033548,
            "relative_error": 0.45,
            "passes_target": False,
        },
        "ui-medium": {
            "ui_factor": 1.02,
            "achieved_mean_delta_trlowz": 0.15398358456067096,
            "target_mean_delta_trlowz": 0.15398358456067096,
            "relative_error": 0.0,
            "passes_target": True,
        },
        "ui-large": {
            "ui_factor": 1.03,
            "achieved_mean_delta_trlowz": 0.4489378839306196,
            "target_mean_delta_trlowz": 0.23097537684100644,
            "relative_error": 0.94,
            "passes_target": False,
        },
    }
    points = _collect_ui_ladder_points(calibration=calibration, rung_results=rung_results)
    assert _interpolate_ui_factor_for_target(points=points, target_metric=0.07699179228033548) == pytest.approx(
        1.013121704472146
    )
    assert _interpolate_ui_factor_for_target(points=points, target_metric=0.23097537684100644) == pytest.approx(
        1.0226102956439285,
        rel=1e-6,
    )
    refined_specs, refined_calibration = _refined_ui_ladder_specs(
        calibration=calibration,
        rung_results=rung_results,
    )
    assert [(spec.variant_id, spec.ui_factor) for spec in refined_specs] == [
        ("baseline-observed", 1.0),
        ("ui-small", pytest.approx(1.013121704472146)),
        ("ui-medium", pytest.approx(1.02)),
        ("ui-large", pytest.approx(1.0226102956439285)),
    ]
    assert refined_calibration["rungs"]["ui-small"]["ui_factor"] == pytest.approx(1.013121704472146)
