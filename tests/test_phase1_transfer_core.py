from __future__ import annotations

import pytest

from fp_ineq.phase1_transfer_core import (
    _acceptance_summary,
    _collect_transfer_composite_points,
    _interpolate_transfer_alpha_for_target,
    _refined_transfer_composite_specs,
    _required_moves_for_variant,
    _transfer_composite_specs_from_targets,
)


def test_required_moves_follow_transfer_family() -> None:
    assert _required_moves_for_variant("ui-relief") == ("UB", "YD", "GDPR")
    assert _required_moves_for_variant("federal-transfer-relief") == ("TRGH", "YD", "GDPR")
    assert _required_moves_for_variant("state-local-transfer-relief") == ("TRSH", "YD", "GDPR")
    assert _required_moves_for_variant("transfer-package-relief") == (
        "UB",
        "TRGH",
        "TRSH",
        "YD",
        "GDPR",
    )
    assert _required_moves_for_variant("transfer-composite-small") == (
        "UB",
        "TRGH",
        "TRSH",
        "YD",
        "GDPR",
    )


def test_acceptance_summary_checks_family_specific_transmission() -> None:
    baseline = {
        "UB": 10.0,
        "TRGH": 20.0,
        "TRSH": 30.0,
        "YD": 40.0,
        "GDPR": 50.0,
        "UR": 0.05,
        "PCY": 2.0,
        "PIEF": 60.0,
        "SG": -10.0,
        "EXPG": 70.0,
        "RS": 4.0,
        "RB": 3.0,
        "RM": 4.5,
        "SH": 80.0,
        "AH": 90.0,
    }
    results = {
        "baseline-observed": baseline,
        "ui-relief": {**baseline, "UB": 11.0, "YD": 41.0, "GDPR": 51.0, "UR": 0.049},
        "federal-transfer-relief": {
            **baseline,
            "TRGH": 21.0,
            "YD": 40.5,
            "GDPR": 50.4,
            "PCY": 2.01,
        },
        "state-local-transfer-relief": {
            **baseline,
            "TRSH": 31.0,
            "YD": 40.8,
            "GDPR": 50.6,
            "UR": 0.0495,
        },
        "transfer-package-relief": {
            **baseline,
            "UB": 11.5,
            "TRGH": 21.0,
            "TRSH": 31.2,
            "YD": 42.0,
            "GDPR": 51.3,
            "UR": 0.0485,
        },
    }
    summary = _acceptance_summary(results)
    assert summary["passes_core"] is True
    assert summary["scenario_checks"]["ui-relief"]["required_moves"] == {
        "UB": True,
        "YD": True,
        "GDPR": True,
    }
    assert summary["scenario_checks"]["ui-relief"]["required_signs"] == {
        "UB": True,
        "YD": True,
        "GDPR": True,
    }
    assert summary["scenario_checks"]["federal-transfer-relief"]["required_moves"] == {
        "TRGH": True,
        "YD": True,
        "GDPR": True,
    }
    assert summary["scenario_checks"]["federal-transfer-relief"]["required_signs"] == {
        "TRGH": True,
        "YD": True,
        "GDPR": True,
    }
    assert summary["scenario_checks"]["state-local-transfer-relief"]["required_moves"] == {
        "TRSH": True,
        "YD": True,
        "GDPR": True,
    }
    assert summary["scenario_checks"]["transfer-package-relief"]["required_moves"] == {
        "UB": True,
        "TRGH": True,
        "TRSH": True,
        "YD": True,
        "GDPR": True,
    }
    assert summary["scenario_checks"]["transfer-package-relief"]["one_of_signs"]["UR"] is True


def test_acceptance_summary_rejects_wrong_direction() -> None:
    baseline = {
        "UB": 10.0,
        "TRGH": 20.0,
        "TRSH": 30.0,
        "YD": 40.0,
        "GDPR": 50.0,
        "UR": 0.05,
        "PCY": 2.0,
        "PIEF": 60.0,
        "SG": -10.0,
        "EXPG": 70.0,
        "RS": 4.0,
        "RB": 3.0,
        "RM": 4.5,
        "SH": 80.0,
        "AH": 90.0,
    }
    results = {
        "baseline-observed": baseline,
        "ui-relief": {**baseline, "UB": 11.0, "YD": 41.0, "GDPR": 51.0, "UR": 0.051},
    }
    summary = _acceptance_summary(results)
    assert summary["passes_core"] is False
    assert summary["scenario_checks"]["ui-relief"]["one_of_signs"] == {
        "UR": False,
        "PCY": False,
    }


def test_transfer_composite_specs_scale_trial_package() -> None:
    specs, rungs = _transfer_composite_specs_from_targets(
        targets={
            "transfer-composite-small": 0.1,
            "transfer-composite-medium": 0.2,
            "transfer-composite-large": 0.3,
        },
        trial_delta=0.2,
    )
    assert [(spec.variant_id, spec.ui_factor, spec.trgh_delta_q, spec.trsh_factor) for spec in specs] == [
        ("baseline-observed", 1.0, 0.0, 1.0),
        ("transfer-composite-small", pytest.approx(1.01), pytest.approx(1.0), pytest.approx(1.01)),
        ("transfer-composite-medium", pytest.approx(1.02), pytest.approx(2.0), pytest.approx(1.02)),
        ("transfer-composite-large", pytest.approx(1.03), pytest.approx(3.0), pytest.approx(1.03)),
    ]
    assert rungs["transfer-composite-medium"]["alpha"] == pytest.approx(1.0)


def test_refined_transfer_composite_specs_interpolate_observed_points() -> None:
    _specs, rungs = _transfer_composite_specs_from_targets(
        targets={
            "transfer-composite-small": 0.1,
            "transfer-composite-medium": 0.2,
            "transfer-composite-large": 0.3,
        },
        trial_delta=0.2,
    )
    calibration = {
        "trial_mean_delta_trlowz": 0.2,
        "rungs": rungs,
    }
    rung_results = {
        "transfer-composite-small": {
            "alpha": 0.5,
            "achieved_mean_delta_trlowz": 0.07,
            "target_mean_delta_trlowz": 0.1,
            "relative_error": 0.3,
            "passes_target": False,
        },
        "transfer-composite-medium": {
            "alpha": 1.0,
            "achieved_mean_delta_trlowz": 0.2,
            "target_mean_delta_trlowz": 0.2,
            "relative_error": 0.0,
            "passes_target": True,
        },
        "transfer-composite-large": {
            "alpha": 1.5,
            "achieved_mean_delta_trlowz": 0.38,
            "target_mean_delta_trlowz": 0.3,
            "relative_error": 0.26,
            "passes_target": False,
        },
    }
    points = _collect_transfer_composite_points(calibration=calibration, rung_results=rung_results)
    assert _interpolate_transfer_alpha_for_target(points=points, target_metric=0.1) == pytest.approx(0.6153846154)
    assert _interpolate_transfer_alpha_for_target(points=points, target_metric=0.3) == pytest.approx(1.2777777778)
    refined_specs, refined_calibration = _refined_transfer_composite_specs(
        calibration=calibration,
        rung_results=rung_results,
    )
    assert [(spec.variant_id, spec.ui_factor, spec.trgh_delta_q, spec.trsh_factor) for spec in refined_specs] == [
        ("baseline-observed", 1.0, 0.0, 1.0),
        (
            "transfer-composite-small",
            pytest.approx(1.0123076923),
            pytest.approx(1.2307692308),
            pytest.approx(1.0123076923),
        ),
        ("transfer-composite-medium", pytest.approx(1.02), pytest.approx(2.0), pytest.approx(1.02)),
        (
            "transfer-composite-large",
            pytest.approx(1.0255555556),
            pytest.approx(2.5555555556),
            pytest.approx(1.0255555556),
        ),
    ]
    assert refined_calibration["rungs"]["transfer-composite-large"]["alpha"] == pytest.approx(1.2777777778)
