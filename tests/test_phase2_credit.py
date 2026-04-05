from __future__ import annotations

from fp_ineq.phase2_credit import _acceptance_summary, _credit_scale_sweep_assessment


def test_credit_acceptance_summary_requires_effective_rate_direction_and_demand_move() -> None:
    baseline = {
        "RSA": 4.0,
        "RMA": 5.0,
        "RSAEFF": 4.0,
        "RMAEFF": 5.0,
        "CS": 100.0,
        "CN": 90.0,
        "CD": 80.0,
        "IHH": 70.0,
        "CUR": 60.0,
        "YD": 5000.0,
        "GDPR": 10000.0,
        "UR": 0.045,
        "PCY": 2.0,
        "IWGAP150": 3.2,
    }
    results = {
        "baseline-observed": baseline,
        "credit-easing": {**baseline, "RSAEFF": 3.0, "RMAEFF": 4.0, "CS": 101.0},
        "credit-tightening": {**baseline, "RSAEFF": 5.0, "RMAEFF": 6.0, "IHH": 69.0},
    }
    summary = _acceptance_summary(results)
    assert summary["passes_core"] is True
    assert summary["scenario_checks"]["credit-easing"]["required_signs"] == {
        "RSAEFF": True,
        "RMAEFF": True,
    }
    assert summary["scenario_checks"]["credit-easing"]["demand_moves"]["CS"] is True
    assert summary["scenario_checks"]["credit-tightening"]["required_signs"] == {
        "RSAEFF": True,
        "RMAEFF": True,
    }


def test_credit_acceptance_summary_rejects_wrong_rate_direction() -> None:
    baseline = {
        "RSA": 4.0,
        "RMA": 5.0,
        "RSAEFF": 4.0,
        "RMAEFF": 5.0,
        "CS": 100.0,
        "CN": 90.0,
        "CD": 80.0,
        "IHH": 70.0,
        "CUR": 60.0,
        "YD": 5000.0,
        "GDPR": 10000.0,
        "UR": 0.045,
        "PCY": 2.0,
        "IWGAP150": 3.2,
    }
    results = {
        "baseline-observed": baseline,
        "credit-easing": {**baseline, "RSAEFF": 4.5, "RMAEFF": 5.5, "CS": 101.0},
    }
    summary = _acceptance_summary(results)
    assert summary["passes_core"] is False
    assert summary["scenario_checks"]["credit-easing"]["required_signs"] == {
        "RSAEFF": False,
        "RMAEFF": False,
    }


def test_credit_scale_sweep_assessment_keeps_private_when_signal_is_tiny() -> None:
    assessment = _credit_scale_sweep_assessment(
        {
            1.0: {
                "credit-easing": {"RSAEFF": -1.0, "RMAEFF": -1.0, "CS": -1e-6, "GDPR": -2e-6},
                "credit-tightening": {"RSAEFF": 1.0, "RMAEFF": 1.0, "CS": 1e-6, "GDPR": 2e-6},
            },
            10.0: {
                "credit-easing": {"RSAEFF": -10.0, "RMAEFF": -10.0, "CS": -4e-5, "GDPR": -3e-5},
                "credit-tightening": {"RSAEFF": 10.0, "RMAEFF": 10.0, "CS": 2e-5, "GDPR": 2e-5},
            },
        }
    )
    assert assessment["publication_ready"] is False
    assert assessment["recommended_action"] == "keep_private_and_do_not_build_credit_ladder_yet"
    assert assessment["best_magnitude"] == 10.0


def test_credit_scale_sweep_assessment_allows_ladder_when_signal_clears_threshold() -> None:
    assessment = _credit_scale_sweep_assessment(
        {
            5.0: {
                "credit-easing": {"RSAEFF": -5.0, "RMAEFF": -5.0, "CS": -2e-4, "GDPR": -1.5e-4},
                "credit-tightening": {"RSAEFF": 5.0, "RMAEFF": 5.0, "CS": 1.8e-4, "GDPR": 1.2e-4},
            }
        }
    )
    assert assessment["publication_ready"] is True
    assert assessment["recommended_action"] == "build_private_credit_ladder"
