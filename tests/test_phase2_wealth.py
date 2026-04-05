from __future__ import annotations

from fp_ineq.phase2_wealth import _wealth_maturity_assessment


def test_wealth_maturity_assessment_marks_expert_only_candidate() -> None:
    coefficient_report = {
        "equations": {
            "IWGAP150": {
                "effective_nobs": 11.0,
                "main_rmse": 0.025,
                "benchmarks": {"restricted_regressor": "LAAZ"},
                "loo": {
                    "rmse_mean": 0.05,
                    "rmse_std": 0.02,
                    "sign_stability": {"WG0": 9, "WGA": 9, "WGT": 11, "WGU": 11},
                },
            }
        }
    }
    run_report = {
        "scenarios": {
            "baseline-observed": {"last_levels": {"IWGAP150": 2.4}},
            "ui-relief": {"last_levels": {"IWGAP150": 2.49}},
            "ui-shock": {"last_levels": {"IWGAP150": 2.38}},
            "transfer-package-relief": {"last_levels": {"IWGAP150": 2.50}},
            "transfer-package-shock": {"last_levels": {"IWGAP150": 2.37}},
            "transfer-composite-medium": {"last_levels": {"IWGAP150": 2.48}},
        }
    }
    assessment = _wealth_maturity_assessment(coefficient_report, run_report)
    assert assessment["structural_status"]["public_family_ready"] is False
    assert assessment["structural_status"]["expert_only_candidate"] is True
    assert assessment["recommendation"] == "candidate_for_expert_only_preset_keep_public_wealth_family_deferred"


def test_wealth_maturity_assessment_keeps_shadow_when_signal_is_weak() -> None:
    coefficient_report = {
        "equations": {
            "IWGAP150": {
                "effective_nobs": 11.0,
                "main_rmse": 0.025,
                "benchmarks": {"restricted_regressor": "LAAZ"},
                "loo": {
                    "rmse_mean": 0.05,
                    "rmse_std": 0.02,
                    "sign_stability": {"WG0": 9, "WGA": 9, "WGT": 11, "WGU": 11},
                },
            }
        }
    }
    run_report = {
        "scenarios": {
            "baseline-observed": {"last_levels": {"IWGAP150": 2.4}},
            "ui-relief": {"last_levels": {"IWGAP150": 2.405}},
            "ui-shock": {"last_levels": {"IWGAP150": 2.399}},
            "transfer-package-relief": {"last_levels": {"IWGAP150": 2.401}},
            "transfer-package-shock": {"last_levels": {"IWGAP150": 2.398}},
            "transfer-composite-medium": {"last_levels": {"IWGAP150": 2.404}},
        }
    }
    assessment = _wealth_maturity_assessment(coefficient_report, run_report)
    assert assessment["structural_status"]["expert_only_candidate"] is False
    assert assessment["recommendation"] == "keep_private_shadow_until_wealth_family_is_built"
