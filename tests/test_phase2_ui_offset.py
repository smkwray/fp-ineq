from __future__ import annotations

from fp_ineq.phase2_ui_offset import (
    _acceptance_summary,
    _clawback_percent_label,
    _clawback_share,
    _offset_variant_id,
    _target_offset_delta_ur,
)


def test_clawback_share_measures_partial_ur_reversal() -> None:
    assert _target_offset_delta_ur(-0.002, 0.25) == -0.0015
    assert _clawback_share(-0.002, -0.0015) == 0.25
    assert _clawback_percent_label(0.5) == 50
    assert _offset_variant_id(0.5) == "ui-relief-offset-50"


def test_ui_offset_acceptance_summary_passes_for_partial_clawback_case() -> None:
    summary = _acceptance_summary(
        {
            "baseline_reference_max_abs_diff": 0.0,
            "no_offset_reference_max_abs_diff": 0.0,
            "reference_match_tolerance": 1e-9,
            "target_clawback_share": 0.25,
            "clawback_tolerance": 0.05,
            "trlowz_relative_gap": 0.01,
            "trlowz_relative_tolerance": 0.02,
            "clawback_share": 0.25,
            "no_offset_last_levels": {"UR": 0.0400, "GDPR": 100.0, "YD": 80.0},
            "offset_last_levels": {"UR": 0.0405, "GDPR": 99.6, "YD": 79.7},
        }
    )
    assert summary["passes_core"] is True
    assert all(summary["checks"].values())
    assert summary["diagnostics"]["offset_has_lower_gdpr"] is True


def test_ui_offset_acceptance_summary_rejects_wrong_direction_and_trlowz_drift() -> None:
    summary = _acceptance_summary(
        {
            "baseline_reference_max_abs_diff": 0.0,
            "no_offset_reference_max_abs_diff": 1e-8,
            "reference_match_tolerance": 1e-9,
            "target_clawback_share": 0.25,
            "clawback_tolerance": 0.05,
            "trlowz_relative_gap": 0.08,
            "trlowz_relative_tolerance": 0.02,
            "clawback_share": 0.10,
            "no_offset_last_levels": {"UR": 0.0400, "GDPR": 100.0, "YD": 80.0},
            "offset_last_levels": {"UR": 0.0395, "GDPR": 100.3, "YD": 80.2},
        }
    )
    assert summary["passes_core"] is False
    assert summary["checks"]["no_offset_matches_reference"] is False
    assert summary["checks"]["offset_has_higher_ur"] is False
    assert summary["checks"]["offset_has_lower_yd"] is False
    assert summary["checks"]["offset_preserves_trlowz"] is False
    assert summary["checks"]["offset_hits_target_clawback"] is False
    assert summary["diagnostics"]["offset_has_lower_gdpr"] is False
