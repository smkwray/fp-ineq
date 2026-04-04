from __future__ import annotations

from fp_ineq.phase1_ui import (
    _PHASE1_FLAT_TAIL_CHECKS,
    _apply_manifest_patches,
    _load_phase1_manifest,
    _movement_summary,
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
