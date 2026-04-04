from __future__ import annotations

from fp_ineq.phase1_transfer_core import _acceptance_summary, _required_moves_for_variant


def test_required_moves_follow_transfer_family() -> None:
    assert _required_moves_for_variant("ui-relief") == ("UB", "YD", "GDPR")
    assert _required_moves_for_variant("snap-relief") == ("TRGH", "YD", "GDPR")
    assert _required_moves_for_variant("social-security-relief") == ("TRSH", "YD", "GDPR")
    assert _required_moves_for_variant("transfer-package-relief") == (
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
        "snap-relief": {**baseline, "TRGH": 21.0, "YD": 40.5, "GDPR": 50.4, "PCY": 2.01},
        "social-security-relief": {
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
    assert summary["scenario_checks"]["snap-relief"]["required_moves"] == {
        "TRGH": True,
        "YD": True,
        "GDPR": True,
    }
    assert summary["scenario_checks"]["snap-relief"]["required_signs"] == {
        "TRGH": True,
        "YD": True,
        "GDPR": True,
    }
    assert summary["scenario_checks"]["social-security-relief"]["required_moves"] == {
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
