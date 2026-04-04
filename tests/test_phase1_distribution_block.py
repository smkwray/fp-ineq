from __future__ import annotations

from fp_ineq.phase1_distribution_block import _movement_summary


def test_distribution_movement_summary_requires_headline_and_macro_movement() -> None:
    results = {
        "baseline-observed": {
            "IPOVALL": 0.11,
            "IPOVCH": 0.15,
            "IGINIHH": 0.49,
            "IMEDRINC": 67.2,
            "TRLOWZ": 1.2,
            "RYDPC": 65.0,
            "UB": 9.0,
            "TRGH": 1200.0,
            "TRSH": 384.0,
            "YD": 7126.0,
            "GDPR": 6620.0,
            "UR": 0.045,
            "PCY": 2.87,
            "RS": 4.55,
        },
        "ui-relief": {
            "IPOVALL": 0.108,
            "IPOVCH": 0.147,
            "IGINIHH": 0.488,
            "IMEDRINC": 67.5,
            "TRLOWZ": 1.25,
            "RYDPC": 65.8,
            "UB": 55.0,
            "TRGH": 1200.0,
            "TRSH": 384.0,
            "YD": 7230.0,
            "GDPR": 6645.0,
            "UR": 0.0438,
            "PCY": 2.89,
            "RS": 4.72,
        },
        "transfer-package-relief": {
            "IPOVALL": 0.105,
            "IPOVCH": 0.142,
            "IGINIHH": 0.487,
            "IMEDRINC": 67.6,
            "TRLOWZ": 1.31,
            "RYDPC": 66.1,
            "UB": 56.0,
            "TRGH": 1208.0,
            "TRSH": 393.0,
            "YD": 7258.0,
            "GDPR": 6653.0,
            "UR": 0.0435,
            "PCY": 2.90,
            "RS": 4.80,
        },
        "transfer-package-shock": {
            "IPOVALL": 0.113,
            "IPOVCH": 0.154,
            "IGINIHH": 0.492,
            "IMEDRINC": 66.9,
            "TRLOWZ": 1.18,
            "RYDPC": 64.7,
            "UB": -7.0,
            "TRGH": 1196.0,
            "TRSH": 380.0,
            "YD": 7098.0,
            "GDPR": 6612.0,
            "UR": 0.0454,
            "PCY": 2.86,
            "RS": 4.49,
        },
    }
    summary = _movement_summary(results)
    assert summary["passes_core"] is True
    assert summary["scenario_checks"]["ui-relief"]["required_moves"] == {
        "IPOVALL": True,
        "IPOVCH": True,
        "YD": True,
        "GDPR": True,
        "UB": True,
    }
    assert summary["scenario_checks"]["ui-relief"]["required_signs"] == {
        "IPOVALL": True,
        "IPOVCH": True,
        "TRLOWZ": True,
        "RYDPC": True,
        "YD": True,
        "GDPR": True,
        "UB": True,
    }
    assert summary["scenario_checks"]["ui-relief"]["one_of_moves"]["UR"] is True
    assert summary["scenario_checks"]["transfer-package-relief"]["required_moves"] == {
        "IPOVALL": True,
        "IPOVCH": True,
        "YD": True,
        "GDPR": True,
        "UB": True,
        "TRGH": True,
        "TRSH": True,
    }
    assert summary["scenario_checks"]["transfer-package-shock"]["required_signs"]["IPOVALL"] is True
    assert summary["scenario_checks"]["transfer-package-shock"]["required_signs"]["TRLOWZ"] is True


def test_distribution_movement_summary_rejects_wrong_poverty_direction() -> None:
    results = {
        "baseline-observed": {
            "IPOVALL": 0.11,
            "IPOVCH": 0.15,
            "IGINIHH": 0.49,
            "IMEDRINC": 67.2,
            "TRLOWZ": 1.2,
            "RYDPC": 65.0,
            "UB": 9.0,
            "TRGH": 1200.0,
            "TRSH": 384.0,
            "YD": 7126.0,
            "GDPR": 6620.0,
            "UR": 0.045,
            "PCY": 2.87,
            "RS": 4.55,
        },
        "ui-relief": {
            "IPOVALL": 0.111,
            "IPOVCH": 0.151,
            "IGINIHH": 0.488,
            "IMEDRINC": 67.5,
            "TRLOWZ": 1.25,
            "RYDPC": 65.8,
            "UB": 55.0,
            "TRGH": 1200.0,
            "TRSH": 384.0,
            "YD": 7230.0,
            "GDPR": 6645.0,
            "UR": 0.0438,
            "PCY": 2.89,
            "RS": 4.72,
        },
    }
    summary = _movement_summary(results)
    assert summary["passes_core"] is False
    assert summary["scenario_checks"]["ui-relief"]["required_signs"]["IPOVALL"] is False
    assert summary["scenario_checks"]["ui-relief"]["required_signs"]["IPOVCH"] is False
