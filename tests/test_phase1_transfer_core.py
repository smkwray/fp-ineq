from __future__ import annotations

import pytest

from fp_ineq.phase1_transfer_core import (
    TransferScenarioSpec,
    _acceptance_summary,
    _required_moves_for_variant,
    _transfer_composite_ladder_specs,
    _transfer_input_patches,
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


def test_transfer_composite_ladder_specs_are_catalog_driven() -> None:
    specs = _transfer_composite_ladder_specs()
    assert [(spec.variant_id, spec.ui_factor, spec.trgh_delta_q, spec.trsh_factor) for spec in specs] == [
        ("baseline-observed", 1.0, 0.0, 1.0),
        ("transfer-composite-small", pytest.approx(1.01), pytest.approx(1.0), pytest.approx(1.01)),
        ("transfer-composite-medium", pytest.approx(1.02), pytest.approx(2.0), pytest.approx(1.02)),
        ("transfer-composite-large", pytest.approx(1.03), pytest.approx(3.0), pytest.approx(1.03)),
    ]
    assert [(spec.variant_id, spec.trfin_fed_share, spec.trfin_sl_share) for spec in specs] == [
        ("baseline-observed", 0.0, 0.0),
        ("transfer-composite-small", 1.0, 1.0),
        ("transfer-composite-medium", 1.0, 1.0),
        ("transfer-composite-large", 1.0, 1.0),
    ]


def test_transfer_input_patches_inline_financing_formulas() -> None:
    spec = TransferScenarioSpec(
        variant_id="transfer-composite-medium",
        ui_factor=1.02,
        trgh_delta_q=2.0,
        trsh_factor=1.02,
        trfin_fed_share=1.0,
        trfin_sl_share=1.0,
        description="test",
    )
    patches = _transfer_input_patches(spec)
    assert patches == {
        "CREATE UIFAC=1;": "CREATE UIFAC=1.02;",
        "CREATE SNAPDELTAQ=0;": "CREATE SNAPDELTAQ=2;",
        "CREATE SSFAC=1;": "CREATE SSFAC=1.02;",
        "CREATE TFEDSHR=0;": "CREATE TFEDSHR=1;",
        "CREATE TSLSHR=0;": "CREATE TSLSHR=1;",
    }
