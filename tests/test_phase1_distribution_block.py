from __future__ import annotations

import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from fp_ineq.paths import RepoPaths
from fp_ineq.phase1_distribution_block import (
    _distribution_bridge_parse_errors,
    _derive_private_package_levels,
    _distribution_decomposition,
    _expected_sign_for_variant,
    _latest_transfer_core_baseline_loadformat,
    _movement_summary,
    _render_runtime_distribution_block,
    _scenario_input_patches,
    estimate_phase1_distribution_coefficients,
)


def _repo_paths_for_test(tmp_path) -> RepoPaths:
    runtime_root = tmp_path / "runtime"
    data_root = tmp_path / "data"
    phase1_root = runtime_root / "phase1_ui"
    transfer_root = runtime_root / "phase1_transfer_core"
    distribution_root = runtime_root / "phase1_distribution_block"
    credit_root = runtime_root / "phase2_credit_family"
    ui_offset_root = runtime_root / "phase2_ui_offset_family"
    solved_public_root = runtime_root / "phase1_solved_public"
    return RepoPaths(
        repo_root=tmp_path,
        data_root=data_root,
        data_series_root=data_root / "series",
        data_reports_root=data_root / "reports",
        overlay_source_root=tmp_path / "overlay" / "stock_fm",
        runtime_root=runtime_root,
        runtime_overlay_root=runtime_root / "overlay_stock_fm",
        runtime_bundle_root=runtime_root / "bundles",
        runtime_artifacts_root=runtime_root / "artifacts-ineq",
        runtime_phase1_root=phase1_root,
        runtime_phase1_overlay_root=phase1_root / "overlay",
        runtime_phase1_scenarios_root=phase1_root / "scenarios",
        runtime_phase1_artifacts_root=phase1_root / "artifacts",
        runtime_phase1_reports_root=phase1_root / "reports",
        runtime_transfer_root=transfer_root,
        runtime_transfer_scenarios_root=transfer_root / "scenarios",
        runtime_transfer_artifacts_root=transfer_root / "artifacts",
        runtime_transfer_reports_root=transfer_root / "reports",
        runtime_distribution_root=distribution_root,
        runtime_distribution_overlay_root=distribution_root / "overlay",
        runtime_distribution_scenarios_root=distribution_root / "scenarios",
        runtime_distribution_artifacts_root=distribution_root / "artifacts",
        runtime_distribution_reports_root=distribution_root / "reports",
        runtime_credit_root=credit_root,
        runtime_credit_overlay_root=credit_root / "overlay",
        runtime_credit_scenarios_root=credit_root / "scenarios",
        runtime_credit_artifacts_root=credit_root / "artifacts",
        runtime_credit_reports_root=credit_root / "reports",
        runtime_ui_offset_root=ui_offset_root,
        runtime_ui_offset_overlay_root=ui_offset_root / "overlay",
        runtime_ui_offset_scenarios_root=ui_offset_root / "scenarios",
        runtime_ui_offset_artifacts_root=ui_offset_root / "artifacts",
        runtime_ui_offset_reports_root=ui_offset_root / "reports",
        runtime_solved_public_root=solved_public_root,
        docs_root=tmp_path / "docs",
        specs_root=tmp_path / "specs",
        reference_root=tmp_path / "reference",
    )


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
            "THG": 2.0,
            "THS": 1.5,
            "RECG": 3.0,
            "RECS": 1.0,
            "SGP": 4.0,
            "SSP": 1.8,
            "PKGGROSS": 0.0,
            "PKGFIN": 0.0,
            "PKGNET": 0.0,
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
            "THG": 2.2,
            "THS": 1.7,
            "RECG": 3.2,
            "RECS": 1.2,
            "SGP": 4.3,
            "SSP": 2.0,
            "PKGGROSS": 0.0,
            "PKGFIN": 0.0,
            "PKGNET": 0.0,
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
            "THG": 2.4,
            "THS": 1.9,
            "RECG": 3.4,
            "RECS": 1.4,
            "SGP": 4.5,
            "SSP": 2.2,
            "PKGGROSS": 21.5,
            "PKGFIN": 0.0,
            "PKGNET": 21.5,
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
            "THG": 1.8,
            "THS": 1.1,
            "RECG": 2.6,
            "RECS": 0.7,
            "SGP": 3.6,
            "SSP": 1.4,
            "PKGGROSS": 18.9,
            "PKGFIN": 0.0,
            "PKGNET": 18.9,
        },
        "transfer-composite-small": {
            "IPOVALL": 0.106,
            "IPOVCH": 0.141,
            "IGINIHH": 0.486,
            "IMEDRINC": 67.7,
            "TRLOWZ": 1.37,
            "RYDPC": 66.4,
            "UB": 58.0,
            "TRGH": 1210.0,
            "TRSH": 395.0,
            "YD": 7092.0,
            "GDPR": 6615.0,
            "UR": 0.0432,
            "PCY": 2.91,
            "RS": 4.81,
            "THG": 2.6,
            "THS": 2.1,
            "RECG": 3.6,
            "RECS": 1.6,
            "SGP": 4.7,
            "SSP": 2.4,
            "PKGGROSS": 22.0,
            "PKGFIN": 22.0,
            "PKGNET": 0.0,
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
    assert summary["scenario_checks"]["transfer-composite-small"]["required_signs"] == {
        "IPOVALL": True,
        "IPOVCH": True,
        "TRLOWZ": True,
        "RYDPC": True,
        "UB": True,
        "TRGH": True,
        "TRSH": True,
    }
    assert summary["scenario_checks"]["transfer-composite-small"]["private_package_gates"]["passes"] is True
    assert summary["scenario_checks"]["transfer-composite-small"]["private_package_gates"]["diagnostics"]["gross_positive"] is True
    assert summary["scenario_checks"]["transfer-composite-small"]["private_package_gates"]["diagnostics"]["package_net_ok"] is True


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


def test_distribution_scenario_input_patches_include_transfer_composite_package_shares() -> None:
    spec = SimpleNamespace(
        variant_id="transfer-composite-small",
        ui_factor=1.0125839776982168,
        trgh_delta_q=1.2583977698216835,
        trsh_factor=1.0125839776982168,
        trfin_fed_share=1.0,
        trfin_sl_share=0.75,
    )
    patches = _scenario_input_patches(spec)
    assert patches["CREATE UIFAC=1;"] == "CREATE UIFAC=1.0125839777;"
    assert patches["CREATE SNAPDELTAQ=0;"] == "CREATE SNAPDELTAQ=1.25839776982;"
    assert patches["CREATE SSFAC=1;"] == "CREATE SSFAC=1.0125839777;"
    assert patches["CREATE TFEDSHR=0;"] == "CREATE TFEDSHR=1;"
    assert patches["CREATE TSLSHR=0;"] == "CREATE TSLSHR=0.75;"


def test_derive_private_package_levels_uses_scenario_levels_and_shares() -> None:
    spec = SimpleNamespace(
        variant_id="transfer-composite-medium",
        ui_factor=1.02,
        trgh_delta_q=2.0,
        trsh_factor=1.02,
        trfin_fed_share=1.0,
        trfin_sl_share=1.0,
    )

    derived = _derive_private_package_levels(
        {
            "UB": 12.24,
            "TRSH": 110.16,
            "GDPD": 4.5,
        },
        spec,
    )

    assert derived["PKGGROSS"] == pytest.approx((12.24 - 12.24 / 1.02) + 9.0 + (110.16 - 110.16 / 1.02))
    assert derived["PKGFIN"] == pytest.approx(derived["PKGGROSS"])
    assert derived["PKGNET"] == pytest.approx(0.0)


def test_distribution_expected_sign_supports_ladder_variants() -> None:
    assert _expected_sign_for_variant("ui-small") == 1.0
    assert _expected_sign_for_variant("ui-medium") == 1.0
    assert _expected_sign_for_variant("ui-large") == 1.0
    assert _expected_sign_for_variant("transfer-composite-small") == 1.0


def test_distribution_bridge_parse_errors_only_collect_after_distribution_include() -> None:
    fmout_text = "\n".join(
        [
            "UNRECOGNIZABLE VARIABLE",
            "BEFORE",
            "INPUT FILE=idp1blk.txt;",
            "UNRECOGNIZABLE VARIABLE",
            "LPOVCHGA",
            "UNRECOGNIZABLE VARIABLE",
            "0.0IDENT",
        ]
    )

    assert _distribution_bridge_parse_errors(fmout_text) == [
        "UNRECOGNIZABLE VARIABLE | LPOVCHGA",
        "UNRECOGNIZABLE VARIABLE | 0.0IDENT",
    ]


def test_render_runtime_distribution_block_splits_child_logit_level() -> None:
    coefficient_report = {
        "deviation_basis": {
            "standardization": {
                "UBBAR": 10.0,
                "UBSTD": 2.0,
                "TRGHBAR": 20.0,
                "TRGHSTD": 4.0,
                "TRSHBAR": 5.0,
                "TRSHSTD": 1.0,
            }
        },
        "equations": {
            "IPOVALL": {"coefficients": {"PV0": -2.0, "PVU": 3.0, "PVT": -0.5, "PVUI": -0.1, "PVGH": 0.05}},
            "IPOVCH": {"coefficients": {"CG0": 0.4, "CGU": 1.2, "CGT": -0.3, "CGUI": -0.08, "CGGH": 0.02}},
            "IGINIHH": {"coefficients": {"GN0": -0.2, "GNU": 0.9, "GNT": 0.04}},
            "IMEDRINC": {"coefficients": {"MD0": 1.4, "MDR": 0.3, "MDU": -0.2}},
        },
    }

    text = _render_runtime_distribution_block(coefficient_report)

    assert "IDENT LPOVCHLVL=LPOVALL+LPOVCHGAP;" in text
    assert "IDENT IPOVCH=EXP(LPOVCHLVL)/(1+EXP(LPOVCHLVL));" in text


def test_latest_transfer_core_baseline_loadformat_reads_explicit_baseline_path(
    tmp_path, monkeypatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    baseline_path = tmp_path / "baseline" / "LOADFORMAT.DAT"
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text("stub", encoding="utf-8")
    report_path = paths.runtime_transfer_reports_root / "run_phase1_transfer_core.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(
            {
                "scenarios": {
                    "baseline-observed": {
                        "loadformat_path": str(baseline_path),
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)

    assert _latest_transfer_core_baseline_loadformat() == baseline_path


def test_latest_transfer_core_baseline_loadformat_fails_without_baseline_metadata(
    tmp_path, monkeypatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    report_path = paths.runtime_transfer_reports_root / "run_phase1_transfer_core.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps({"scenarios": {}}), encoding="utf-8")

    fallback_candidate = paths.runtime_transfer_artifacts_root / "zzz" / "LOADFORMAT.DAT"
    fallback_candidate.parent.mkdir(parents=True, exist_ok=True)
    fallback_candidate.write_text("should not be used", encoding="utf-8")

    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)

    with pytest.raises(KeyError, match="baseline-observed.loadformat_path"):
        _latest_transfer_core_baseline_loadformat()


def test_latest_transfer_core_baseline_loadformat_fails_when_report_is_missing(
    tmp_path, monkeypatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)

    with pytest.raises(FileNotFoundError, match="Transfer-core report missing"):
        _latest_transfer_core_baseline_loadformat()


def test_estimate_distribution_coefficients_reports_benchmarks_and_loo(
    tmp_path, monkeypatch
) -> None:
    periods = [f"{year}.1" for year in range(2015, 2026)]
    ur = np.linspace(0.04, 0.07, len(periods))
    trlowz = np.linspace(1.0, 1.4, len(periods))
    lrydpc = np.linspace(10.0, 10.6, len(periods))
    laaz = np.linspace(4.0, 4.5, len(periods))
    regressors = pd.DataFrame(
        {
            "UR": ur,
            "UB": np.linspace(20.0, 30.0, len(periods)),
            "TRGH": np.linspace(100.0, 120.0, len(periods)),
            "TRSH": np.linspace(30.0, 33.0, len(periods)),
            "POP": np.linspace(300.0, 310.0, len(periods)),
            "PH": np.linspace(1.0, 1.05, len(periods)),
            "GDPR": np.linspace(500.0, 560.0, len(periods)),
            "YD": np.linspace(450.0, 520.0, len(periods)),
            "LAAZ": laaz,
            "TRLOWZ": trlowz,
            "LRYDPC": lrydpc,
        },
        index=periods,
    )
    poverty_all = pd.Series(1 / (1 + np.exp(-(-2.6 + 18 * ur - 1.2 * trlowz))), index=periods)
    poverty_child = pd.Series(
        1 / (1 + np.exp(-(-2.2 + 20 * ur - 1.0 * trlowz))),
        index=periods,
    )
    gini = pd.Series(1 / (1 + np.exp(-(-0.4 + 9 * ur - 0.3 * trlowz))), index=periods)
    med_income = pd.Series(np.exp(1.8 + 0.22 * lrydpc - 0.8 * ur), index=periods)
    wealth_gap = pd.Series(np.exp(-0.9 + 0.4 * laaz + 0.8 * ur - 0.08 * trlowz), index=periods)
    targets = {
        "IPOVALL": poverty_all,
        "IPOVCH": poverty_child,
        "IGINIHH": gini,
        "IMEDRINC": med_income,
        "IWGAP150": wealth_gap,
    }

    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: _repo_paths_for_test(tmp_path))
    monkeypatch.setattr("fp_ineq.phase1_distribution_block._load_baseline_regressors", lambda: regressors)
    monkeypatch.setattr("fp_ineq.phase1_distribution_block._load_target_series", lambda name: targets[name])

    payload = estimate_phase1_distribution_coefficients()
    equations = payload["equations"]

    expected_restricted = {
        "IPOVALL": {"UR"},
        "IPOVCH": {"UR"},
        "IGINIHH": {"UR"},
        "IMEDRINC": {"LRYDPC"},
        "IWGAP150": {"UR", "TRLOWZ", "LAAZ"},
    }

    for equation_name, allowed_restricted in expected_restricted.items():
        equation = equations[equation_name]
        assert "benchmarks" in equation
        assert "loo" in equation
        assert equation["benchmarks"]["restricted_regressor"] in allowed_restricted
        assert equation["benchmarks"]["constant_only"] >= 0.0
        assert equation["benchmarks"]["restricted"] >= 0.0
        assert equation["loo"]["rmse_mean"] >= 0.0
        assert equation["loo"]["rmse_std"] >= 0.0
        assert equation["loo"]["prediction_max"] >= equation["loo"]["prediction_min"]
        sign_stability = equation["loo"]["sign_stability"]
        assert set(sign_stability) == set(equation["coefficients"])
        assert all(0 <= value <= 11 for value in sign_stability.values())

    assert payload["deviation_basis"]["ridge_alpha_grid"] == [0.1, 0.3, 1.0, 3.0, 10.0, 30.0]
    assert set(payload["deviation_basis"]["standardization"]) == {
        "UBBAR",
        "UBSTD",
        "TRGHBAR",
        "TRGHSTD",
        "TRSHBAR",
        "TRSHSTD",
    }
    assert equations["IPOVALL"]["fit_mode"] == "poverty_deviation_ridge"
    assert equations["IPOVCH"]["fit_mode"] == "poverty_deviation_ridge"
    assert equations["IGINIHH"]["fit_mode"] == "linear"
    assert equations["IPOVALL"]["ridge"]["base_regressors"] == ["UR", "TRLOWZ"]
    assert equations["IPOVALL"]["ridge"]["deviation_regressors"] == ["UIDEV", "GHSHDV"]
    assert equations["IPOVALL"]["ridge"]["ridge_alpha"] in payload["deviation_basis"]["ridge_alpha_grid"]


def test_distribution_decomposition_sums_bridge_contributions() -> None:
    coefficient_report = {
        "deviation_basis": {
            "standardization": {
                "UBBAR": 100.0,
                "UBSTD": 10.0,
                "TRGHBAR": 200.0,
                "TRGHSTD": 20.0,
                "TRSHBAR": 50.0,
                "TRSHSTD": 5.0,
            }
        },
        "equations": {
            "IPOVALL": {
                "coefficients": {
                    "PV0": -2.0,
                    "PVU": 4.0,
                    "PVT": -0.8,
                    "PVUI": -0.3,
                    "PVGH": 0.2,
                }
            },
            "IPOVCH": {
                "coefficients": {
                    "CG0": 0.6,
                    "CGU": 1.1,
                    "CGT": -0.5,
                    "CGUI": -0.2,
                    "CGGH": 0.1,
                }
            },
            "IGINIHH": {
                "coefficients": {
                    "GN0": -0.3,
                    "GNU": 1.4,
                    "GNT": -0.2,
                }
            },
            "IMEDRINC": {
                "coefficients": {
                    "MD0": 1.2,
                    "MDR": 0.4,
                    "MDU": -0.7,
                }
            },
        },
    }

    baseline = {
        "UR": 0.05,
        "TRLOWZ": 1.10,
        "UB": 100.0,
        "TRGH": 200.0,
        "TRSH": 50.0,
        "RYDPC": float(np.exp(4.0)),
    }
    scenario = {
        "UR": 0.045,
        "TRLOWZ": 1.25,
        "UB": 112.0,
        "TRGH": 210.0,
        "TRSH": 48.0,
        "RYDPC": float(np.exp(4.08)),
    }

    def _augment(levels: dict[str, float], *, child: bool = False) -> dict[str, float]:
        ubz = (levels["UB"] - 100.0) / 10.0
        trghz = (levels["TRGH"] - 200.0) / 20.0
        trshz = (levels["TRSH"] - 50.0) / 5.0
        uidev = ubz - 0.5 * (trghz + trshz)
        ghshdv = trghz - trshz
        lpovall = -2.0 + 4.0 * levels["UR"] - 0.8 * levels["TRLOWZ"] - 0.3 * uidev + 0.2 * ghshdv
        lpovchg = 0.6 + 1.1 * levels["UR"] - 0.5 * levels["TRLOWZ"] - 0.2 * uidev + 0.1 * ghshdv
        lgini = -0.3 + 1.4 * levels["UR"] - 0.2 * levels["TRLOWZ"]
        lmed = 1.2 + 0.4 * np.log(levels["RYDPC"]) - 0.7 * levels["UR"]
        return {
            **levels,
            "IPOVALL": float(1.0 / (1.0 + np.exp(-lpovall))),
            "IPOVCH": float(1.0 / (1.0 + np.exp(-(lpovall + lpovchg)))),
            "IGINIHH": float(1.0 / (1.0 + np.exp(-lgini))),
            "IMEDRINC": float(np.exp(lmed)),
        }

    results = {
        "baseline-observed": _augment(baseline),
        "ui-relief": _augment(scenario),
    }
    payload = _distribution_decomposition(results, coefficient_report)
    ui_relief = payload["scenarios"]["ui-relief"]

    for output_name in ("IPOVALL", "IPOVCH", "IGINIHH", "IMEDRINC"):
        detail = ui_relief[output_name]
        state_sum = sum(
            float(item["state_contribution"]) for item in detail["bridge_contributions"].values()
        )
        assert abs(state_sum - float(detail["state_delta"])) < 1e-12
        assert abs(float(detail["reconstruction_error"])) < 1e-12

    child_terms = ui_relief["IPOVCH"]["bridge_contributions"]
    assert set(child_terms) == {"UR", "TRLOWZ", "UIDEV", "GHSHDV"}
    assert len(child_terms["UR"]["component_terms"]) == 2
