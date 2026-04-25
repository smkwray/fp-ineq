from __future__ import annotations

import csv
import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import yaml

from fp_ineq import canonical_freeze as canonical_freeze_module
from fp_ineq.paths import RepoPaths
from fp_ineq.phase1_catalog import phase1_scenario_by_variant
from fp_ineq.phase1_distribution_block import (
    _apply_runtime_text_post_patches,
    _DISTRIBUTION_DEFAULT_BACKEND,
    _distribution_identity_checks,
    _distribution_bridge_parse_errors,
    _merge_runtime_text_files,
    _safe_distribution_decomposition,
    assess_phase1_distribution_canonical_parity,
    assess_phase1_distribution_backend_boundary,
    analyze_phase1_distribution_driver_gap,
    analyze_phase1_distribution_first_levels,
    analyze_phase1_distribution_policy_gap,
    analyze_phase1_distribution_transfer_macro_block,
    analyze_phase1_distribution_ui_attenuation,
    compare_phase1_distribution_reports,
    compare_phase1_distribution_backends,
    _derive_private_package_levels,
    _distribution_decomposition,
    _expected_sign_for_variant,
    _latest_transfer_core_baseline_loadformat,
    _movement_summary,
    _read_fp_r_series_levels,
    _render_runtime_distribution_block,
    _scenario_input_patches,
    _scenario_overrides,
    _setupsolve_compose_post_patches,
    _supplement_missing_levels,
    _transfer_intg_driver_delta_breakdown,
    _transfer_intg_driver_identity,
    _transfer_jf_driver_delta_breakdown,
    _transfer_jf_driver_identity,
    _transfer_ly1_driver_delta_breakdown,
    _transfer_ly1_driver_identity,
    _transfer_ag_driver_delta_breakdown,
    _transfer_ag_driver_identity,
    analyze_phase1_distribution_canonical_blocker_traces,
    analyze_phase1_distribution_canonical_solved_path,
    estimate_phase1_distribution_coefficients,
    run_phase1_distribution_block,
    validate_phase1_distribution_identities,
    write_phase1_distribution_scenarios,
)
from fp_ineq.phase1_distribution_interventions import (
    assess_phase1_distribution_generalization_readiness,
    assess_phase1_distribution_family_generalization,
    compose_phase1_distribution_package_evidence,
    assess_phase1_distribution_package_readiness,
    assess_phase1_distribution_intervention_ladder_selection,
    assess_phase1_distribution_intervention_effect,
    assess_phase1_distribution_holdout_directionality,
    load_phase1_distribution_intervention_spec,
    run_phase1_distribution_family_holdout,
    run_phase1_distribution_intervention_experiment,
    run_phase1_distribution_intervention_ladder,
)
from fp_ineq.phase1_ui import _extract_levels_from_loadformat


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


def test_read_fp_r_series_levels_derives_rydpc_from_yd_pop_ph(tmp_path: Path) -> None:
    series_path = tmp_path / "fp_r_series.csv"
    series_path.write_text(
        "period,YD,POP,PH,GDPR,UB,TRGH,TRSH\n"
        "2026.1,60,100,0.2,700,1,2,3\n"
        "2029.4,72,120,0.25,800,2,3,4\n",
        encoding="utf-8",
    )
    first_levels, last_levels = _read_fp_r_series_levels(series_path, ["RYDPC", "TRLOWZ"])
    assert first_levels["RYDPC"] == pytest.approx(3.0)
    assert last_levels["RYDPC"] == pytest.approx(2.4)
    assert first_levels["TRLOWZ"] == pytest.approx(0.3)
    assert last_levels["TRLOWZ"] == pytest.approx(0.3)


def test_read_fp_r_series_levels_prefers_forecast_window_rows(tmp_path: Path) -> None:
    series_path = tmp_path / "fp_r_series.csv"
    series_path.write_text(
        "period,YD,POP,PH,UB,TRGH,TRSH\n"
        "1952.1,10,10,1,1,1,1\n"
        "2026.1,60,100,0.2,1,2,3\n"
        "2026.4,72,120,0.25,2,3,4\n"
        "2029.4,90,150,0.3,9,9,9\n",
        encoding="utf-8",
    )

    first_levels, last_levels = _read_fp_r_series_levels(
        series_path,
        ["RYDPC", "TRLOWZ"],
        forecast_start="2026.1",
        forecast_end="2026.4",
    )

    assert first_levels["RYDPC"] == pytest.approx(3.0)
    assert first_levels["TRLOWZ"] == pytest.approx(0.3)
    assert last_levels["RYDPC"] == pytest.approx(2.4)
    assert last_levels["TRLOWZ"] == pytest.approx(0.3)


def test_supplement_missing_levels_replaces_negative_99_sentinels() -> None:
    supplemented = _supplement_missing_levels(
        {"TRLOWZ": -99.0, "PCY": -99.0, "RYDPC": 4.0},
        {"TRLOWZ": 0.2, "PCY": 2.8, "RYDPC": 5.0},
    )
    assert supplemented["TRLOWZ"] == pytest.approx(0.2)
    assert supplemented["PCY"] == pytest.approx(2.8)
    assert supplemented["RYDPC"] == pytest.approx(4.0)


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


def test_distribution_scenario_overrides_are_forecast_window_only() -> None:
    spec = SimpleNamespace(
        variant_id="transfer-composite-small",
        ui_factor=1.0125839776982168,
        trgh_delta_q=1.2583977698216835,
        trsh_factor=1.0125839776982168,
        trfin_fed_share=1.0,
        trfin_sl_share=0.75,
    )
    assert _scenario_input_patches(spec) == {}
    overrides = _scenario_overrides(spec)
    assert set(overrides) == {"UIFAC", "SNAPDELTAQ", "SSFAC", "TFEDSHR", "TSLSHR"}
    assert overrides["UIFAC"]["method"] == "SAMEVALUE"
    assert overrides["UIFAC"]["value"] == pytest.approx(1.0125839776982168)
    assert overrides["SNAPDELTAQ"]["value"] == pytest.approx(1.2583977698216835)
    assert overrides["SSFAC"]["value"] == pytest.approx(1.0125839776982168)
    assert overrides["TFEDSHR"]["value"] == pytest.approx(1.0)
    assert overrides["TSLSHR"]["value"] == pytest.approx(0.75)


def test_distribution_scenario_overrides_compile_to_forecast_window_fmexog(tmp_path) -> None:
    spec = SimpleNamespace(
        variant_id="transfer-composite-medium",
        ui_factor=1.018637285379202,
        trgh_delta_q=1.8637285379202133,
        trsh_factor=1.018637285379202,
        trfin_fed_share=1.0,
        trfin_sl_share=1.0,
    )
    from fp_wraptr.io.writer import write_exogenous_file
    from fp_wraptr.io.input_parser import parse_fmexog_text

    fmexog_path = tmp_path / "fmexog.txt"
    write_exogenous_file(
        variables=_scenario_overrides(spec),
        sample_start="2026.1",
        sample_end="2029.4",
        output_path=fmexog_path,
    )

    parsed = parse_fmexog_text(fmexog_path.read_text(encoding="utf-8"))
    assert parsed["sample_start"] == "2026.1"
    assert parsed["sample_end"] == "2029.4"
    changes = {item["variable"]: item for item in parsed["changes"]}
    assert set(changes) == {"UIFAC", "SNAPDELTAQ", "SSFAC", "TFEDSHR", "TSLSHR"}
    assert changes["UIFAC"]["values"] == [pytest.approx(1.018637285379202)]
    assert changes["SNAPDELTAQ"]["values"] == [pytest.approx(1.8637285379202133)]
    assert changes["SSFAC"]["values"] == [pytest.approx(1.018637285379202)]
    assert changes["TFEDSHR"]["values"] == [pytest.approx(1.0)]
    assert changes["TSLSHR"]["values"] == [pytest.approx(1.0)]


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


def test_compare_phase1_distribution_backends_writes_small_report(
    tmp_path, monkeypatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    observed_calls: list[dict[str, object]] = []

    def fake_run_phase1_distribution_block(
        *,
        fp_home,
        backend,
        scenarios_root=None,
        artifacts_root=None,
        overlay_root=None,
        report_path=None,
        variant_ids=None,
        fpr_timeout_seconds=None,
    ):
        observed_calls.append(
            {
                "backend": backend,
                "overlay_root": str(overlay_root) if overlay_root is not None else None,
                "fpr_timeout_seconds": fpr_timeout_seconds,
            }
        )
        payload = {
            "scenarios": {
                variant_id: {
                    "loadformat_path": f"/tmp/{backend}/{variant_id}/LOADFORMAT.DAT",
                    "success": True,
                    "first_levels": {
                        "TRLOWZ": 0.1 if backend == "fp-r" else 0.2,
                        "IPOVALL": 0.14 if backend == "fp-r" else 0.141,
                    },
                    "last_levels": {
                        "TRLOWZ": 4.0 if backend == "fp-r" else 3.9,
                        "IPOVALL": 0.11 if backend == "fp-r" else 0.12,
                    },
                }
                for variant_id in variant_ids
            }
        }
        path = Path(report_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        return {
            "report_path": str(path),
            "backend": backend,
            "passes": True,
        }

    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)
    monkeypatch.setattr("fp_ineq.phase1_distribution_block.run_phase1_distribution_block", fake_run_phase1_distribution_block)
    monkeypatch.setattr(
        "fp_ineq.phase1_distribution_block._distribution_identity_checks_for_loadformat",
        lambda loadformat_path, spec, **_kwargs: {
            "ub_scaled": {
                "expression": "UB - EXP(LUB) * UIFAC",
                "max_abs_residual": 0.0 if "fp-r" in str(loadformat_path) else 1.25,
                "terminal_abs_residual": 0.0 if "fp-r" in str(loadformat_path) else 1.25,
            }
        },
    )

    payload = compare_phase1_distribution_backends(
        fp_home=tmp_path / "FM",
        left_backend="fp-r",
        right_backend="fpexe",
        variant_ids=("baseline-observed", "ui-relief"),
        variables=("TRLOWZ", "IPOVALL"),
        report_path=paths.runtime_distribution_reports_root / "compare_phase1_distribution_backends.json",
        left_fpr_timeout_seconds=3600,
        right_fpr_timeout_seconds=4800,
    )

    assert payload["variant_count"] == 2
    report = json.loads((paths.runtime_distribution_reports_root / "compare_phase1_distribution_backends.json").read_text(encoding="utf-8"))
    assert report["left_backend"] == "fp-r"
    assert report["right_backend"] == "fpexe"
    assert report["variant_ids"] == ["baseline-observed", "ui-relief"]
    assert report["comparisons"][0]["last_levels"]["TRLOWZ"]["left"] == pytest.approx(4.0)
    assert report["comparisons"][0]["last_levels"]["TRLOWZ"]["right"] == pytest.approx(3.9)
    assert report["comparisons"][0]["identity_comparison"]["ub_scaled"]["left_max_abs_residual"] == pytest.approx(0.0)
    assert report["comparisons"][0]["identity_comparison"]["ub_scaled"]["right_max_abs_residual"] == pytest.approx(1.25)
    assert report["summary"]["right_backend_max_identity_residual"] == pytest.approx(1.25)
    assert observed_calls == [
        {
            "backend": "fp-r",
            "overlay_root": str(paths.runtime_distribution_root / "overlay-cmp-fp-r"),
            "fpr_timeout_seconds": 3600,
        },
        {
            "backend": "fpexe",
            "overlay_root": str(paths.runtime_distribution_root / "overlay-cmp-fpexe"),
            "fpr_timeout_seconds": None,
        },
    ]


def test_analyze_phase1_distribution_driver_gap_reports_ui_and_transfer_findings(
    tmp_path: Path,
    monkeypatch,
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)

    def _write_series_csv(path: Path, rows: list[dict[str, float]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        columns = list(rows[0].keys())
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)

    baseline_series_path = tmp_path / "artifacts" / "baseline" / "work" / "fp_r_series.csv"
    ui_series_path = tmp_path / "artifacts" / "ui" / "work" / "fp_r_series.csv"
    transfer_series_path = tmp_path / "artifacts" / "transfer" / "work" / "fp_r_series.csv"
    experiment_series_path = tmp_path / "artifacts" / "ui_experiment" / "work" / "fp_r_series.csv"

    baseline_rows = [
        {
            "period": "2026.1",
            "YD": 100.0,
            "PH": 2.0,
            "POP": 10.0,
            "RYDPC": 5.0,
            "GDPR": 120.0,
            "UR": 0.05,
            "PCY": 1.5,
            "RS": 4.0,
            "GDPD": 2.0,
            "TRLOWZ": 1.0,
            "IPOVALL": 0.10,
            "IPOVCH": 0.20,
            "LUB": 2.20,
            "UB": 9.0,
            "UIFAC": 1.0,
            "TRGH": 10.0,
            "TRSH": 5.0,
            "THG": 3.0,
            "THS": 1.0,
            "RECG": 2.0,
            "RECS": 0.5,
            "SGP": 4.0,
            "SSP": 1.5,
        },
        {
            "period": "2029.4",
            "YD": 110.0,
            "PH": 2.1,
            "POP": 10.0,
            "RYDPC": 110.0 / (10.0 * 2.1),
            "GDPR": 130.0,
            "UR": 0.049,
            "PCY": 1.6,
            "RS": 4.1,
            "GDPD": 2.1,
            "TRLOWZ": 1.1,
            "IPOVALL": 0.09,
            "IPOVCH": 0.18,
            "LUB": 2.20,
            "UB": 9.0,
            "UIFAC": 1.0,
            "TRGH": 11.0,
            "TRSH": 5.5,
            "THG": 3.1,
            "THS": 1.1,
            "RECG": 2.1,
            "RECS": 0.6,
            "SGP": 4.1,
            "SSP": 1.6,
        },
    ]
    ui_rows = [
        {
            "period": "2026.1",
            "YD": 101.0,
            "PH": 2.0,
            "POP": 10.0,
            "RYDPC": 5.05,
            "GDPR": 121.0,
            "TRLOWZ": 1.02,
            "IPOVALL": 0.099,
            "IPOVCH": 0.198,
            "LUB": 2.197224577,
            "UB": 9.18,
            "UIFAC": 1.02,
            "TRGH": 10.0,
            "TRSH": 5.0,
        },
        {
            "period": "2029.4",
            "YD": 111.0,
            "PH": 2.1,
            "POP": 10.0,
            "RYDPC": 111.0 / (10.0 * 2.1),
            "GDPR": 131.0,
            "TRLOWZ": 1.11,
            "IPOVALL": 0.089,
            "IPOVCH": 0.177,
            "LUB": 2.197224577,
            "UB": 9.18,
            "UIFAC": 1.02,
            "TRGH": 11.0,
            "TRSH": 5.5,
        },
    ]
    experiment_rows = [
        {
            "period": "2026.1",
            "YD": 101.3,
            "PH": 2.0,
            "POP": 10.0,
            "RYDPC": 5.065,
            "GDPR": 121.1,
            "TRLOWZ": 1.025,
            "IPOVALL": 0.0985,
            "IPOVCH": 0.197,
            "LUB": 2.23070379191803,
            "UB": 9.49254182490513,
            "UIFAC": 1.02,
            "TRGH": 10.0,
            "TRSH": 5.0,
        },
        {
            "period": "2029.4",
            "YD": 112.5,
            "PH": 2.1,
            "POP": 10.0,
            "RYDPC": 112.5 / (10.0 * 2.1),
            "GDPR": 132.3,
            "TRLOWZ": 1.118,
            "IPOVALL": 0.0875,
            "IPOVCH": 0.173,
            "LUB": 2.48796698168,
            "UB": 12.2775158386,
            "UIFAC": 1.02,
            "TRGH": 11.0,
            "TRSH": 5.5,
        },
    ]
    transfer_rows = [
        {
            "period": "2026.1",
            "YD": 99.0,
            "PH": 1.999,
            "POP": 10.0,
            "RYDPC": 99.0 / (10.0 * 1.999),
            "GDPR": 119.5,
            "UR": 0.0501,
            "PCY": 1.49,
            "RS": 3.98,
            "GDPD": 1.999,
            "TRLOWZ": 1.08,
            "IPOVALL": 0.097,
            "IPOVCH": 0.193,
            "LUB": 2.20,
            "UB": 9.16,
            "UIFAC": 1.018,
            "TRGH": 11.5,
            "TRSH": 6.2,
            "THG": 4.2,
            "THS": 2.1,
            "RECG": 3.0,
            "RECS": 1.4,
            "SGP": 5.2,
            "SSP": 1.45,
        },
        {
            "period": "2029.4",
            "YD": 108.0,
            "PH": 2.098,
            "POP": 10.0,
            "RYDPC": 108.0 / (10.0 * 2.098),
            "GDPR": 129.0,
            "UR": 0.0495,
            "PCY": 1.595,
            "RS": 3.92,
            "GDPD": 2.08,
            "TRLOWZ": 1.14,
            "IPOVALL": 0.088,
            "IPOVCH": 0.172,
            "LUB": 2.20,
            "UB": 9.16,
            "UIFAC": 1.018,
            "TRGH": 12.0,
            "TRSH": 6.8,
            "THG": 4.0,
            "THS": 2.5,
            "RECG": 2.3,
            "RECS": 1.8,
            "SGP": 5.8,
            "SSP": 1.7,
        },
    ]
    baseline_component_updates = {
        "2026.1": {
            "Y": 120.0,
            "WF": 0.5,
            "JF": 10.0,
            "HN": 10.0,
            "HO": 0.0,
            "WG": 0.5,
            "JG": 2.0,
            "HG": 5.0,
            "WM": 0.5,
            "JM": 1.0,
            "HM": 4.0,
            "WS": 0.5,
            "JS": 1.0,
            "HS": 4.0,
            "RNT": 10.0,
            "INTZ": 1.0,
            "INTF": 1.0,
            "INTG": 3.0,
            "INTGZ": 0.30,
            "RQG": 0.10,
            "AAG": 10.0,
            "AG": -10.0,
            "SG": 1.0,
            "MG": 0.0,
            "CUR": 0.0,
            "BR": 0.0,
            "BO": 0.0,
            "INTGR": 0.5,
            "INTS": 1.0,
            "DF": 2.0,
            "DB": 1.0,
            "DR": 0.5,
            "DG": 0.0,
            "DS": 1.0,
            "TRFH": 6.0,
            "TRSHQ": 5.0,
            "SSFAC": 1.0,
            "SIHG": 2.0,
            "SIHS": 1.0,
            "TRHR": 1.0,
            "SIGG": 0.5,
            "SISS": 0.5,
            "D1S": 0.1,
            "YT": 20.0,
            "TSLSHR": 1.0,
            "PSI13": 0.0,
            "STATP": 0.0,
        },
        "2029.4": {
            "Y": 130.0,
            "WF": 0.5,
            "JF": 10.0,
            "HN": 10.6,
            "HO": 0.0,
            "WG": 0.5,
            "JG": 2.0,
            "HG": 5.0,
            "WM": 0.5,
            "JM": 1.0,
            "HM": 4.0,
            "WS": 0.5,
            "JS": 1.0,
            "HS": 6.0,
            "RNT": 10.0,
            "INTZ": 1.0,
            "INTF": 1.0,
            "INTG": 3.0,
            "INTGZ": 0.25,
            "RQG": 0.08,
            "AAG": 12.0,
            "AG": -12.0,
            "SG": 1.2,
            "MG": 0.0,
            "CUR": 0.0,
            "BR": 0.0,
            "BO": 0.0,
            "INTGR": 0.5,
            "INTS": 1.0,
            "DF": 4.0,
            "DB": 1.0,
            "DR": 0.5,
            "DG": 0.0,
            "DS": 2.0,
            "TRFH": 6.0,
            "TRSHQ": 5.5,
            "SSFAC": 1.0,
            "SIHG": 0.8,
            "SIHS": 0.5,
            "TRHR": 0.5,
            "SIGG": 0.3,
            "SISS": 0.2,
            "D1S": 0.1,
            "YT": 22.0,
            "TSLSHR": 1.0,
            "PSI13": 0.0,
            "STATP": 0.0,
        },
    }
    transfer_component_updates = {
        "2026.1": {
            "Y": 119.5,
            "WF": 0.5,
            "JF": 10.0,
            "HN": 9.9,
            "HO": 0.0,
            "WG": 0.5,
            "JG": 2.0,
            "HG": 5.0,
            "WM": 0.5,
            "JM": 1.0,
            "HM": 4.0,
            "WS": 0.5,
            "JS": 1.0,
            "HS": 4.0,
            "RNT": 8.0,
            "INTZ": 1.0,
            "INTF": 1.0,
            "INTG": 3.0,
            "INTGZ": 0.30,
            "RQG": 0.10,
            "AAG": 10.0,
            "AG": -10.0,
            "SG": 1.0,
            "MG": 0.0,
            "CUR": 0.0,
            "BR": 0.0,
            "BO": 0.0,
            "INTGR": 0.5,
            "INTS": 1.0,
            "DF": 2.0,
            "DB": 1.0,
            "DR": 0.5,
            "DG": 0.0,
            "DS": 1.0,
            "TRFH": 5.5,
            "TRSHQ": 5.0,
            "SSFAC": 1.018,
            "SIHG": 1.5,
            "SIHS": 0.8,
            "TRHR": 0.5,
            "SIGG": 0.4,
            "SISS": 0.36,
            "D1S": 0.1,
            "YT": 19.5,
            "TSLSHR": 1.0,
            "PSI13": 0.0,
            "STATP": 0.0,
        },
        "2029.4": {
            "Y": 129.0,
            "WF": 0.5,
            "JF": 10.0,
            "HN": 10.0,
            "HO": 0.0,
            "WG": 0.5,
            "JG": 2.0,
            "HG": 5.0,
            "WM": 0.5,
            "JM": 1.0,
            "HM": 4.0,
            "WS": 0.5,
            "JS": 1.0,
            "HS": 6.0,
            "RNT": 10.0,
            "INTZ": 1.0,
            "INTF": 1.0,
            "INTG": 3.0,
            "INTGZ": 0.20,
            "RQG": 0.07,
            "AAG": 11.0,
            "AG": -11.0,
            "SG": 1.0,
            "MG": 0.0,
            "CUR": 0.0,
            "BR": 0.0,
            "BO": 0.0,
            "INTGR": 0.5,
            "INTS": 1.0,
            "DF": 4.0,
            "DB": 1.0,
            "DR": 0.5,
            "DG": 0.0,
            "DS": 2.0,
            "TRFH": 5.5,
            "TRSHQ": 5.5,
            "SSFAC": 1.018,
            "SIHG": 1.0,
            "SIHS": 0.8,
            "TRHR": 0.5,
            "SIGG": 0.4,
            "SISS": 0.26,
            "D1S": 0.1,
            "YT": 20.0,
            "TSLSHR": 1.0,
            "PSI13": 0.0,
            "STATP": 0.0,
        },
    }
    for row in baseline_rows:
        row.update(baseline_component_updates[row["period"]])
    for row in transfer_rows:
        row.update(transfer_component_updates[row["period"]])
    for path, rows in (
        (baseline_series_path, baseline_rows),
        (ui_series_path, ui_rows),
        (transfer_series_path, transfer_rows),
        (experiment_series_path, experiment_rows),
    ):
        _write_series_csv(path, rows)
        path.with_name("LOADFORMAT.DAT").write_text("", encoding="utf-8")
    baseline_series_path.parent.joinpath("scenario.yaml").write_text(
        "overrides:\n"
        "  SSFAC:\n"
        "    method: SAMEVALUE\n"
        "    value: 1.0\n"
        "  TFEDSHR:\n"
        "    method: SAMEVALUE\n"
        "    value: 0.0\n"
        "  TSLSHR:\n"
        "    method: SAMEVALUE\n"
        "    value: 0.0\n",
        encoding="utf-8",
    )
    transfer_series_path.parent.joinpath("scenario.yaml").write_text(
        "overrides:\n"
        "  UIFAC:\n"
        "    method: SAMEVALUE\n"
        "    value: 1.018\n"
        "  SNAPDELTAQ:\n"
        "    method: SAMEVALUE\n"
        "    value: 0.5\n"
        "  SSFAC:\n"
        "    method: SAMEVALUE\n"
        "    value: 1.018\n"
        "  TFEDSHR:\n"
        "    method: SAMEVALUE\n"
        "    value: 1.0\n"
        "  TSLSHR:\n"
        "    method: SAMEVALUE\n"
        "    value: 1.0\n",
        encoding="utf-8",
    )

    equation_input_path = experiment_series_path.with_name("EQUATION_INPUT_SNAPSHOT.csv")
    with equation_input_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["period", "iteration", "target", "trace_kind", "variable", "lag", "source_name", "source_period", "value", "solve_stage"],
        )
        writer.writeheader()
        writer.writerows(
            [
                {
                    "period": "2026.1",
                    "iteration": 1,
                    "target": "LUB",
                    "trace_kind": "previous_value",
                    "variable": "LUB",
                    "lag": 0,
                    "source_name": "",
                    "source_period": "2026.1",
                    "value": 2.25915553506671,
                    "solve_stage": 1,
                },
                {
                    "period": "2026.1",
                    "iteration": 1,
                    "target": "LUB",
                    "trace_kind": "evaluated_structural",
                    "variable": "LUB",
                    "lag": 0,
                    "source_name": "",
                    "source_period": "2026.1",
                    "value": 2.33854682266921,
                    "solve_stage": 1,
                },
                {
                    "period": "2026.1",
                    "iteration": 1,
                    "target": "LUB",
                    "trace_kind": "evaluated_value",
                    "variable": "LUB",
                    "lag": 0,
                    "source_name": "",
                    "source_period": "2026.1",
                    "value": 2.26677150101052,
                    "solve_stage": 1,
                },
                {
                    "period": "2026.1",
                    "iteration": 1,
                    "target": "LUB",
                    "trace_kind": "compiled_reference",
                    "variable": "LU",
                    "lag": 0,
                    "source_name": "LU",
                    "source_period": "2026.1",
                    "value": 2.03,
                    "solve_stage": 1,
                },
                {
                    "period": "2026.1",
                    "iteration": 1,
                    "target": "LUB",
                    "trace_kind": "compiled_reference",
                    "variable": "LWF",
                    "lag": 0,
                    "source_name": "LWF",
                    "source_period": "2026.1",
                    "value": -2.91,
                    "solve_stage": 1,
                },
                {
                    "period": "2026.1",
                    "iteration": 1,
                    "target": "LUB",
                    "trace_kind": "active_fsr_reference",
                    "variable": "LUB",
                    "lag": -1,
                    "source_name": "LUB",
                    "source_period": "2025.4",
                    "value": 2.25915553506671,
                    "solve_stage": 1,
                },
                {
                    "period": "2026.1",
                    "iteration": 1,
                    "target": "LUB",
                    "trace_kind": "active_fsr_reference",
                    "variable": "PCPD",
                    "lag": -1,
                    "source_name": "PCPD",
                    "source_period": "2025.4",
                    "value": 3.14,
                    "solve_stage": 1,
                },
            ]
        )
    baseline_equation_input_path = baseline_series_path.with_name("EQUATION_INPUT_SNAPSHOT.csv")
    with baseline_equation_input_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["period", "iteration", "target", "trace_kind", "variable", "lag", "source_name", "source_period", "value", "solve_stage"],
        )
        writer.writeheader()
        writer.writerows(
            [
                {"period": "2026.1", "iteration": 1, "target": "RS", "trace_kind": "previous_value", "variable": "RS", "lag": 0, "source_name": "", "source_period": "2026.1", "value": 3.73, "solve_stage": 1},
                {"period": "2026.1", "iteration": 1, "target": "RS", "trace_kind": "evaluated_structural", "variable": "RS", "lag": 0, "source_name": "", "source_period": "2026.1", "value": 3.74, "solve_stage": 1},
                {"period": "2026.1", "iteration": 1, "target": "RS", "trace_kind": "evaluated_value", "variable": "RS", "lag": 0, "source_name": "", "source_period": "2026.1", "value": 3.74, "solve_stage": 1},
                {"period": "2026.1", "iteration": 1, "target": "RS", "trace_kind": "compiled_reference", "variable": "PCPD", "lag": 0, "source_name": "PCPD", "source_period": "2026.1", "value": 3.14, "solve_stage": 1},
                {"period": "2026.1", "iteration": 1, "target": "RS", "trace_kind": "compiled_reference", "variable": "UR", "lag": 0, "source_name": "UR", "source_period": "2026.1", "value": 0.05, "solve_stage": 1},
                {"period": "2026.1", "iteration": 2, "target": "RS", "trace_kind": "previous_value", "variable": "RS", "lag": 0, "source_name": "", "source_period": "2026.1", "value": 3.80, "solve_stage": 1},
                {"period": "2026.1", "iteration": 2, "target": "RS", "trace_kind": "evaluated_structural", "variable": "RS", "lag": 0, "source_name": "", "source_period": "2026.1", "value": 3.81, "solve_stage": 1},
                {"period": "2026.1", "iteration": 2, "target": "RS", "trace_kind": "evaluated_value", "variable": "RS", "lag": 0, "source_name": "", "source_period": "2026.1", "value": 3.81, "solve_stage": 1},
                {"period": "2026.1", "iteration": 2, "target": "RS", "trace_kind": "compiled_reference", "variable": "PCPD", "lag": 0, "source_name": "PCPD", "source_period": "2026.1", "value": 3.20, "solve_stage": 1},
                {"period": "2026.1", "iteration": 2, "target": "RS", "trace_kind": "compiled_reference", "variable": "UR", "lag": 0, "source_name": "UR", "source_period": "2026.1", "value": 0.045, "solve_stage": 1},
                {"period": "2026.1", "iteration": 2, "target": "RS", "trace_kind": "compiled_reference", "variable": "UR1", "lag": 0, "source_name": "UR1", "source_period": "2026.1", "value": -0.001, "solve_stage": 1},
            ]
        )
    transfer_equation_input_path = transfer_series_path.with_name("EQUATION_INPUT_SNAPSHOT.csv")
    with transfer_equation_input_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["period", "iteration", "target", "trace_kind", "variable", "lag", "source_name", "source_period", "value", "solve_stage"],
        )
        writer.writeheader()
        writer.writerows(
            [
                {"period": "2026.1", "iteration": 1, "target": "RS", "trace_kind": "previous_value", "variable": "RS", "lag": 0, "source_name": "", "source_period": "2026.1", "value": 3.73, "solve_stage": 1},
                {"period": "2026.1", "iteration": 1, "target": "RS", "trace_kind": "evaluated_structural", "variable": "RS", "lag": 0, "source_name": "", "source_period": "2026.1", "value": 3.7395, "solve_stage": 1},
                {"period": "2026.1", "iteration": 1, "target": "RS", "trace_kind": "evaluated_value", "variable": "RS", "lag": 0, "source_name": "", "source_period": "2026.1", "value": 3.7395, "solve_stage": 1},
                {"period": "2026.1", "iteration": 1, "target": "RS", "trace_kind": "compiled_reference", "variable": "PCPD", "lag": 0, "source_name": "PCPD", "source_period": "2026.1", "value": 3.14, "solve_stage": 1},
                {"period": "2026.1", "iteration": 1, "target": "RS", "trace_kind": "compiled_reference", "variable": "UR", "lag": 0, "source_name": "UR", "source_period": "2026.1", "value": 0.0501, "solve_stage": 1},
                {"period": "2026.1", "iteration": 2, "target": "RS", "trace_kind": "previous_value", "variable": "RS", "lag": 0, "source_name": "", "source_period": "2026.1", "value": 3.799, "solve_stage": 1},
                {"period": "2026.1", "iteration": 2, "target": "RS", "trace_kind": "evaluated_structural", "variable": "RS", "lag": 0, "source_name": "", "source_period": "2026.1", "value": 3.7991, "solve_stage": 1},
                {"period": "2026.1", "iteration": 2, "target": "RS", "trace_kind": "evaluated_value", "variable": "RS", "lag": 0, "source_name": "", "source_period": "2026.1", "value": 3.7991, "solve_stage": 1},
                {"period": "2026.1", "iteration": 2, "target": "RS", "trace_kind": "compiled_reference", "variable": "PCPD", "lag": 0, "source_name": "PCPD", "source_period": "2026.1", "value": 3.19, "solve_stage": 1},
                {"period": "2026.1", "iteration": 2, "target": "RS", "trace_kind": "compiled_reference", "variable": "UR", "lag": 0, "source_name": "UR", "source_period": "2026.1", "value": 0.0502, "solve_stage": 1},
                {"period": "2026.1", "iteration": 2, "target": "RS", "trace_kind": "compiled_reference", "variable": "UR1", "lag": 0, "source_name": "UR1", "source_period": "2026.1", "value": -0.0008, "solve_stage": 1},
            ]
        )

    estimation_equations_path = experiment_series_path.with_name("ESTIMATION_EQUATIONS.csv")
    with estimation_equations_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "request_order",
                "request_command",
                "sample_start",
                "sample_end",
                "method",
                "equation_number",
                "target",
                "detail_source",
                "rho_order",
                "rhs_count",
                "eq_reference_names",
                "fsr_reference_names",
                "active_fsr_reference_names",
                "fsr_token_count",
                "fsr_name_count",
                "fsr_max_lag",
                "fsr_has_lags",
                "modeq_name_count",
                "modeq_fsr_name_count",
                "modeq_shared_name_count",
                "modeq_active_fsr_tokens",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "request_order": 1,
                "request_command": "EST",
                "sample_start": "1954.1",
                "sample_end": "2000.4",
                "method": "2SLS",
                "equation_number": 28,
                "target": "LUB",
                "detail_source": "equations",
                "rho_order": 1,
                "rhs_count": 4,
                "eq_reference_names": "",
                "fsr_reference_names": "C LUB LU LWF LCOGSZ LTRGSZ LEXZ LPIMZ PCPD T",
                "active_fsr_reference_names": "NA",
                "fsr_token_count": 11,
                "fsr_name_count": 10,
                "fsr_max_lag": 2,
                "fsr_has_lags": "TRUE",
                "modeq_name_count": "NA",
                "modeq_fsr_name_count": "NA",
                "modeq_shared_name_count": "NA",
                "modeq_active_fsr_tokens": "NA",
            }
        )
    for path in (
        baseline_series_path.with_name("ESTIMATION_EQUATIONS.csv"),
        transfer_series_path.with_name("ESTIMATION_EQUATIONS.csv"),
    ):
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "request_order",
                    "request_command",
                    "sample_start",
                    "sample_end",
                    "method",
                    "equation_number",
                    "target",
                    "detail_source",
                    "rho_order",
                    "rhs_count",
                    "eq_reference_names",
                    "fsr_reference_names",
                    "active_fsr_reference_names",
                    "fsr_token_count",
                    "fsr_name_count",
                    "fsr_max_lag",
                    "fsr_has_lags",
                    "modeq_name_count",
                    "modeq_fsr_name_count",
                    "modeq_shared_name_count",
                    "modeq_active_fsr_tokens",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "request_order": 1,
                    "request_command": "EST",
                    "sample_start": "1954.1",
                    "sample_end": "2008.3",
                    "method": "2SLS",
                    "equation_number": 30,
                    "target": "RS",
                    "detail_source": "equations",
                    "rho_order": 0,
                    "rhs_count": 9,
                    "eq_reference_names": "",
                    "fsr_reference_names": "C RS PCPD UR UR1 PCM1L1B PCM1L1A RS1 LCOGSZ LTRGSZ LEXZ",
                    "active_fsr_reference_names": "NA",
                    "fsr_token_count": 12,
                    "fsr_name_count": 11,
                    "fsr_max_lag": 2,
                    "fsr_has_lags": "TRUE",
                    "modeq_name_count": "NA",
                    "modeq_fsr_name_count": "NA",
                    "modeq_shared_name_count": "NA",
                    "modeq_active_fsr_tokens": "NA",
                }
            )

    compare_report_path = paths.runtime_distribution_reports_root / "compare_phase1_distribution_backends.json"
    compare_report_path.parent.mkdir(parents=True, exist_ok=True)
    compare_report_path.write_text(
        json.dumps(
            {
                "left_backend": "fp-r",
                "right_backend": "fpexe",
                "comparisons": [
                    {
                        "variant_id": "baseline-observed",
                        "left_loadformat_path": str(baseline_series_path.with_name("LOADFORMAT.DAT")),
                        "right_loadformat_path": str(tmp_path / "other" / "baseline" / "LOADFORMAT.DAT"),
                    },
                    {
                        "variant_id": "ui-relief",
                        "left_loadformat_path": str(ui_series_path.with_name("LOADFORMAT.DAT")),
                        "right_loadformat_path": str(tmp_path / "other" / "ui" / "LOADFORMAT.DAT"),
                    },
                    {
                        "variant_id": "transfer-composite-medium",
                        "left_loadformat_path": str(transfer_series_path.with_name("LOADFORMAT.DAT")),
                        "right_loadformat_path": str(tmp_path / "other" / "transfer" / "LOADFORMAT.DAT"),
                    },
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = analyze_phase1_distribution_driver_gap(
        compare_report_path=compare_report_path,
        ui_experiment_series_path=experiment_series_path,
        report_path=paths.runtime_distribution_reports_root / "analyze_phase1_distribution_driver_gap.json",
    )

    assert payload["ui_retained_target_lub_moves"] is True
    assert payload["transfer_rydpc_negative"] is True
    report = json.loads((paths.runtime_distribution_reports_root / "analyze_phase1_distribution_driver_gap.json").read_text(encoding="utf-8"))
    assert report["ui_retained_target_analysis"]["periods"]["2026.1"]["experiment_minus_default"]["LUB"] == pytest.approx(
        2.23070379191803 - 2.197224577
    )
    assert report["ui_retained_target_analysis"]["lub_equation_summary"]["has_uifac_reference"] is False
    assert report["ui_retained_target_analysis"]["lub_equation_summary"]["first_iteration"]["compiled_references"]["LU"] == pytest.approx(2.03)
    assert report["ui_retained_target_analysis"]["lub_uplift_breakdown"]["carry_lift_vs_default"] == pytest.approx(
        2.25915553506671 - 2.197224577
    )
    assert report["ui_retained_target_analysis"]["lub_uplift_breakdown"]["solve_step_from_previous"] == pytest.approx(
        2.26677150101052 - 2.25915553506671
    )
    transfer_periods = report["transfer_income_gap_analysis"]["periods"]
    assert transfer_periods["2026.1"]["scenario_minus_baseline"]["RYDPC"] < 0
    assert transfer_periods["2026.1"]["rydpc_identity_residual"] == pytest.approx(0.0)
    assert transfer_periods["2026.1"]["macro_indicator_deltas"]["RS"] == pytest.approx(-0.02)
    assert transfer_periods["2026.1"]["yd_component_breakdown"]["scenario_minus_baseline"]["transfers"] == pytest.approx(
        2.3599999999999994
    )
    assert transfer_periods["2026.1"]["yd_component_breakdown"]["scenario_minus_baseline"]["deductions"] == pytest.approx(
        0.8600000000000012
    )
    assert transfer_periods["2026.1"]["yd_term_breakdown"]["scenario_minus_baseline"]["RNT"] == pytest.approx(-2.0)
    assert transfer_periods["2026.1"]["yd_term_breakdown"]["scenario_minus_baseline"]["THG_effect"] == pytest.approx(-1.2)
    assert transfer_periods["2026.1"]["yd_term_breakdown"]["top_abs_delta_terms"][0]["term"] == "RNT"
    assert transfer_periods["2026.1"]["intg_upstream_bridge"]["scenario_minus_baseline"]["AAG"] == pytest.approx(0.0)
    assert transfer_periods["2026.1"]["ths_upstream_bridge"]["scenario_minus_baseline"]["SSFAC"] == pytest.approx(0.018)
    assert transfer_periods["2026.1"]["private_labor_upstream_bridge"]["scenario_minus_baseline"]["HO"] == pytest.approx(
        0.0
    )
    assert transfer_periods["2026.1"]["sg_upstream_bridge"]["scenario_minus_baseline"]["RECG"] == pytest.approx(1.0)
    assert transfer_periods["2026.1"]["sg_upstream_bridge"]["top_abs_delta_terms"][0]["term"] == "TRGH"
    assert transfer_periods["2026.1"]["aag_upstream_bridge"]["scenario_minus_baseline"]["SG"] == pytest.approx(0.0)
    assert report["transfer_income_gap_analysis"]["scenario_input_overrides"]["scenario"]["SSFAC"] == pytest.approx(1.018)
    assert report["transfer_income_gap_analysis"]["scenario_input_overrides"]["scenario"]["TSLSHR"] == pytest.approx(1.0)
    assert report["transfer_income_gap_analysis"]["scenario_spec"]["trsh_factor"] == pytest.approx(1.018637285379202)
    assert report["transfer_income_gap_analysis"]["scenario_spec"]["trfin_fed_share"] == pytest.approx(1.0)
    assert transfer_periods["2029.4"]["gdpr_component_breakdown"]["scenario_minus_baseline"]["output_y"] == pytest.approx(
        -1.0
    )
    assert transfer_periods["2026.1"]["pcy_growth_bridge"]["scenario"]["current_y"] == pytest.approx(119.5)
    rs_compare = report["transfer_income_gap_analysis"]["rs_equation_comparison"]
    assert rs_compare["baseline"]["equation_number"] == 30
    assert rs_compare["scenario"]["last_iteration"]["evaluated_value"] == pytest.approx(3.7991)
    assert rs_compare["last_iteration_compiled_reference_deltas"]["PCPD"] == pytest.approx(-0.01)
    assert rs_compare["last_iteration_compiled_reference_deltas"]["UR"] == pytest.approx(0.0052)
    propagation = report["transfer_income_gap_analysis"]["propagation_summary"]
    assert propagation["yd_gap_monotone_more_negative"] is True
    assert propagation["gdpr_gap_monotone_more_negative"] is True
    assert propagation["ub_gap_constant"] is True
    assert propagation["trgh_gap_monotone_smaller"] is True
    assert propagation["trsh_gap_monotone_larger"] is True
    dynamics = report["transfer_income_gap_analysis"]["dynamics_summary"]
    assert dynamics["intg_sg_aag_loop_signature"] is False
    assert dynamics["jf_path_looks_lag_persistent"] is True


def test_distribution_identity_checks_flag_scaled_ui_gap() -> None:
    spec = phase1_scenario_by_variant()["transfer-composite-medium"]
    periods = ["2025.4", "2026.1", "2026.2"]
    ub = np.array([100.0, 120.0, 121.0], dtype=float)
    trsh = np.array([80.0, 90.0, 91.0], dtype=float)
    gdpd = np.array([2.0, 2.1, 2.2], dtype=float)
    d1g = np.array([5.0, 5.0, 5.0], dtype=float)
    d1s = np.array([4.0, 4.0, 4.0], dtype=float)
    yt = np.array([10.0, 10.0, 10.0], dtype=float)
    series = {
        "UB": ub.tolist(),
        "LUB": np.log(ub / spec.ui_factor).tolist(),
        "THG": (d1g * yt + (ub - (ub / spec.ui_factor) + spec.trgh_delta_q * gdpd)).tolist(),
        "D1G": d1g.tolist(),
        "YT": yt.tolist(),
        "GDPD": gdpd.tolist(),
        "THS": (d1s * yt + (trsh - (trsh / spec.trsh_factor))).tolist(),
        "D1S": d1s.tolist(),
        "TRSH": trsh.tolist(),
    }

    checks = _distribution_identity_checks(periods, series, spec)

    assert checks["ub_scaled"]["max_abs_residual"] == pytest.approx(0.0)
    assert checks["thg_financing"]["max_abs_residual"] == pytest.approx(0.0)
    assert checks["ths_financing"]["max_abs_residual"] == pytest.approx(0.0)
    assert checks["ub_unscaled"]["max_abs_residual"] > 1.0
    assert checks["ub_scaled"]["period_start"] == "2026.1"
    assert checks["ub_scaled"]["period_end"] == "2026.2"


def test_distribution_identity_checks_respect_short_forecast_window() -> None:
    spec = phase1_scenario_by_variant()["ui-relief"]
    periods = ["2025.4", "2026.1", "2026.2"]
    series = {
        "UB": [9.575, 9.49254182490513, 9.575],
        "LUB": [2.197224577, 2.23070379191803, 2.197224577],
        "UIFAC": [1.0, 1.02, 1.0],
        "THG": [0.0, 0.0, 0.0],
        "D1G": [0.0, 0.0, 0.0],
        "YT": [0.0, 0.0, 0.0],
        "GDPD": [0.0, 0.0, 0.0],
        "THS": [0.0, 0.0, 0.0],
        "D1S": [0.0, 0.0, 0.0],
        "TRSH": [0.0, 0.0, 0.0],
    }

    full_window = _distribution_identity_checks(periods, series, spec)
    short_window = _distribution_identity_checks(
        periods,
        series,
        spec,
        forecast_start="2026.1",
        forecast_end="2026.1",
    )

    assert full_window["ub_scaled"]["max_abs_residual"] == pytest.approx(0.3950000030864924)
    assert full_window["ub_scaled"]["period_of_max_abs_residual"] == "2026.2"
    assert short_window["ub_scaled"]["max_abs_residual"] == pytest.approx(0.0)
    assert short_window["ub_scaled"]["period_of_max_abs_residual"] == "2026.1"


def test_validate_phase1_distribution_identities_writes_pass_fail_report(
    tmp_path, monkeypatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    run_report_path = paths.runtime_distribution_reports_root / "run_phase1_distribution_block.json"
    run_report_path.parent.mkdir(parents=True, exist_ok=True)
    run_report_path.write_text(
        json.dumps(
            {
                "scenarios": {
                    "ui-relief": {
                        "backend": "fp-r",
                        "loadformat_path": "/tmp/ui/LOADFORMAT.DAT",
                    },
                    "transfer-composite-medium": {
                        "backend": "fp-r",
                        "loadformat_path": "/tmp/tc/LOADFORMAT.DAT",
                    },
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )

    def fake_identity_checks(loadformat_path: Path, spec, **_kwargs) -> dict[str, dict[str, object]]:
        if "ui" in str(loadformat_path):
            return {
                "ub_scaled": {"evaluated": True, "max_abs_residual": 1e-9},
                "thg_financing": {"evaluated": True, "max_abs_residual": 2e-9},
                "ths_financing": {"evaluated": True, "max_abs_residual": 3e-9},
                "ub_unscaled": {"evaluated": True, "max_abs_residual": 0.18},
            }
        return {
            "ub_scaled": {"evaluated": True, "max_abs_residual": 2e-4},
            "thg_financing": {"evaluated": True, "max_abs_residual": 4e-9},
            "ths_financing": {"evaluated": True, "max_abs_residual": 5e-9},
            "ub_unscaled": {"evaluated": True, "max_abs_residual": 0.16},
        }

    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)
    monkeypatch.setattr(
        "fp_ineq.phase1_distribution_block._distribution_identity_checks_for_loadformat",
        fake_identity_checks,
    )

    payload = validate_phase1_distribution_identities(
        backend="fp-r",
        variant_ids=("ui-relief", "transfer-composite-medium"),
        run_report_path=run_report_path,
        report_path=paths.runtime_distribution_reports_root / "validate_phase1_distribution_identities.fp-r.json",
        max_abs_residual=1e-6,
    )

    assert payload["variant_count"] == 2
    assert payload["passes"] is False
    report = json.loads(
        (paths.runtime_distribution_reports_root / "validate_phase1_distribution_identities.fp-r.json").read_text(
            encoding="utf-8"
        )
    )
    assert report["summary"]["passes"] is False
    assert report["summary"]["max_required_identity_residual"] == pytest.approx(2e-4)
    assert report["variants"][0]["passes_required_identities"] is True
    assert report["variants"][1]["passes_required_identities"] is False
    assert report["variants"][1]["required_identity_checks"]["ub_scaled"]["passes"] is False


def test_validate_phase1_distribution_identities_can_require_unscaled_ub_for_transformed_lhs(
    tmp_path, monkeypatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    run_report_path = paths.runtime_distribution_reports_root / "run_phase1_distribution_block.json"
    run_report_path.parent.mkdir(parents=True, exist_ok=True)
    run_report_path.write_text(
        json.dumps(
            {
                "scenarios": {
                    "ui-relief": {
                        "backend": "fp-r",
                        "loadformat_path": "/tmp/ui/LOADFORMAT.DAT",
                    },
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )

    def fake_identity_checks(loadformat_path: Path, spec, **_kwargs) -> dict[str, dict[str, object]]:
        return {
            "ub_scaled": {"evaluated": True, "max_abs_residual": 12.0},
            "ub_unscaled": {"evaluated": True, "max_abs_residual": 1e-9},
            "thg_financing": {"evaluated": True, "max_abs_residual": 2e-9},
            "ths_financing": {"evaluated": True, "max_abs_residual": 3e-9},
        }

    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)
    monkeypatch.setattr(
        "fp_ineq.phase1_distribution_block._distribution_identity_checks_for_loadformat",
        fake_identity_checks,
    )

    payload = validate_phase1_distribution_identities(
        backend="fp-r",
        variant_ids=("ui-relief",),
        run_report_path=run_report_path,
        report_path=paths.runtime_distribution_reports_root / "validate.unscaled.json",
        max_abs_residual=1e-6,
        ub_identity_mode="unscaled",
        forecast_start="2026.1",
        forecast_end="2026.1",
    )

    assert payload["passes"] is True
    report = json.loads((paths.runtime_distribution_reports_root / "validate.unscaled.json").read_text())
    assert report["summary"]["ub_identity_mode"] == "unscaled"
    assert report["summary"]["forecast_start"] == "2026.1"
    assert report["summary"]["forecast_end"] == "2026.1"
    assert report["summary"]["required_identity_ids"] == ["ub_unscaled", "thg_financing", "ths_financing"]
    assert report["variants"][0]["required_identity_checks"]["ub_unscaled"]["passes"] is True
    assert "ub_scaled" not in report["variants"][0]["required_identity_checks"]


def test_validate_phase1_distribution_identities_passes_timeout_to_generated_run(
    tmp_path, monkeypatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    observed: dict[str, object] = {}

    def fake_run_phase1_distribution_block(**kwargs):
        observed.update(kwargs)
        report_path = Path(kwargs["report_path"])
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                {
                    "scenario_forecast_start": "2026.1",
                    "scenario_forecast_end": "2029.4",
                    "scenarios": {
                        "ui-relief": {
                            "backend": "fp-r",
                            "loadformat_path": "/tmp/ui/LOADFORMAT.DAT",
                        }
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        return {"report_path": str(report_path), "backend": kwargs["backend"], "passes": True}

    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)
    monkeypatch.setattr("fp_ineq.phase1_distribution_block.run_phase1_distribution_block", fake_run_phase1_distribution_block)
    monkeypatch.setattr(
        "fp_ineq.phase1_distribution_block._distribution_identity_checks_for_loadformat",
        lambda *args, **kwargs: {
            "ub_scaled": {"evaluated": True, "max_abs_residual": 0.0},
            "thg_financing": {"evaluated": True, "max_abs_residual": 0.0},
            "ths_financing": {"evaluated": True, "max_abs_residual": 0.0},
            "ub_unscaled": {"evaluated": True, "max_abs_residual": 0.1},
        },
    )

    payload = validate_phase1_distribution_identities(
        fp_home=tmp_path / "FM",
        backend="fp-r",
        variant_ids=("ui-relief",),
        run_report_path=paths.runtime_distribution_reports_root / "missing.json",
        report_path=paths.runtime_distribution_reports_root / "validate_phase1_distribution_identities.fp-r.json",
        max_abs_residual=1e-6,
        fpr_timeout_seconds=3600,
    )

    assert payload["variant_count"] == 1
    assert observed["fpr_timeout_seconds"] == 3600


def test_analyze_phase1_distribution_policy_gap_reports_first_and_last_deltas(
    tmp_path, monkeypatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    compare_report_path = paths.runtime_distribution_reports_root / "compare_phase1_distribution_backends.json"
    compare_report_path.parent.mkdir(parents=True, exist_ok=True)
    compare_report_path.write_text(
        json.dumps(
            {
                "left_backend": "fp-r",
                "right_backend": "fpexe",
                "comparisons": [
                    {
                        "variant_id": "baseline-observed",
                        "left_loadformat_path": "/tmp/fp-r/baseline/LOADFORMAT.DAT",
                        "right_loadformat_path": "/tmp/fpexe/baseline/LOADFORMAT.DAT",
                    },
                    {
                        "variant_id": "ui-relief",
                        "left_loadformat_path": "/tmp/fp-r/ui/LOADFORMAT.DAT",
                        "right_loadformat_path": "/tmp/fpexe/ui/LOADFORMAT.DAT",
                    },
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payloads = {
        "/tmp/fp-r/baseline/LOADFORMAT.DAT": (
            ["2025.4", "2026.1", "2029.4"],
            {"UB": [9.0, 9.0, 9.0], "TRLOWZ": [1.0, 1.0, 1.0]},
        ),
        "/tmp/fp-r/ui/LOADFORMAT.DAT": (
            ["2025.4", "2026.1", "2029.4"],
            {"UB": [9.0, 9.18, 9.18], "TRLOWZ": [1.0, 1.001, 1.002]},
        ),
        "/tmp/fpexe/baseline/LOADFORMAT.DAT": (
            ["2025.4", "2026.1", "2029.4"],
            {"UB": [9.0, 9.0, 9.0], "TRLOWZ": [1.0, 1.0, 1.0]},
        ),
        "/tmp/fpexe/ui/LOADFORMAT.DAT": (
            ["2025.4", "2026.1", "2029.4"],
            {"UB": [9.0, 65.2, 65.2], "TRLOWZ": [1.0, 1.14, 1.138]},
        ),
    }
    fp_r_series_payloads = {
        "/tmp/fp-r/baseline/fp_r_series.csv": (
            ["2025.4", "2026.1", "2029.4"],
            {"IPOVALL": [0.11, 0.11, 0.11]},
        ),
        "/tmp/fp-r/ui/fp_r_series.csv": (
            ["2025.4", "2026.1", "2029.4"],
            {"IPOVALL": [0.11, 0.1099, 0.1098]},
        ),
    }

    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)
    monkeypatch.setattr(
        "fp_ineq.phase1_distribution_block._read_loadformat_payload",
        lambda path: payloads[str(path)],
    )
    monkeypatch.setattr(
        "fp_ineq.phase1_distribution_block._read_fp_r_series_payload",
        lambda path: fp_r_series_payloads.get(str(path), ([], {})),
    )

    payload = analyze_phase1_distribution_policy_gap(
        compare_report_path=compare_report_path,
        variant_ids=("ui-relief",),
        variables=("UB", "TRLOWZ", "IPOVALL"),
        report_path=paths.runtime_distribution_reports_root / "analyze_phase1_distribution_policy_gap.json",
    )

    assert payload["variant_count"] == 1
    report = json.loads(
        (paths.runtime_distribution_reports_root / "analyze_phase1_distribution_policy_gap.json").read_text(
            encoding="utf-8"
        )
    )
    ub = report["variants"][0]["variables"]["UB"]
    trlowz = report["variants"][0]["variables"]["TRLOWZ"]
    assert ub["fp-r"]["delta_first"] == pytest.approx(0.18)
    assert ub["fpexe"]["delta_first"] == pytest.approx(56.2)
    assert ub["first_gap_ratio_abs"] == pytest.approx(0.18 / 56.2)
    assert trlowz["fp-r"]["delta_last"] == pytest.approx(0.002)
    assert trlowz["fpexe"]["delta_last"] == pytest.approx(0.138)
    assert report["variants"][0]["variables"]["IPOVALL"]["fp-r"]["delta_first"] == pytest.approx(-0.0001)


def test_analyze_phase1_distribution_first_levels_reports_direct_first_quarter_levels(
    tmp_path, monkeypatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    compare_report_path = paths.runtime_distribution_reports_root / "compare_phase1_distribution_backends.json"
    compare_report_path.parent.mkdir(parents=True, exist_ok=True)
    compare_report_path.write_text(
        json.dumps(
            {
                "left_backend": "fp-r",
                "right_backend": "fpexe",
                "comparisons": [
                    {
                        "variant_id": "baseline-observed",
                        "left_loadformat_path": "/tmp/fp-r/baseline/LOADFORMAT.DAT",
                        "right_loadformat_path": "/tmp/fpexe/baseline/LOADFORMAT.DAT",
                    },
                    {
                        "variant_id": "ui-relief",
                        "left_loadformat_path": "/tmp/fp-r/ui/LOADFORMAT.DAT",
                        "right_loadformat_path": "/tmp/fpexe/ui/LOADFORMAT.DAT",
                    },
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payloads = {
        "/tmp/fp-r/baseline/LOADFORMAT.DAT": (
            ["2025.4", "2026.1", "2029.4"],
            {"UB": [9.0, 9.0, 9.0], "TRGH": [3.0, 3.0, 3.0]},
        ),
        "/tmp/fp-r/ui/LOADFORMAT.DAT": (
            ["2025.4", "2026.1", "2029.4"],
            {"UB": [9.0, 9.18, 9.18], "TRGH": [3.0, 3.4, 3.45]},
        ),
        "/tmp/fpexe/baseline/LOADFORMAT.DAT": (
            ["2025.4", "2026.1", "2029.4"],
            {"UB": [9.0, 9.0, 9.0], "TRGH": [3.0, 3.0, 3.0]},
        ),
        "/tmp/fpexe/ui/LOADFORMAT.DAT": (
            ["2025.4", "2026.1", "2029.4"],
            {"UB": [9.0, 65.2, 65.2], "TRGH": [3.0, 3.9, 4.0]},
        ),
    }
    fp_r_series_payloads = {
        "/tmp/fp-r/baseline/fp_r_series.csv": (
            ["2025.4", "2026.1", "2029.4"],
            {"IPOVALL": [0.11, 0.11, 0.11]},
        ),
        "/tmp/fp-r/ui/fp_r_series.csv": (
            ["2025.4", "2026.1", "2029.4"],
            {"IPOVALL": [0.11, 0.1099, 0.1098]},
        ),
    }

    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)
    monkeypatch.setattr(
        "fp_ineq.phase1_distribution_block._read_loadformat_payload",
        lambda path: payloads[str(path)],
    )
    monkeypatch.setattr(
        "fp_ineq.phase1_distribution_block._read_fp_r_series_payload",
        lambda path: fp_r_series_payloads.get(str(path), ([], {})),
    )

    payload = analyze_phase1_distribution_first_levels(
        compare_report_path=compare_report_path,
        variant_ids=("ui-relief",),
        variables=("UB", "TRGH", "IPOVALL"),
        report_path=paths.runtime_distribution_reports_root / "analyze_phase1_distribution_first_levels.json",
    )

    assert payload["variant_count"] == 1
    report = json.loads(
        (paths.runtime_distribution_reports_root / "analyze_phase1_distribution_first_levels.json").read_text(
            encoding="utf-8"
        )
    )
    ub = report["variants"][0]["variables"]["UB"]
    trgh = report["variants"][0]["variables"]["TRGH"]
    ipovall = report["variants"][0]["variables"]["IPOVALL"]
    assert ub["fp-r"]["scenario_first"] == pytest.approx(9.18)
    assert ub["fpexe"]["scenario_first"] == pytest.approx(65.2)
    assert ub["level_abs_diff"] == pytest.approx(65.2 - 9.18)
    assert ub["delta_abs_diff"] == pytest.approx(56.2 - 0.18)
    assert ub["delta_ratio_abs"] == pytest.approx(0.18 / 56.2)
    assert trgh["fp-r"]["delta_first"] == pytest.approx(0.4)
    assert trgh["fpexe"]["delta_first"] == pytest.approx(0.9)
    assert ipovall["fp-r"]["scenario_first"] == pytest.approx(0.1099)
    assert ipovall["fpexe"]["evaluated"] is False


def test_assess_phase1_distribution_backend_boundary_flags_ui_attenuation_and_transfer_macro_block(
    tmp_path, monkeypatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)

    first_levels_path = paths.runtime_distribution_reports_root / "analyze_phase1_distribution_first_levels.json"
    identity_path = paths.runtime_distribution_reports_root / "validate_phase1_distribution_identities.fp-r.json"
    driver_gap_path = paths.runtime_distribution_reports_root / "analyze_phase1_distribution_driver_gap.json"
    first_levels_path.parent.mkdir(parents=True, exist_ok=True)

    first_levels_path.write_text(
        json.dumps(
            {
                "left_backend": "fp-r",
                "right_backend": "fpexe",
                "variants": [
                    {
                        "variant_id": "ui-relief",
                        "variables": {
                            "UB": {
                                "fp-r": {"evaluated": True, "scenario_first": 9.18, "delta_first": 0.18},
                                "fpexe": {"evaluated": True, "scenario_first": 65.2, "delta_first": 56.2},
                                "delta_ratio_abs": 0.18 / 56.2,
                                "delta_sign_match": True,
                                "level_abs_diff": 56.02,
                                "delta_abs_diff": 56.02,
                            },
                            "YD": {
                                "fp-r": {"evaluated": True, "scenario_first": 5648.39, "delta_first": 0.19},
                                "fpexe": {"evaluated": True, "scenario_first": 5706.68, "delta_first": 58.48},
                                "delta_ratio_abs": 0.19 / 58.48,
                                "delta_sign_match": True,
                                "level_abs_diff": 58.29,
                                "delta_abs_diff": 58.29,
                            },
                        },
                    },
                    {
                        "variant_id": "transfer-composite-medium",
                        "variables": {
                            "TRGH": {
                                "fp-r": {"evaluated": True, "scenario_first": 924.00, "delta_first": 2.45},
                                "fpexe": {"evaluated": True, "scenario_first": 924.07, "delta_first": 2.52},
                                "delta_ratio_abs": 0.97,
                                "delta_sign_match": True,
                                "level_abs_diff": 0.07,
                                "delta_abs_diff": 0.07,
                            },
                            "TRSH": {
                                "fp-r": {"evaluated": True, "scenario_first": 300.90, "delta_first": 5.50},
                                "fpexe": {"evaluated": True, "scenario_first": 300.93, "delta_first": 5.53},
                                "delta_ratio_abs": 1.0,
                                "delta_sign_match": True,
                                "level_abs_diff": 0.02,
                                "delta_abs_diff": 0.02,
                            },
                            "THS": {
                                "fp-r": {"evaluated": True, "scenario_first": 172.27, "delta_first": 5.41},
                                "fpexe": {"evaluated": True, "scenario_first": 172.44, "delta_first": 5.57},
                                "delta_ratio_abs": 0.97,
                                "delta_sign_match": True,
                                "level_abs_diff": 0.16,
                                "delta_abs_diff": 0.16,
                            },
                            "YD": {
                                "fp-r": {"evaluated": True, "scenario_first": 5645.55, "delta_first": -2.65},
                                "fpexe": {"evaluated": True, "scenario_first": 5697.11, "delta_first": 48.91},
                                "delta_ratio_abs": 0.054,
                                "delta_sign_match": False,
                                "level_abs_diff": 51.56,
                                "delta_abs_diff": 51.56,
                            },
                            "GDPR": {
                                "fp-r": {"evaluated": True, "scenario_first": 6075.40, "delta_first": -0.22},
                                "fpexe": {"evaluated": True, "scenario_first": 6079.49, "delta_first": 3.87},
                                "delta_ratio_abs": 0.057,
                                "delta_sign_match": False,
                                "level_abs_diff": 4.09,
                                "delta_abs_diff": 4.09,
                            },
                            "RYDPC": {
                                "fp-r": {"evaluated": True, "scenario_first": 15.70, "delta_first": -0.0073},
                                "fpexe": {"evaluated": True, "scenario_first": 15.85, "delta_first": 0.135},
                                "delta_ratio_abs": 0.054,
                                "delta_sign_match": False,
                                "level_abs_diff": 0.142,
                                "delta_abs_diff": 0.142,
                            },
                            "RS": {
                                "fp-r": {"evaluated": True, "scenario_first": 3.95, "delta_first": -0.0009},
                                "fpexe": {"evaluated": True, "scenario_first": 3.97, "delta_first": 0.015},
                                "delta_ratio_abs": 0.059,
                                "delta_sign_match": False,
                                "level_abs_diff": 0.016,
                                "delta_abs_diff": 0.016,
                            },
                            "PCY": {
                                "fp-r": {"evaluated": True, "scenario_first": 1.0, "delta_first": -0.01},
                                "fpexe": {"evaluated": True, "scenario_first": 1.3, "delta_first": 0.29},
                                "delta_ratio_abs": 0.057,
                                "delta_sign_match": False,
                                "level_abs_diff": 0.30,
                                "delta_abs_diff": 0.30,
                            },
                            "UR": {
                                "fp-r": {"evaluated": True, "scenario_first": 0.04, "delta_first": 0.0001},
                                "fpexe": {"evaluated": True, "scenario_first": 0.0398, "delta_first": -0.0002},
                                "delta_ratio_abs": 0.058,
                                "delta_sign_match": False,
                                "level_abs_diff": 0.0002,
                                "delta_abs_diff": 0.0002,
                            },
                            "GDPD": {
                                "fp-r": {"evaluated": True, "scenario_first": 1.0, "delta_first": -0.0001},
                                "fpexe": {"evaluated": True, "scenario_first": 1.0001, "delta_first": 0.0017},
                                "delta_ratio_abs": 0.058,
                                "delta_sign_match": False,
                                "level_abs_diff": 0.0001,
                                "delta_abs_diff": 0.0001,
                            },
                        },
                    },
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    identity_path.write_text(
        json.dumps(
            {
                "variants": [
                    {"variant_id": "ui-relief", "passes_required_identities": True},
                    {"variant_id": "transfer-composite-medium", "passes_required_identities": True},
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    driver_gap_path.write_text(
        json.dumps(
            {
                "ui_retained_target_analysis": {
                    "lub_equation_summary": {"has_uifac_reference": False},
                    "lub_uplift_breakdown": {"solve_step_from_previous": 0.0076},
                },
                "transfer_income_gap_analysis": {
                    "periods": {
                        "2026.1": {
                            "scenario_minus_baseline": {
                                "YD": -2.6532,
                                "RS": -0.000885,
                            }
                        }
                    }
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payload = assess_phase1_distribution_backend_boundary(
        first_levels_report_path=first_levels_path,
        identity_report_path=identity_path,
        driver_gap_report_path=driver_gap_path,
        report_path=paths.runtime_distribution_reports_root / "assess_phase1_distribution_backend_boundary.json",
    )

    assert payload["identity_surface_passes"] is True
    assert payload["replacement_readiness"] == "not_ready"
    report = json.loads(
        (paths.runtime_distribution_reports_root / "assess_phase1_distribution_backend_boundary.json").read_text(
            encoding="utf-8"
        )
    )
    assert report["ui_relief"]["assessment"] == "open_attenuation"
    assert report["transfer_composite_medium"]["direct_channel_assessment"] == "pass_direct_channels"
    assert report["transfer_composite_medium"]["macro_channel_assessment"] == "block_macro_sign_flip"


def test_analyze_phase1_distribution_ui_attenuation_reports_structural_lub_block(
    tmp_path, monkeypatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)

    policy_gap_path = paths.runtime_distribution_reports_root / "analyze_phase1_distribution_policy_gap.json"
    driver_gap_path = paths.runtime_distribution_reports_root / "analyze_phase1_distribution_driver_gap.json"
    policy_gap_path.parent.mkdir(parents=True, exist_ok=True)
    policy_gap_path.write_text(
        json.dumps(
            {
                "left_backend": "fp-r",
                "right_backend": "fpexe",
                "variants": [
                    {
                        "variant_id": "ui-relief",
                        "variables": {
                            "LUB": {
                                "fp-r": {"delta_first": 0.0},
                                "fpexe": {"delta_first": 1.98},
                                "first_gap_ratio_abs": 0.0,
                                "first_sign_match": False,
                            },
                            "UB": {
                                "fp-r": {"delta_first": 0.18},
                                "fpexe": {"delta_first": 56.2},
                                "first_gap_ratio_abs": 0.18 / 56.2,
                                "first_sign_match": True,
                            },
                            "TRLOWZ": {
                                "fp-r": {"delta_first": 0.0005},
                                "fpexe": {"delta_first": 0.156},
                                "first_gap_ratio_abs": 0.0005 / 0.156,
                                "first_sign_match": True,
                            },
                            "IPOVALL": {
                                "fp-r": {"delta_first": -0.00001},
                                "fpexe": {"delta_first": -0.00279},
                                "first_gap_ratio_abs": 0.00358,
                                "first_sign_match": True,
                            },
                            "IPOVCH": {
                                "fp-r": {"delta_first": -0.00002},
                                "fpexe": {"delta_first": -0.00726},
                                "first_gap_ratio_abs": 0.00275,
                                "first_sign_match": True,
                            },
                            "RYDPC": {
                                "fp-r": {"delta_first": 0.00052},
                                "fpexe": {"delta_first": 0.161},
                                "first_gap_ratio_abs": 0.00323,
                                "first_sign_match": True,
                            },
                        },
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    driver_gap_path.write_text(
        json.dumps(
            {
                "ui_retained_target_analysis": {
                    "lub_equation_summary": {"has_uifac_reference": False},
                    "lub_uplift_breakdown": {
                        "default_2026q1_lub": 2.1972,
                        "retained_target_previous_value": 2.2592,
                        "retained_target_evaluated_value": 2.2668,
                        "carry_lift_vs_default": 0.0620,
                        "solve_lift_vs_default": 0.0696,
                        "solve_step_from_previous": 0.0076,
                    },
                    "periods": {
                        "2026.1": {
                            "experiment_minus_default": {
                                "LUB": 0.0335,
                                "UB": 0.3125,
                                "YD": 0.3261,
                                "GDPR": 0.0270,
                                "RYDPC": 0.0009,
                                "TRLOWZ": 0.00087,
                                "IPOVALL": -0.000016,
                                "IPOVCH": -0.000042,
                            }
                        }
                    },
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payload = analyze_phase1_distribution_ui_attenuation(
        policy_gap_report_path=policy_gap_path,
        driver_gap_report_path=driver_gap_path,
        report_path=paths.runtime_distribution_reports_root / "analyze_phase1_distribution_ui_attenuation.json",
    )

    assert payload["assessment"] == "structural_lub_channel_block"
    assert payload["core_median_gap_ratio_abs"] == pytest.approx(0.003205128205128205)
    report = json.loads(
        (paths.runtime_distribution_reports_root / "analyze_phase1_distribution_ui_attenuation.json").read_text(
            encoding="utf-8"
        )
    )
    assert report["default_fp_r_signature"]["lub_is_flat"] is True
    assert report["retained_target_counterfactual"]["lub_equation_has_uifac_reference"] is False
    assert report["retained_target_counterfactual"]["same_quarter_solve_share_of_total_lub_lift"] == pytest.approx(
        0.0076 / 0.0696
    )
    assert report["retained_target_counterfactual"]["first_quarter_lifts_vs_default"]["UB"] == pytest.approx(0.3125)


def test_analyze_phase1_distribution_transfer_macro_block_reports_income_path_block(
    tmp_path, monkeypatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)

    boundary_path = paths.runtime_distribution_reports_root / "assess_phase1_distribution_backend_boundary.json"
    driver_gap_path = paths.runtime_distribution_reports_root / "analyze_phase1_distribution_driver_gap.json"
    boundary_path.parent.mkdir(parents=True, exist_ok=True)
    boundary_path.write_text(
        json.dumps(
            {
                "transfer_composite_medium": {
                    "direct_channel_assessment": "pass_direct_channels",
                    "macro_channel_assessment": "block_macro_sign_flip",
                    "first_quarter_direct_stats": {"median_delta_ratio_abs": 0.59},
                    "first_quarter_macro_stats": {
                        "median_delta_ratio_abs": 0.057,
                        "sign_mismatch_count": 7,
                    },
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )
    driver_gap_path.write_text(
        json.dumps(
            {
                "transfer_income_gap_analysis": {
                    "propagation_summary": {"yd_gap_monotone_more_negative": True},
                    "dynamics_summary": {"intg_sg_aag_loop_signature": True},
                    "periods": {
                        "2026.1": {
                            "yd_component_breakdown": {
                                "scenario_minus_baseline": {
                                    "transfers": 8.13,
                                    "deductions": 7.65,
                                    "property_and_interest_income": -3.03,
                                    "reconstructed_yd": -2.65,
                                }
                            },
                            "yd_term_breakdown": {
                                "top_abs_delta_terms": [
                                    {"term": "TRSH", "delta": 5.50, "abs_delta": 5.50},
                                    {"term": "THS_effect", "delta": -5.41, "abs_delta": 5.41},
                                    {"term": "INTG", "delta": -4.02, "abs_delta": 4.02},
                                ]
                            },
                            "gdpr_component_breakdown": {
                                "scenario_minus_baseline": {
                                    "output_y": -0.22,
                                    "sector_hours_adjustment": 0.0,
                                    "statp": 0.0,
                                }
                            },
                            "rs_equation_comparison": {
                                "last_iteration_evaluated_value_delta": -0.000885,
                                "last_iteration_compiled_reference_deltas": {
                                    "PCPD": -0.001229,
                                    "UR": 0.000009,
                                },
                            },
                        },
                        "2029.4": {
                            "yd_component_breakdown": {
                                "scenario_minus_baseline": {
                                    "transfers": 8.82,
                                    "deductions": 7.60,
                                    "labor_total": -6.48,
                                    "property_and_interest_income": -6.99,
                                    "reconstructed_yd": -12.25,
                                }
                            },
                            "yd_term_breakdown": {
                                "top_abs_delta_terms": [
                                    {"term": "INTG", "delta": -8.01, "abs_delta": 8.01},
                                    {"term": "TRSH", "delta": 6.85, "abs_delta": 6.85},
                                    {"term": "THS_effect", "delta": -6.76, "abs_delta": 6.76},
                                ]
                            },
                            "intg_driver_bridge": {
                                "scenario_minus_baseline": {"INTG": -8.01, "AAG": -437.74},
                                "delta_breakdown": {"aag_component": -6.13, "intgz_component": -1.90},
                            },
                            "jf_driver_bridge": {
                                "scenario_minus_baseline": {"JF": -0.0758, "LJF1": -0.000003},
                                "delta_breakdown": {"lagged_jf_component": -0.0754, "ljf1_component": -0.00045},
                            },
                        },
                    },
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payload = analyze_phase1_distribution_transfer_macro_block(
        boundary_report_path=boundary_path,
        driver_gap_report_path=driver_gap_path,
        report_path=paths.runtime_distribution_reports_root / "analyze_phase1_distribution_transfer_macro_block.json",
    )

    assert payload["assessment"] == "macro_income_sign_flip_block"
    assert payload["macro_sign_mismatch_count"] == 7
    report = json.loads(
        (paths.runtime_distribution_reports_root / "analyze_phase1_distribution_transfer_macro_block.json").read_text(
            encoding="utf-8"
        )
    )
    assert report["first_quarter_signature"]["direct_channel_assessment"] == "pass_direct_channels"
    assert report["first_quarter_signature"]["yd_component_breakdown"]["reconstructed_yd"] == pytest.approx(-2.65)
    assert report["late_path_signature"]["intg_driver_bridge_2029q4"]["delta_breakdown"]["aag_component"] == pytest.approx(
        -6.13
    )
    assert report["late_path_signature"]["jf_driver_bridge_2029q4"]["scenario_minus_baseline"]["JF"] == pytest.approx(
        -0.0758
    )


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


def test_write_phase1_distribution_scenarios_supports_backend_specific_runtime_roots(
    tmp_path, monkeypatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    fp_home = tmp_path / "FM"
    fp_home.mkdir(parents=True, exist_ok=True)
    scenarios_root = paths.runtime_distribution_root / "scenarios-fpr"
    artifacts_root = paths.runtime_distribution_root / "artifacts-fpr"

    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)
    monkeypatch.setattr("fp_ineq.phase1_distribution_block.locate_fp_home", lambda path: Path(path))
    monkeypatch.setattr(
        "fp_ineq.phase1_distribution_block.build_phase1_distribution_overlay",
        lambda **_: {"overlay_root": str(paths.runtime_distribution_overlay_root)},
    )

    written = write_phase1_distribution_scenarios(
        fp_home=fp_home,
        backend="fp-r",
        scenarios_root=scenarios_root,
        artifacts_root=artifacts_root,
    )

    baseline_path = scenarios_root / "baseline-observed.yaml"
    assert baseline_path in written
    baseline_text = baseline_path.read_text(encoding="utf-8")
    assert "backend: fp-r" in baseline_text
    assert f"artifacts_root: {artifacts_root}" in baseline_text
    assert "exogenous_equation_target_policy" not in baseline_text

    ui_relief_text = (scenarios_root / "ui-relief.yaml").read_text(encoding="utf-8")
    assert "exogenous_equation_target_policy: retain_reduced_eq_only" in ui_relief_text
    assert "UIFAC:" in ui_relief_text
    assert "method: SAMEVALUE" in ui_relief_text
    assert "CREATE UIFAC=1;" not in ui_relief_text

    transfer_composite_text = (scenarios_root / "transfer-composite-medium.yaml").read_text(encoding="utf-8")
    assert "exogenous_equation_target_policy: retain_reduced_eq_only" in transfer_composite_text
    assert "SNAPDELTAQ:" in transfer_composite_text
    assert "SSFAC:" in transfer_composite_text
    assert "TFEDSHR:" in transfer_composite_text
    assert "TSLSHR:" in transfer_composite_text
    assert "CREATE SNAPDELTAQ=0;" not in transfer_composite_text


def test_write_phase1_distribution_scenarios_all_fpr_additions_can_override_local_policy(
    tmp_path, monkeypatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    fp_home = tmp_path / "FM"
    fp_home.mkdir(parents=True, exist_ok=True)
    scenarios_root = paths.runtime_distribution_root / "scenarios-fpr-override"
    artifacts_root = paths.runtime_distribution_root / "artifacts-fpr-override"

    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)
    monkeypatch.setattr("fp_ineq.phase1_distribution_block.locate_fp_home", lambda path: Path(path))
    monkeypatch.setattr(
        "fp_ineq.phase1_distribution_block.build_phase1_distribution_overlay",
        lambda **_: {"overlay_root": str(paths.runtime_distribution_overlay_root)},
    )

    written = write_phase1_distribution_scenarios(
        fp_home=fp_home,
        backend="fp-r",
        scenarios_root=scenarios_root,
        artifacts_root=artifacts_root,
        variant_ids=("ui-relief", "transfer-composite-medium"),
        scenario_fpr_additions={"__all__": {"exogenous_equation_target_policy": "exclude_from_solve"}},
    )

    assert len(written) == 2
    ui_relief_text = (scenarios_root / "ui-relief.yaml").read_text(encoding="utf-8")
    transfer_composite_text = (scenarios_root / "transfer-composite-medium.yaml").read_text(encoding="utf-8")
    assert "exogenous_equation_target_policy: exclude_from_solve" in ui_relief_text
    assert "exogenous_equation_target_policy: exclude_from_solve" in transfer_composite_text
    assert "retain_reduced_eq_only" not in ui_relief_text
    assert "retain_reduced_eq_only" not in transfer_composite_text


def test_setupsolve_compose_post_patches_normalizes_statement() -> None:
    patches = _setupsolve_compose_post_patches(
        "SETUPSOLVE RHORESIDAR1=YES RHORESIDSOURCESUFFIX=_OBS TARGETLAGSUFFIX=_OBS"
    )

    assert len(patches) == 1
    assert patches[0]["search"] == "SETUPEST ALT2SLS\n@SETUPEST DIVIDET;"
    assert (
        patches[0]["replace"]
        == "SETUPEST ALT2SLS\n"
        "SETUPSOLVE RHORESIDAR1=YES RHORESIDSOURCESUFFIX=_OBS TARGETLAGSUFFIX=_OBS;\n"
        "@SETUPEST DIVIDET;"
    )


def test_run_phase1_distribution_block_injects_setupsolve_compose_patch(tmp_path, monkeypatch) -> None:
    paths = _repo_paths_for_test(tmp_path)
    fp_home = tmp_path / "FM"
    fp_home.mkdir(parents=True, exist_ok=True)
    scenario_path = paths.runtime_distribution_scenarios_root / "ui-relief.yaml"
    captured: dict[str, object] = {}

    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)
    monkeypatch.setattr("fp_ineq.phase1_distribution_block.locate_fp_home", lambda path: Path(path))
    monkeypatch.setattr("fp_ineq.phase1_distribution_block.ensure_fp_wraptr_importable", lambda: None)
    monkeypatch.setattr(
        "fp_ineq.phase1_distribution_block._load_distribution_coefficient_report",
        lambda: {"equations": {}},
    )

    def fake_write_phase1_distribution_scenarios(**kwargs):
        captured["compose_post_patches"] = kwargs["compose_post_patches"]
        scenario_path.parent.mkdir(parents=True, exist_ok=True)
        scenario_path.write_text("name: ui-relief\n", encoding="utf-8")
        return [scenario_path]

    monkeypatch.setattr(
        "fp_ineq.phase1_distribution_block.write_phase1_distribution_scenarios",
        fake_write_phase1_distribution_scenarios,
    )

    fake_runner_module = types.SimpleNamespace()

    def fake_load_scenario_config(path):
        return SimpleNamespace(name=Path(path).stem, description="ui relief", backend="fp-r")

    def fake_run_scenario(*, config, output_dir):
        work_dir = Path(output_dir) / config.name
        work_dir.mkdir(parents=True, exist_ok=True)
        (work_dir / "fmout.txt").write_text("", encoding="utf-8")
        (work_dir / "fprint.txt").write_text("", encoding="utf-8")
        (work_dir / "run.yml").write_text("", encoding="utf-8")
        return SimpleNamespace(
            success=True,
            output_dir=work_dir,
            parse_payload={"loadformat_path": None},
            parsed_output=None,
            run_result=SimpleNamespace(return_code=0, stdout="ok stdout", stderr="ok stderr"),
            backend_diagnostics={},
            metadata={},
        )

    fake_runner_module.run_scenario = fake_run_scenario
    fake_runner_module.load_scenario_config = fake_load_scenario_config
    fake_runner_module.parse_fp_output = lambda path: None
    monkeypatch.setitem(sys.modules, "fp_wraptr.scenarios.runner", fake_runner_module)
    monkeypatch.setitem(sys.modules, "fp_wraptr.scenarios", types.SimpleNamespace(runner=fake_runner_module))

    payload = run_phase1_distribution_block(
        fp_home=fp_home,
        backend="fp-r",
        variant_ids=("ui-relief",),
        fpr_setupsolve_statement="SETUPSOLVE RHORESIDAR1=YES RHORESIDSOURCESUFFIX=_OBS TARGETLAGSUFFIX=_OBS",
    )

    assert payload["backend"] == "fp-r"
    assert captured["compose_post_patches"] == [
        {
            "search": "SETUPEST ALT2SLS\n@SETUPEST DIVIDET;",
            "replace": "SETUPEST ALT2SLS\nSETUPSOLVE RHORESIDAR1=YES RHORESIDSOURCESUFFIX=_OBS TARGETLAGSUFFIX=_OBS;\n@SETUPEST DIVIDET;",
        }
    ]
    persisted = json.loads(Path(payload["report_path"]).read_text(encoding="utf-8"))
    assert persisted["scenarios"]["ui-relief"]["run_result_stdout_tail"] == "ok stdout"
    assert persisted["scenarios"]["ui-relief"]["run_result_stderr_tail"] == "ok stderr"


def test_write_phase1_distribution_scenarios_defaults_to_fp_r(
    tmp_path, monkeypatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    fp_home = tmp_path / "FM"
    fp_home.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr("fp_ineq.phase1_distribution_block.repo_paths", lambda: paths)
    monkeypatch.setattr("fp_ineq.phase1_distribution_block.locate_fp_home", lambda path: Path(path))
    monkeypatch.setattr(
        "fp_ineq.phase1_distribution_block.build_phase1_distribution_overlay",
        lambda **_: {"overlay_root": str(paths.runtime_distribution_overlay_root)},
    )

    written = write_phase1_distribution_scenarios(fp_home=fp_home)

    baseline_path = paths.runtime_distribution_scenarios_root / "baseline-observed.yaml"
    assert baseline_path in written
    baseline_text = baseline_path.read_text(encoding="utf-8")
    assert f"backend: {_DISTRIBUTION_DEFAULT_BACKEND}" in baseline_text


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


def test_merge_runtime_text_files_inserts_appends_before_terminal_return() -> None:
    payload = _merge_runtime_text_files(
        {"idp1blk.txt": "GENR A=1;\nRETURN;\n"},
        appends={"idp1blk.txt": "GENR B=2;\n"},
    )

    assert payload["idp1blk.txt"] == "GENR A=1;\nGENR B=2;\nRETURN;\n"


def test_apply_runtime_text_post_patches_supports_fp_home_files(tmp_path: Path) -> None:
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    (fp_home / "fmexog.txt").write_text("A SAMEVALUE\n1\nLUB SAMEVALUE\n2.197224577\nRETURN;\n", encoding="utf-8")

    payload = _apply_runtime_text_post_patches(
        {"idp1blk.txt": "RETURN;\n"},
        fp_home=fp_home,
        post_patches={
            "fmexog.txt": [
                {
                    "search": "LUB SAMEVALUE\n2.197224577",
                    "replace": "@ LUB SAMEVALUE\n@ 2.197224577",
                }
            ]
        },
    )

    assert payload["fmexog.txt"] == "A SAMEVALUE\n1\n@ LUB SAMEVALUE\n@ 2.197224577\nRETURN;\n"


def test_safe_distribution_decomposition_returns_error_payload_for_nonpositive_rydpc() -> None:
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
        "equations": {},
    }
    results = {
        "baseline-observed": {
            "UR": 0.05,
            "TRLOWZ": 1.10,
            "UB": 100.0,
            "TRGH": 200.0,
            "TRSH": 50.0,
            "RYDPC": -1.0,
        },
        "ui-relief": {
            "UR": 0.045,
            "TRLOWZ": 1.25,
            "UB": 112.0,
            "TRGH": 210.0,
            "TRSH": 48.0,
            "RYDPC": -2.0,
        },
    }

    payload = _safe_distribution_decomposition(results, coefficient_report)

    assert payload["scenarios"] == {}
    assert payload["error"] == "Cannot decompose IMEDRINC with nonpositive RYDPC"
    assert payload["metadata"]["error"] == payload["error"]


def test_transfer_driver_bridges_reconstruct_intg_and_jf() -> None:
    baseline_row = {"AAG": 100.0, "INTGZ": 0.02, "INTG": 2.0, "LJF1": 0.01, "JF": 101.00501670841679}
    scenario_row = {"AAG": 90.0, "INTGZ": 0.018, "INTG": 1.62, "LJF1": 0.009, "JF": 100.60134999085463}
    baseline_lag_row = {"JF": 100.0}
    scenario_lag_row = {"JF": 99.7}

    intg_identity = _transfer_intg_driver_identity(scenario_row)
    intg_breakdown = _transfer_intg_driver_delta_breakdown(baseline_row, scenario_row)
    jf_identity = _transfer_jf_driver_identity(scenario_row, scenario_lag_row)
    jf_breakdown = _transfer_jf_driver_delta_breakdown(
        baseline_row,
        scenario_row,
        baseline_lag_row,
        scenario_lag_row,
    )

    assert intg_identity["reconstructed_intg"] == pytest.approx(1.62)
    assert intg_identity["identity_residual"] == pytest.approx(0.0)
    assert intg_breakdown["reconstructed_delta"] == pytest.approx(1.62 - 2.0)
    assert jf_identity["reconstructed_jf"] == pytest.approx(100.60134999085463)
    assert jf_identity["identity_residual"] == pytest.approx(0.0)
    assert jf_breakdown["reconstructed_delta"] == pytest.approx(100.60134999085463 - 101.00501670841679)


def test_transfer_ly1_and_ag_driver_bridges_reconstruct_formulas() -> None:
    baseline_row = {"Y": 102.0, "LY1": np.log(102.0 / 100.0), "AG": -48.8, "SG": 2.0, "MG": 1.0, "CUR": 0.2, "BR": 0.3, "BO": 0.1}
    scenario_row = {"Y": 101.0, "LY1": np.log(101.0 / 100.5), "AG": -45.8, "SG": 4.0, "MG": 1.0, "CUR": 0.15, "BR": 0.35, "BO": 0.1}
    baseline_lag_row = {"Y": 100.0, "AG": -51.0, "MG": 1.0, "CUR": 0.1, "BR": 0.2, "BO": 0.1}
    scenario_lag_row = {"Y": 100.5, "AG": -50.0, "MG": 1.0, "CUR": 0.1, "BR": 0.2, "BO": 0.1}

    ly1_identity = _transfer_ly1_driver_identity(scenario_row, scenario_lag_row)
    ly1_breakdown = _transfer_ly1_driver_delta_breakdown(
        baseline_row,
        scenario_row,
        baseline_lag_row,
        scenario_lag_row,
    )
    ag_identity = _transfer_ag_driver_identity(scenario_row, scenario_lag_row)
    ag_breakdown = _transfer_ag_driver_delta_breakdown(
        baseline_row,
        scenario_row,
        baseline_lag_row,
        scenario_lag_row,
    )

    assert ly1_identity["reconstructed_ly1"] == pytest.approx(np.log(101.0 / 100.5))
    assert ly1_identity["identity_residual"] == pytest.approx(0.0)
    assert ly1_breakdown["reconstructed_delta"] == pytest.approx(np.log(101.0 / 100.5) - np.log(102.0 / 100.0))
    assert ag_identity["reconstructed_ag"] == pytest.approx(-45.8)
    assert ag_identity["identity_residual"] == pytest.approx(0.0)
    assert ag_breakdown["reconstructed_delta"] == pytest.approx((-45.8) - (-48.8))


def test_fp_r_series_levels_backfill_missing_loadformat_tracks(tmp_path) -> None:
    series_path = tmp_path / "fp_r_series.csv"
    series_path.write_text(
        "\n".join(
            [
                "period,UB,TRGH,TRSH,POP,PH,TRLOWZ,RYDPC,GDPR,YD,IPOVALL,IPOVCH,IGINIHH,IMEDRINC,UR",
                "2026.1,10,20,30,2,1,,,40,39,0.1,0.2,0.3,50,0.05",
                "2029.4,11,21,31,2,1,,41,,40,0.11,0.21,0.31,51,0.04",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    first_levels, last_levels = _read_fp_r_series_levels(
        series_path,
        ["TRLOWZ", "RYDPC", "IPOVALL", "IPOVCH", "IGINIHH", "IMEDRINC", "UR", "UB"],
    )

    supplemented_first = _supplement_missing_levels(
        {"TRLOWZ": None, "RYDPC": None, "IPOVALL": None, "UR": 0.05, "UB": 10.0},
        first_levels,
    )
    supplemented_last = _supplement_missing_levels(
        {"TRLOWZ": None, "RYDPC": None, "IPOVALL": None, "UR": 0.04, "UB": 11.0},
        last_levels,
    )

    assert supplemented_first["TRLOWZ"] == pytest.approx((10.0 + 20.0 + 30.0) / (2.0 * 1.0))
    assert supplemented_first["RYDPC"] == pytest.approx(39.0 / (2.0 * 1.0))
    assert supplemented_first["IPOVALL"] == pytest.approx(0.1)
    assert supplemented_first["UR"] == pytest.approx(0.05)
    assert supplemented_last["TRLOWZ"] == pytest.approx((11.0 + 21.0 + 31.0) / (2.0 * 1.0))
    assert supplemented_last["RYDPC"] == pytest.approx(41.0)
    assert supplemented_last["IPOVALL"] == pytest.approx(0.11)
    assert supplemented_last["UB"] == pytest.approx(11.0)


def test_extract_levels_from_loadformat_replaces_negative_99_sentinels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loadformat_path = tmp_path / "LOADFORMAT.DAT"
    loadformat_path.write_text("placeholder\n", encoding="utf-8")

    fake_loadformat = types.ModuleType("fp_wraptr.io.loadformat")

    def read_loadformat(_path: Path) -> tuple[list[str], dict[str, list[float]]]:
        return ["2026.1", "2026.2"], {
            "TRLOWZ": [-99.0, 0.41],
            "RYDPC": [-99.0, -99.0],
            "YD": [60.0, 66.0],
            "POP": [100.0, 110.0],
            "PH": [0.2, 0.2],
            "UB": [1.0, 2.0],
            "TRGH": [2.0, 3.0],
            "TRSH": [3.0, 4.0],
        }

    def add_derived_series(series: dict[str, list[float]]) -> dict[str, list[float]]:
        return series

    fake_loadformat.read_loadformat = read_loadformat
    fake_loadformat.add_derived_series = add_derived_series

    monkeypatch.setattr("fp_ineq.phase1_ui.ensure_fp_wraptr_importable", lambda: None)
    monkeypatch.setitem(sys.modules, "fp_wraptr", types.ModuleType("fp_wraptr"))
    monkeypatch.setitem(sys.modules, "fp_wraptr.io", types.ModuleType("fp_wraptr.io"))
    monkeypatch.setitem(sys.modules, "fp_wraptr.io.loadformat", fake_loadformat)

    first_levels, last_levels = _extract_levels_from_loadformat(loadformat_path, ["TRLOWZ", "RYDPC"])

    assert first_levels["TRLOWZ"] == pytest.approx(0.3)
    assert first_levels["RYDPC"] == pytest.approx(3.0)
    assert last_levels["TRLOWZ"] == pytest.approx(0.41)
    assert last_levels["RYDPC"] == pytest.approx(3.0)


def test_write_phase1_distribution_scenarios_applies_intervention_overlay_and_variant_merges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fp_ineq import phase1_distribution_block as block

    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr(block, "repo_paths", lambda: paths)
    monkeypatch.setattr(block, "locate_fp_home", lambda fp_home: Path(fp_home))
    monkeypatch.setattr(block, "ensure_fp_wraptr_importable", lambda: None)

    fake_config_module = types.ModuleType("fp_wraptr.scenarios.config")

    class FakeScenarioConfig:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def to_yaml(self, path: Path) -> Path:
            def _normalize(value):
                if isinstance(value, Path):
                    return str(value)
                if isinstance(value, dict):
                    return {key: _normalize(item) for key, item in value.items()}
                if isinstance(value, (list, tuple)):
                    return [_normalize(item) for item in value]
                return value

            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(yaml.safe_dump(_normalize(self.kwargs), sort_keys=False), encoding="utf-8")
            return path

    fake_config_module.ScenarioConfig = FakeScenarioConfig
    monkeypatch.setitem(sys.modules, "fp_wraptr", types.ModuleType("fp_wraptr"))
    monkeypatch.setitem(sys.modules, "fp_wraptr.scenarios", types.ModuleType("fp_wraptr.scenarios"))
    monkeypatch.setitem(sys.modules, "fp_wraptr.scenarios.config", fake_config_module)

    fp_home = tmp_path / "FM"
    fp_home.mkdir()

    overlay_calls: dict[str, object] = {}

    def fake_build_phase1_distribution_overlay(**kwargs):
        overlay_root = Path(kwargs["overlay_root"])
        overlay_root.mkdir(parents=True, exist_ok=True)
        (overlay_root / "fminput.txt").write_text("INPUT FILE=idp1blk.txt;\n", encoding="utf-8")
        overlay_calls.update(kwargs)
        return {"overlay_root": str(overlay_root)}

    monkeypatch.setattr(block, "build_phase1_distribution_overlay", fake_build_phase1_distribution_overlay)

    written = write_phase1_distribution_scenarios(
        fp_home=fp_home,
        backend="fp-r",
        scenarios_root=paths.runtime_distribution_scenarios_root,
        artifacts_root=paths.runtime_distribution_artifacts_root,
        variant_ids=("ui-relief",),
        overlay_root=paths.runtime_distribution_root / "overlay-itv",
        forecast_end="2026.1",
        experimental_patch_ids=("probe-a",),
        runtime_text_appends={"idp1blk.txt": "GENR LUB=LUB+0.1*(UIFAC-1);\n"},
        runtime_text_post_patches={"fmexog.txt": [{"search": "LUB SAMEVALUE", "replace": "@ LUB SAMEVALUE"}]},
        scenario_override_additions={"ui-relief": {"UIFAC": {"method": "SAMEVALUE", "value": 1.2}}},
        scenario_fpr_additions={"ui-relief": {"solver_policy": "active_set_v1"}},
        scenario_extra_metadata={"ui-relief": {"intervention_id": "ui-lub-probe"}},
    )

    assert len(written) == 1
    payload = yaml.safe_load(written[0].read_text(encoding="utf-8"))
    assert overlay_calls["experimental_patch_ids"] == ("probe-a",)
    assert overlay_calls["runtime_text_post_patches"]["fmexog.txt"][0]["search"] == "LUB SAMEVALUE"
    assert payload["input_overlay_dir"] == str((paths.runtime_distribution_root / "overlay-itv").resolve())
    assert payload["forecast_end"] == "2026.1"
    assert payload["overrides"]["UIFAC"]["value"] == pytest.approx(1.2)
    assert payload["fpr"]["solver_policy"] == "active_set_v1"
    assert payload["extra"]["intervention_id"] == "ui-lub-probe"


def test_compare_phase1_distribution_reports_uses_existing_run_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fp_ineq import phase1_distribution_block as block

    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr(block, "repo_paths", lambda: paths)
    monkeypatch.setattr(
        block,
        "_distribution_identity_checks_for_loadformat",
        lambda _path, _spec, **_kwargs: {"ub_scaled": {"max_abs_residual": 0.0, "terminal_abs_residual": 0.0}},
    )

    left_report = tmp_path / "left.json"
    right_report = tmp_path / "right.json"
    left_report.write_text(
        json.dumps(
            {
                "scenarios": {
                    "ui-relief": {
                        "loadformat_path": str(tmp_path / "left-load.dat"),
                        "success": True,
                        "first_levels": {"UB": 10.0, "TRLOWZ": 0.2},
                        "last_levels": {"UB": 12.0, "TRLOWZ": 0.25},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    right_report.write_text(
        json.dumps(
            {
                "scenarios": {
                    "ui-relief": {
                        "loadformat_path": str(tmp_path / "right-load.dat"),
                        "success": True,
                        "first_levels": {"UB": 20.0, "TRLOWZ": 0.4},
                        "last_levels": {"UB": 22.0, "TRLOWZ": 0.45},
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    payload = compare_phase1_distribution_reports(
        left_report_path=left_report,
        right_report_path=right_report,
        left_backend="fp-r",
        right_backend="fpexe",
        variant_ids=("ui-relief",),
        variables=("UB", "TRLOWZ"),
        report_path=tmp_path / "compare.json",
    )

    report = json.loads(Path(payload["report_path"]).read_text(encoding="utf-8"))
    row = report["comparisons"][0]
    assert row["first_levels"]["UB"]["abs_diff"] == pytest.approx(10.0)
    assert row["last_levels"]["TRLOWZ"]["abs_diff"] == pytest.approx(0.2)
    assert report["summary"]["variant_count"] == 1


def test_compare_phase1_distribution_reports_prefers_fp_r_series_levels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fp_ineq import phase1_distribution_block as block

    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr(block, "repo_paths", lambda: paths)
    monkeypatch.setattr(
        block,
        "_distribution_identity_checks_for_loadformat",
        lambda _path, _spec, **_kwargs: {"ub_scaled": {"max_abs_residual": 0.0, "terminal_abs_residual": 0.0}},
    )

    left_load = tmp_path / "left-load.dat"
    right_load = tmp_path / "right-load.dat"
    left_series = left_load.with_name("fp_r_series.csv")
    left_series.write_text(
        "\n".join(
            (
                "period,UB,TRLOWZ",
                "2026.1,30.0,0.9",
                "2029.4,32.0,0.95",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    left_report = tmp_path / "left.json"
    right_report = tmp_path / "right.json"
    left_report.write_text(
        json.dumps(
            {
                "scenarios": {
                    "ui-relief": {
                        "loadformat_path": str(left_load),
                        "success": True,
                        "first_levels": {"UB": 10.0, "TRLOWZ": 0.2},
                        "last_levels": {"UB": 12.0, "TRLOWZ": 0.25},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    right_report.write_text(
        json.dumps(
            {
                "scenarios": {
                    "ui-relief": {
                        "loadformat_path": str(right_load),
                        "success": True,
                        "first_levels": {"UB": 20.0, "TRLOWZ": 0.4},
                        "last_levels": {"UB": 22.0, "TRLOWZ": 0.45},
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    payload = compare_phase1_distribution_reports(
        left_report_path=left_report,
        right_report_path=right_report,
        left_backend="fp-r",
        right_backend="fpexe",
        variant_ids=("ui-relief",),
        variables=("UB", "TRLOWZ"),
        report_path=tmp_path / "compare-fpr-series.json",
    )

    report = json.loads(Path(payload["report_path"]).read_text(encoding="utf-8"))
    row = report["comparisons"][0]
    assert row["first_levels"]["UB"]["left"] == pytest.approx(30.0)
    assert row["first_levels"]["UB"]["abs_diff"] == pytest.approx(10.0)
    assert row["last_levels"]["TRLOWZ"]["left"] == pytest.approx(0.95)
    assert row["last_levels"]["TRLOWZ"]["abs_diff"] == pytest.approx(0.5)


def test_compare_phase1_distribution_reports_prefers_loadformat_period_levels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fp_ineq import phase1_distribution_block as block

    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr(block, "repo_paths", lambda: paths)
    monkeypatch.setattr(
        block,
        "_distribution_identity_checks_for_loadformat",
        lambda _path, _spec, **_kwargs: {"ub_scaled": {"max_abs_residual": 0.0, "terminal_abs_residual": 0.0}},
    )

    payloads = {
        str(tmp_path / "left-load.dat"): (
            ["2025.4", "2026.1", "2026.2"],
            {"UB": [10.0, 30.0, 31.0], "TRLOWZ": [0.2, 0.9, 0.95]},
        ),
        str(tmp_path / "right-load.dat"): (
            ["2025.4", "2026.1", "2026.2"],
            {"UB": [20.0, 21.0, 22.0], "TRLOWZ": [0.4, 0.45, 0.5]},
        ),
    }
    (tmp_path / "left-load.dat").write_text("", encoding="utf-8")
    (tmp_path / "right-load.dat").write_text("", encoding="utf-8")
    monkeypatch.setattr(block, "_read_loadformat_payload", lambda path: payloads.get(str(path), ([], {})))

    left_report = tmp_path / "left.json"
    right_report = tmp_path / "right.json"
    left_report.write_text(
        json.dumps(
            {
                "scenarios": {
                    "ui-relief": {
                        "loadformat_path": str(tmp_path / "left-load.dat"),
                        "success": True,
                        "forecast_window_start": "2026.1",
                        "forecast_window_end": "2026.2",
                        "first_levels": {"UB": 10.0, "TRLOWZ": 0.2},
                        "last_levels": {"UB": 12.0, "TRLOWZ": 0.25},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    right_report.write_text(
        json.dumps(
            {
                "scenarios": {
                    "ui-relief": {
                        "loadformat_path": str(tmp_path / "right-load.dat"),
                        "success": True,
                        "forecast_window_start": "2026.1",
                        "forecast_window_end": "2026.2",
                        "first_levels": {"UB": 20.0, "TRLOWZ": 0.4},
                        "last_levels": {"UB": 22.0, "TRLOWZ": 0.45},
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    payload = compare_phase1_distribution_reports(
        left_report_path=left_report,
        right_report_path=right_report,
        left_backend="fpexe",
        right_backend="fpexe",
        variant_ids=("ui-relief",),
        variables=("UB", "TRLOWZ"),
        report_path=tmp_path / "compare-loadformat-periods.json",
    )

    report = json.loads(Path(payload["report_path"]).read_text(encoding="utf-8"))
    row = report["comparisons"][0]
    assert row["first_levels"]["UB"]["left"] == pytest.approx(30.0)
    assert row["first_levels"]["UB"]["right"] == pytest.approx(21.0)
    assert row["first_levels"]["UB"]["abs_diff"] == pytest.approx(9.0)
    assert row["last_levels"]["TRLOWZ"]["left"] == pytest.approx(0.95)
    assert row["last_levels"]["TRLOWZ"]["right"] == pytest.approx(0.5)
    assert row["last_levels"]["TRLOWZ"]["abs_diff"] == pytest.approx(0.45)


def test_run_phase1_distribution_intervention_experiment_orchestrates_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fp_ineq import phase1_distribution_interventions as interventions

    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr(interventions, "repo_paths", lambda: paths)

    spec_path = tmp_path / "ui-lub-probe.yaml"
    spec_path.write_text(
        yaml.safe_dump(
            {
                "id": "ui-lub-probe",
                "description": "probe",
                "runtime_text_appends": {"idp1blk.txt": "GENR LUB=LUB+0.1*(UIFAC-1);\n"},
                "scenario_extra_metadata": {"__all__": {"family": "probe"}},
                "scenario_window": {"forecast_end": "2026.1"},
                "analysis": {
                    "variant_ids": ["ui-relief"],
                    "compare_variables": ["UB", "TRLOWZ"],
                    "first_level_variables": ["UB"],
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    run_calls: list[dict[str, object]] = []

    def fake_run_phase1_distribution_block(**kwargs):
        run_calls.append(kwargs)
        report_path = Path(kwargs["report_path"])
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                {
                    "scenarios": {
                        "ui-relief": {
                            "loadformat_path": str(report_path.with_suffix(".dat")),
                            "success": True,
                            "first_levels": {"UB": 10.0, "TRLOWZ": 0.2},
                            "last_levels": {"UB": 12.0, "TRLOWZ": 0.25},
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        return {"report_path": str(report_path), "passes": True}

    def fake_compare_phase1_distribution_reports(**kwargs):
        report_path = Path(kwargs["report_path"])
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                {
                    "comparisons": [
                        {
                            "variant_id": "ui-relief",
                            "first_levels": {"UB": {"abs_diff": 5.0}},
                            "last_levels": {"UB": {"abs_diff": 6.0}},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        return {"report_path": str(report_path), "variant_count": 1, "max_abs_diff": 6.0}

    def fake_validate_phase1_distribution_identities(**kwargs):
        report_path = tmp_path / "identity.json"
        report_path.write_text(json.dumps({"passes": True}), encoding="utf-8")
        return {"report_path": str(report_path), "passes": True}

    effect_calls: list[dict[str, object]] = []

    def fake_assess_phase1_distribution_intervention_effect(**kwargs):
        effect_calls.append(kwargs)
        report_path = Path(kwargs["report_path"])
        report_path.write_text(
            json.dumps({"summary": {"median_gap_closure_ratio": 0.4}, "rows": []}),
            encoding="utf-8",
        )
        return {
            "report_path": str(report_path),
            "improved_count": 1,
            "worsened_count": 0,
            "median_gap_closure_ratio": 0.4,
        }

    monkeypatch.setattr(interventions, "run_phase1_distribution_block", fake_run_phase1_distribution_block)
    monkeypatch.setattr(interventions, "compare_phase1_distribution_reports", fake_compare_phase1_distribution_reports)
    monkeypatch.setattr(interventions, "validate_phase1_distribution_identities", fake_validate_phase1_distribution_identities)
    monkeypatch.setattr(interventions, "assess_phase1_distribution_intervention_effect", fake_assess_phase1_distribution_intervention_effect)

    payload = run_phase1_distribution_intervention_experiment(
        fp_home=tmp_path / "FM",
        intervention_spec_path=spec_path,
        intervention_backend="fp-r",
        reference_backend="fpexe",
    )

    assert payload["intervention_id"] == "ui-lub-probe"
    assert payload["gap_improved_count"] == 1
    assert payload["intervention_identity_passes"] is True
    assert len(run_calls) == 3
    assert run_calls[0]["forecast_end"] == "2026.1"
    assert run_calls[1]["forecast_end"] == "2026.1"
    assert run_calls[1]["runtime_text_appends"]["idp1blk.txt"].startswith("GENR LUB")
    assert run_calls[1]["runtime_text_post_patches"] == {}
    assert effect_calls[0]["variables"] == ("UB",)


def test_run_phase1_distribution_intervention_experiment_accepts_existing_shared_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fp_ineq import phase1_distribution_interventions as interventions

    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr(interventions, "repo_paths", lambda: paths)

    spec_path = tmp_path / "transfer-probe.yaml"
    spec_path.write_text(
        yaml.safe_dump(
            {
                "id": "transfer-probe",
                "description": "probe",
                "runtime_text_post_patches": {
                    "fminput.txt": [
                        {
                            "search": "LHS INTG=(INTGZ)*AAG*1.0;",
                            "replace": "LHS INTG=(INTGZ)*AAG*0.75;",
                        }
                    ]
                },
                "analysis": {
                    "variant_ids": ["transfer-composite-medium"],
                    "compare_variables": ["INTG", "YD"],
                    "first_level_variables": ["INTG", "YD"],
                },
                "scenario_window": {"forecast_end": "2026.1"},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    control_report = tmp_path / "existing-control.json"
    reference_report = tmp_path / "existing-reference.json"
    for report_path in (control_report, reference_report):
        report_path.write_text(
            json.dumps(
                {
                    "scenarios": {
                        "transfer-composite-medium": {
                            "loadformat_path": str(report_path.with_suffix(".dat")),
                            "success": True,
                            "forecast_window_start": "2026.1",
                            "forecast_window_end": "2026.1",
                        }
                    }
                }
            ),
            encoding="utf-8",
        )

    run_calls: list[dict[str, object]] = []

    def fake_run_phase1_distribution_block(**kwargs):
        run_calls.append(kwargs)
        report_path = Path(kwargs["report_path"])
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                {
                    "scenarios": {
                        "transfer-composite-medium": {
                            "loadformat_path": str(report_path.with_suffix(".dat")),
                            "success": True,
                            "forecast_window_start": "2026.1",
                            "forecast_window_end": "2026.1",
                            "first_levels": {"INTG": 1.0, "YD": 2.0},
                            "last_levels": {"INTG": 1.0, "YD": 2.0},
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        return {"report_path": str(report_path), "passes": True}

    compare_calls: list[dict[str, object]] = []

    def fake_compare_phase1_distribution_reports(**kwargs):
        compare_calls.append(kwargs)
        report_path = Path(kwargs["report_path"])
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                {
                    "comparisons": [
                        {
                            "variant_id": "transfer-composite-medium",
                            "first_levels": {"INTG": {"abs_diff": 1.0}, "YD": {"abs_diff": 2.0}},
                            "last_levels": {"INTG": {"abs_diff": 1.0}, "YD": {"abs_diff": 2.0}},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        return {"report_path": str(report_path), "variant_count": 1, "max_abs_diff": 2.0}

    def fake_validate_phase1_distribution_identities(**kwargs):
        report_path = Path(kwargs["run_report_path"]).with_suffix(".identity.json")
        report_path.write_text(json.dumps({"passes": True}), encoding="utf-8")
        return {"report_path": str(report_path), "passes": True}

    def fake_assess_phase1_distribution_intervention_effect(**kwargs):
        report_path = Path(kwargs["report_path"])
        report_path.write_text(
            json.dumps({"summary": {"median_gap_closure_ratio": 0.2}, "rows": []}),
            encoding="utf-8",
        )
        return {
            "report_path": str(report_path),
            "improved_count": 2,
            "worsened_count": 0,
            "median_gap_closure_ratio": 0.2,
        }

    monkeypatch.setattr(interventions, "run_phase1_distribution_block", fake_run_phase1_distribution_block)
    monkeypatch.setattr(interventions, "compare_phase1_distribution_reports", fake_compare_phase1_distribution_reports)
    monkeypatch.setattr(interventions, "validate_phase1_distribution_identities", fake_validate_phase1_distribution_identities)
    monkeypatch.setattr(interventions, "assess_phase1_distribution_intervention_effect", fake_assess_phase1_distribution_intervention_effect)

    payload = run_phase1_distribution_intervention_experiment(
        fp_home=tmp_path / "FM",
        intervention_spec_path=spec_path,
        intervention_backend="fp-r",
        reference_backend="fpexe",
        control_run_report_path=control_report,
        reference_run_report_path=reference_report,
    )

    assert payload["intervention_id"] == "transfer-probe"
    assert payload["gap_improved_count"] == 2
    assert payload["intervention_identity_passes"] is True
    assert len(run_calls) == 1
    assert run_calls[0]["report_path"].name.endswith("intervention.fp-r.json")
    assert run_calls[0]["runtime_text_post_patches"]["fminput.txt"][0]["search"] == "LHS INTG=(INTGZ)*AAG*1.0;"
    assert len(compare_calls) == 2
    assert Path(compare_calls[0]["left_report_path"]) == control_report
    assert Path(compare_calls[0]["right_report_path"]) == reference_report


def test_load_phase1_distribution_intervention_spec_applies_defaults(tmp_path: Path) -> None:
    spec_path = tmp_path / "probe.yaml"
    spec_path.write_text(yaml.safe_dump({"id": "probe"}), encoding="utf-8")

    payload = load_phase1_distribution_intervention_spec(spec_path)

    assert payload["id"] == "probe"
    assert payload["analysis"]["variant_ids"] == ("baseline-observed", "ui-relief", "transfer-composite-medium")
    assert "UB" in payload["analysis"]["compare_variables"]
    assert payload["scenario_window"]["forecast_end"] == "2029.4"
    assert payload["runtime_text_post_patches"] == {}
    assert payload["ladder"]["coefficients"] == []


def test_load_phase1_distribution_intervention_spec_reads_ladder(tmp_path: Path) -> None:
    spec_path = tmp_path / "probe.yaml"
    spec_path.write_text(
        yaml.safe_dump(
            {
                "id": "probe",
                "ladder": {
                    "target": "lub",
                    "variable": "duifac",
                    "mode": "add",
                    "coefficients": [5, 10],
                },
            }
        ),
        encoding="utf-8",
    )

    payload = load_phase1_distribution_intervention_spec(spec_path)

    assert payload["ladder"]["target"] == "LUB"
    assert payload["ladder"]["variable"] == "DUIFAC"
    assert payload["ladder"]["mode"] == "add"
    assert payload["ladder"]["coefficients"] == [5.0, 10.0]


def test_run_phase1_distribution_intervention_ladder_reuses_control_and_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fp_ineq import phase1_distribution_interventions as interventions

    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr(interventions, "repo_paths", lambda: paths)

    spec_path = tmp_path / "ui-lub-ladder.yaml"
    spec_path.write_text(
        yaml.safe_dump(
            {
                "id": "ui-lub-ladder",
                "description": "ladder",
                "runtime_text_appends": {"idp1blk.txt": "GENR DUIFAC=UIFAC-1;\n"},
                "scenario_window": {"forecast_end": "2026.1"},
                "scenario_fpr_additions": {
                    "__all__": {
                        "equation_term_overrides": [
                            {
                                "target": "LUB",
                                "variable": "DUIFAC",
                                "coefficient": 50.0,
                                "lag": 0,
                                "mode": "add",
                            }
                        ]
                    }
                },
                "analysis": {
                    "variant_ids": ["ui-relief"],
                    "compare_variables": ["UB", "TRLOWZ"],
                    "first_level_variables": ["UB"],
                },
                "ladder": {
                    "target": "LUB",
                    "variable": "DUIFAC",
                    "coefficients": [5.0, 10.0, 20.0],
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    run_calls: list[dict[str, object]] = []

    def fake_run_phase1_distribution_block(**kwargs):
        run_calls.append(kwargs)
        report_path = Path(kwargs["report_path"])
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                {
                    "scenarios": {
                        "ui-relief": {
                            "loadformat_path": str(report_path.with_suffix(".dat")),
                            "success": True,
                            "first_levels": {"UB": 10.0, "TRLOWZ": 0.2},
                            "last_levels": {"UB": 12.0, "TRLOWZ": 0.25},
                            "forecast_window_start": "2026.1",
                            "forecast_window_end": "2026.1",
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        return {"report_path": str(report_path), "passes": True}

    compare_calls: list[dict[str, object]] = []

    def fake_compare_phase1_distribution_reports(**kwargs):
        compare_calls.append(kwargs)
        report_path = Path(kwargs["report_path"])
        report_path.parent.mkdir(parents=True, exist_ok=True)
        coefficient = 0.0
        stem = report_path.stem
        if "5" in stem:
            coefficient = 5.0
        elif "10" in stem:
            coefficient = 10.0
        elif "20" in stem:
            coefficient = 20.0
        abs_diff = 10.0 - coefficient if coefficient else 10.0
        report_path.write_text(
            json.dumps(
                {
                    "comparisons": [
                        {
                            "variant_id": "ui-relief",
                            "first_levels": {"UB": {"abs_diff": abs_diff}},
                            "last_levels": {"UB": {"abs_diff": abs_diff + 1.0}},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        return {"report_path": str(report_path), "variant_count": 1, "max_abs_diff": abs_diff}

    def fake_validate_phase1_distribution_identities(**kwargs):
        report_path = Path(kwargs["run_report_path"]).with_suffix(".identity.json")
        report_path.write_text(json.dumps({"passes": True}), encoding="utf-8")
        return {"report_path": str(report_path), "passes": True}

    def fake_assess_phase1_distribution_intervention_effect(**kwargs):
        report_path = Path(kwargs["report_path"])
        report_path.write_text(
            json.dumps({"summary": {"median_gap_closure_ratio": 0.6}, "rows": []}),
            encoding="utf-8",
        )
        coeff = 0.0
        stem = report_path.stem
        if "5" in stem:
            coeff = 5.0
        elif "10" in stem:
            coeff = 10.0
        elif "20" in stem:
            coeff = 20.0
        return {
            "report_path": str(report_path),
            "improved_count": int(coeff),
            "worsened_count": 0,
            "median_gap_closure_ratio": coeff / 10.0,
        }

    monkeypatch.setattr(interventions, "run_phase1_distribution_block", fake_run_phase1_distribution_block)
    monkeypatch.setattr(interventions, "compare_phase1_distribution_reports", fake_compare_phase1_distribution_reports)
    monkeypatch.setattr(interventions, "validate_phase1_distribution_identities", fake_validate_phase1_distribution_identities)
    monkeypatch.setattr(interventions, "assess_phase1_distribution_intervention_effect", fake_assess_phase1_distribution_intervention_effect)

    payload = run_phase1_distribution_intervention_ladder(
        fp_home=tmp_path / "FM",
        intervention_spec_path=spec_path,
        intervention_backend="fp-r",
        reference_backend="fpexe",
    )

    assert payload["intervention_id"] == "ui-lub-ladder"
    assert payload["row_count"] == 3
    assert payload["best_coefficient"] == pytest.approx(20.0)
    assert len(run_calls) == 5
    assert run_calls[0]["report_path"].name.endswith("control.fp-r.json")
    assert run_calls[1]["report_path"].name.endswith("reference.fpexe.json")
    ladder_coefficients = []
    for call in run_calls[2:]:
        overrides = call["scenario_fpr_additions"]["__all__"]["equation_term_overrides"]
        ladder_coefficients.append(float(overrides[0]["coefficient"]))
        assert call["forecast_end"] == "2026.1"
    assert ladder_coefficients == [5.0, 10.0, 20.0]
    assert len(compare_calls) == 4


def test_run_phase1_distribution_intervention_ladder_accepts_existing_shared_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fp_ineq import phase1_distribution_interventions as interventions

    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr(interventions, "repo_paths", lambda: paths)

    spec_path = tmp_path / "ui-lub-ladder.yaml"
    spec_path.write_text(
        yaml.safe_dump(
            {
                "id": "ui-lub-ladder",
                "analysis": {
                    "variant_ids": ["ui-relief"],
                    "compare_variables": ["UB"],
                    "first_level_variables": ["UB"],
                },
                "ladder": {
                    "target": "LUB",
                    "variable": "DUIFAC",
                    "coefficients": [5.0, 10.0],
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    control_report = tmp_path / "existing-control.json"
    reference_report = tmp_path / "existing-reference.json"
    for report_path in (control_report, reference_report):
        report_path.write_text(
            json.dumps(
                {
                    "scenarios": {
                        "ui-relief": {
                            "loadformat_path": str(report_path.with_suffix(".dat")),
                            "success": True,
                            "forecast_window_start": "2026.1",
                            "forecast_window_end": "2026.1",
                        }
                    }
                }
            ),
            encoding="utf-8",
        )

    run_calls: list[dict[str, object]] = []

    def fake_run_phase1_distribution_block(**kwargs):
        run_calls.append(kwargs)
        report_path = Path(kwargs["report_path"])
        report_path.write_text(
            json.dumps(
                {
                    "scenarios": {
                        "ui-relief": {
                            "loadformat_path": str(report_path.with_suffix(".dat")),
                            "success": True,
                            "forecast_window_start": "2026.1",
                            "forecast_window_end": "2026.1",
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        return {"report_path": str(report_path), "passes": True}

    def fake_compare_phase1_distribution_reports(**kwargs):
        report_path = Path(kwargs["report_path"])
        report_path.write_text(
            json.dumps(
                {"comparisons": [{"variant_id": "ui-relief", "first_levels": {"UB": {"abs_diff": 1.0}}}]}
            ),
            encoding="utf-8",
        )
        return {"report_path": str(report_path), "variant_count": 1, "max_abs_diff": 1.0}

    def fake_validate_phase1_distribution_identities(**kwargs):
        report_path = Path(kwargs["run_report_path"]).with_suffix(".identity.json")
        report_path.write_text(json.dumps({"passes": True}), encoding="utf-8")
        return {"report_path": str(report_path), "passes": True}

    def fake_assess_phase1_distribution_intervention_effect(**kwargs):
        report_path = Path(kwargs["report_path"])
        report_path.write_text(json.dumps({"summary": {}, "rows": []}), encoding="utf-8")
        return {
            "report_path": str(report_path),
            "improved_count": 1,
            "worsened_count": 0,
            "median_gap_closure_ratio": 0.1,
        }

    monkeypatch.setattr(interventions, "run_phase1_distribution_block", fake_run_phase1_distribution_block)
    monkeypatch.setattr(interventions, "compare_phase1_distribution_reports", fake_compare_phase1_distribution_reports)
    monkeypatch.setattr(interventions, "validate_phase1_distribution_identities", fake_validate_phase1_distribution_identities)
    monkeypatch.setattr(interventions, "assess_phase1_distribution_intervention_effect", fake_assess_phase1_distribution_intervention_effect)

    payload = run_phase1_distribution_intervention_ladder(
        fp_home=tmp_path / "FM",
        intervention_spec_path=spec_path,
        intervention_backend="fp-r",
        reference_backend="fpexe",
        control_run_report_path=control_report,
        reference_run_report_path=reference_report,
    )

    assert payload["row_count"] == 2
    assert len(run_calls) == 2


def test_assess_phase1_distribution_intervention_ladder_selection_recommends_by_control_multiple(
    tmp_path: Path,
) -> None:
    summary_path = tmp_path / "zero_history_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "5": {
                    "improved_count": 6,
                    "mean_gap_closure_ratio": 0.02,
                    "median_gap_closure_ratio": 0.01,
                    "optional_ub_unscaled_max_abs_residual": 0.21,
                    "required_identity_max_abs_residual": 1e-9,
                    "required_identity_passes": True,
                    "worsened_count": 0,
                },
                "10": {
                    "improved_count": 6,
                    "mean_gap_closure_ratio": 0.05,
                    "median_gap_closure_ratio": 0.04,
                    "optional_ub_unscaled_max_abs_residual": 0.23,
                    "required_identity_max_abs_residual": 1e-9,
                    "required_identity_passes": True,
                    "worsened_count": 0,
                },
                "20": {
                    "improved_count": 6,
                    "mean_gap_closure_ratio": 0.10,
                    "median_gap_closure_ratio": 0.08,
                    "optional_ub_unscaled_max_abs_residual": 0.27,
                    "required_identity_max_abs_residual": 1e-9,
                    "required_identity_passes": True,
                    "worsened_count": 0,
                },
                "35": {
                    "improved_count": 6,
                    "mean_gap_closure_ratio": 0.18,
                    "median_gap_closure_ratio": 0.17,
                    "optional_ub_unscaled_max_abs_residual": 0.38,
                    "required_identity_max_abs_residual": 1e-9,
                    "required_identity_passes": True,
                    "worsened_count": 0,
                },
            }
        ),
        encoding="utf-8",
    )
    control_identity_path = tmp_path / "validate.control.json"
    control_identity_path.write_text(
        json.dumps(
            {
                "variants": [
                    {
                        "all_identity_checks": {
                            "ub_unscaled": {
                                "max_abs_residual": 0.186,
                            }
                        }
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    payload = assess_phase1_distribution_intervention_ladder_selection(
        zero_history_summary_path=summary_path,
        control_identity_report_path=control_identity_path,
        report_path=tmp_path / "selection.json",
    )

    assert payload["conservative_coefficient"] == pytest.approx(10.0)
    assert payload["balanced_coefficient"] == pytest.approx(20.0)
    assert payload["stretch_coefficient"] == pytest.approx(35.0)
    assert payload["aggressive_coefficient"] == pytest.approx(35.0)


def test_assess_phase1_distribution_intervention_effect_summarizes_gap_closure(tmp_path: Path) -> None:
    control_report = tmp_path / "control.json"
    intervention_report = tmp_path / "intervention.json"
    control_report.write_text(
        json.dumps(
            {
                "comparisons": [
                    {"variant_id": "ui-relief", "first_levels": {"UB": {"abs_diff": 10.0}}},
                ]
            }
        ),
        encoding="utf-8",
    )
    intervention_report.write_text(
        json.dumps(
            {
                "comparisons": [
                    {"variant_id": "ui-relief", "first_levels": {"UB": {"abs_diff": 4.0}}},
                ]
            }
        ),
        encoding="utf-8",
    )

    payload = assess_phase1_distribution_intervention_effect(
        control_compare_report_path=control_report,
        intervention_compare_report_path=intervention_report,
        variant_ids=("ui-relief",),
        variables=("UB",),
        report_path=tmp_path / "effect.json",
    )

    assert payload["improved_count"] == 1
    assert payload["worsened_count"] == 0
    assert payload["median_gap_closure_ratio"] == pytest.approx(0.6)


def test_run_phase1_distribution_family_holdout_resolves_catalog_variants(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fp_ineq import phase1_distribution_interventions as interventions

    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr(interventions, "repo_paths", lambda: paths)

    spec_path = tmp_path / "ui-balanced.yaml"
    spec_path.write_text(
        yaml.safe_dump(
            {
                "id": "ui-balanced",
                "description": "Base UI family intervention.",
                "analysis": {
                    "variant_ids": ["ui-relief"],
                    "compare_variables": ["UB"],
                    "first_level_variables": ["UB"],
                },
                "scenario_fpr_additions": {
                    "ui-relief": {
                        "equation_term_overrides": [
                            {
                                "target": "LUB",
                                "variable": "DUIFAC",
                                "coefficient": 20.0,
                                "lag": 0,
                                "mode": "add",
                            }
                        ]
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    def fake_run_phase1_distribution_intervention_experiment(**kwargs):
        captured.update(kwargs)
        return {
            "report_path": str(tmp_path / "report.json"),
            "intervention_id": "ui-family-holdout",
            "gap_improved_count": 3,
            "gap_worsened_count": 0,
            "median_gap_closure_ratio": 0.05,
            "intervention_identity_passes": True,
        }

    monkeypatch.setattr(
        interventions,
        "run_phase1_distribution_intervention_experiment",
        fake_run_phase1_distribution_intervention_experiment,
    )

    payload = run_phase1_distribution_family_holdout(
        fp_home=tmp_path / "FM",
        intervention_spec_path=spec_path,
        family_ids=("ui",),
        exclude_variant_ids=("ui-relief",),
        report_tag="ui-family-holdout",
    )

    assert payload["holdout_variant_ids"] == ["ui-shock", "ui-small", "ui-large"]
    derived_spec_path = Path(str(payload["derived_spec_path"]))
    derived_payload = yaml.safe_load(derived_spec_path.read_text(encoding="utf-8"))
    assert derived_payload["id"] == "ui-family-holdout"
    assert derived_payload["analysis"]["variant_ids"] == ["ui-shock", "ui-small", "ui-large"]
    assert captured["intervention_spec_path"] == derived_spec_path


def test_run_phase1_distribution_family_holdout_respects_explicit_variant_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fp_ineq import phase1_distribution_interventions as interventions

    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr(interventions, "repo_paths", lambda: paths)

    spec_path = tmp_path / "transfer-balanced.yaml"
    spec_path.write_text(
        yaml.safe_dump(
            {
                "id": "transfer-balanced",
                "analysis": {
                    "variant_ids": ["transfer-composite-medium"],
                    "compare_variables": ["YD"],
                    "first_level_variables": ["YD"],
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    def fake_run_phase1_distribution_intervention_experiment(**kwargs):
        captured.update(kwargs)
        return {
            "report_path": str(tmp_path / "report.json"),
            "intervention_id": "transfer-family-holdout",
            "gap_improved_count": 2,
            "gap_worsened_count": 0,
            "median_gap_closure_ratio": 0.02,
            "intervention_identity_passes": True,
        }

    monkeypatch.setattr(
        interventions,
        "run_phase1_distribution_intervention_experiment",
        fake_run_phase1_distribution_intervention_experiment,
    )

    payload = run_phase1_distribution_family_holdout(
        fp_home=tmp_path / "FM",
        intervention_spec_path=spec_path,
        family_ids=("transfer-composite",),
        holdout_variant_ids=("transfer-composite-small", "transfer-composite-large"),
        report_tag="transfer-family-holdout",
    )

    assert payload["holdout_variant_ids"] == ["transfer-composite-small", "transfer-composite-large"]
    derived_payload = yaml.safe_load(Path(payload["derived_spec_path"]).read_text(encoding="utf-8"))
    assert derived_payload["analysis"]["variant_ids"] == [
        "transfer-composite-small",
        "transfer-composite-large",
    ]
    assert Path(captured["intervention_spec_path"]).name == "derived-holdout-spec.yaml"


def test_assess_phase1_distribution_holdout_directionality_uses_expected_variant_signs(
    tmp_path: Path,
) -> None:
    compare_report = tmp_path / "compare.internal.json"
    compare_report.write_text(
        json.dumps(
            {
                "comparisons": [
                    {
                        "variant_id": "ui-shock",
                        "first_levels": {
                            "IPOVALL": {"left": 0.12, "right": 0.10},
                            "IPOVCH": {"left": 0.18, "right": 0.15},
                            "TRLOWZ": {"left": 0.90, "right": 1.10},
                            "RYDPC": {"left": 48.0, "right": 50.0},
                            "YD": {"left": 96.0, "right": 100.0},
                            "GDPR": {"left": 94.0, "right": 100.0},
                            "UB": {"left": 8.0, "right": 10.0},
                            "UR": {"left": 0.08, "right": 0.06},
                            "PCY": {"left": 2.7, "right": 2.9},
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    payload = assess_phase1_distribution_holdout_directionality(
        compare_report_path=compare_report,
        variant_ids=("ui-shock",),
        report_path=tmp_path / "directionality.json",
    )

    assert payload["core_pass_count"] == 1
    assert payload["core_all_pass"] is True
    assert payload["optional_pass_count"] == 1
    assert payload["optional_all_pass"] is True
    assert payload["pass_count"] == 1
    report = json.loads((tmp_path / "directionality.json").read_text(encoding="utf-8"))
    assert report["summary"]["core_all_pass"] is True
    assert report["summary"]["optional_all_pass"] is True
    assert report["rows"][0]["passes_required"] is True
    assert report["rows"][0]["passes_optional"] is True
    assert report["summary"]["all_pass"] is True
    assert report["rows"][0]["passes_core"] is True


def test_run_phase1_distribution_family_holdout_internal_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fp_ineq import phase1_distribution_interventions as interventions

    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr(interventions, "repo_paths", lambda: paths)

    spec_path = tmp_path / "ui-balanced.yaml"
    spec_path.write_text(
        yaml.safe_dump(
            {
                "id": "ui-balanced",
                "analysis": {
                    "variant_ids": ["ui-relief"],
                    "compare_variables": ["UB", "YD", "GDPR", "IPOVALL", "IPOVCH", "TRLOWZ", "RYDPC", "UR", "PCY"],
                    "first_level_variables": ["UB"],
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    run_calls: list[dict[str, object]] = []

    def fake_run_phase1_distribution_block(**kwargs):
        run_calls.append(kwargs)
        report_path = Path(kwargs["report_path"])
        report_path.write_text(
            json.dumps(
                {
                    "scenarios": {
                        "ui-shock": {
                            "loadformat_path": str(report_path.with_suffix(".dat")),
                            "success": True,
                            "forecast_window_start": "2026.1",
                            "forecast_window_end": "2026.4",
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        return {"report_path": str(report_path), "passes": True}

    compare_calls: list[dict[str, object]] = []

    def fake_compare_phase1_distribution_reports(**kwargs):
        compare_calls.append(kwargs)
        report_path = Path(kwargs["report_path"])
        report_path.write_text(
            json.dumps(
                {
                    "comparisons": [
                        {
                            "variant_id": "ui-shock",
                            "first_levels": {
                                "IPOVALL": {"left": 0.12, "right": 0.10},
                                "IPOVCH": {"left": 0.18, "right": 0.15},
                                "TRLOWZ": {"left": 0.90, "right": 1.10},
                                "RYDPC": {"left": 48.0, "right": 50.0},
                                "YD": {"left": 96.0, "right": 100.0},
                                "GDPR": {"left": 94.0, "right": 100.0},
                                "UB": {"left": 8.0, "right": 10.0},
                                "UR": {"left": 0.08, "right": 0.06},
                                "PCY": {"left": 2.7, "right": 2.9},
                            },
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        return {"report_path": str(report_path), "variant_count": 1, "max_abs_diff": 4.0}

    monkeypatch.setattr(interventions, "run_phase1_distribution_block", fake_run_phase1_distribution_block)
    monkeypatch.setattr(interventions, "compare_phase1_distribution_reports", fake_compare_phase1_distribution_reports)

    payload = run_phase1_distribution_family_holdout(
        fp_home=tmp_path / "FM",
        intervention_spec_path=spec_path,
        family_ids=("ui",),
        holdout_variant_ids=("ui-shock",),
        internal_only=True,
        report_tag="ui-family-internal",
    )

    assert payload["directionality_core_all_pass"] is True
    assert payload["directionality_optional_all_pass"] is True
    assert payload["directionality_all_pass"] is True
    assert len(run_calls) == 2
    assert tuple(compare_calls[0]["variables"]) == (
        "UB",
        "YD",
        "GDPR",
        "IPOVALL",
        "IPOVCH",
        "TRLOWZ",
        "RYDPC",
        "UR",
        "PCY",
    )
    report = json.loads(Path(payload["report_path"]).read_text(encoding="utf-8"))
    assert report["summary"]["directionality_core_all_pass"] is True
    assert report["summary"]["directionality_optional_all_pass"] is True
    assert report["summary"]["directionality_all_pass"] is True


def test_run_phase1_distribution_family_holdout_internal_only_expands_compare_surface_for_transfer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fp_ineq import phase1_distribution_interventions as interventions

    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr(interventions, "repo_paths", lambda: paths)

    spec_path = tmp_path / "transfer-balanced.yaml"
    spec_path.write_text(
        yaml.safe_dump(
            {
                "id": "transfer-balanced",
                "analysis": {
                    "variant_ids": ["transfer-composite-medium"],
                    "compare_variables": ["YD", "GDPR", "RYDPC"],
                    "first_level_variables": ["YD"],
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    def fake_run_phase1_distribution_block(**kwargs):
        report_path = Path(kwargs["report_path"])
        report_path.write_text(
            json.dumps(
                {
                    "scenarios": {
                        "transfer-composite-small": {
                            "loadformat_path": str(report_path.with_suffix(".dat")),
                            "success": True,
                            "forecast_window_start": "2026.1",
                            "forecast_window_end": "2026.4",
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        return {"report_path": str(report_path), "passes": True}

    compare_calls: list[dict[str, object]] = []

    def fake_compare_phase1_distribution_reports(**kwargs):
        compare_calls.append(kwargs)
        report_path = Path(kwargs["report_path"])
        report_path.write_text(
            json.dumps(
                {
                    "comparisons": [
                        {
                            "variant_id": "transfer-composite-small",
                            "first_levels": {
                                "YD": {"left": 101.0, "right": 100.0},
                                "GDPR": {"left": 101.0, "right": 100.0},
                                "RYDPC": {"left": 10.1, "right": 10.0},
                                "TRGH": {"left": 11.0, "right": 10.0},
                                "TRSH": {"left": 6.0, "right": 5.0},
                                "TRLOWZ": {"left": 1.1, "right": 1.0},
                                "IPOVALL": {"left": 0.09, "right": 0.10},
                                "IPOVCH": {"left": 0.14, "right": 0.15},
                                "UB": {"left": 10.5, "right": 10.0},
                                "UR": {"left": 0.049, "right": 0.05},
                                "PCY": {"left": 3.1, "right": 3.0},
                            },
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        return {"report_path": str(report_path), "variant_count": 1, "max_abs_diff": 1.0}

    monkeypatch.setattr(interventions, "run_phase1_distribution_block", fake_run_phase1_distribution_block)
    monkeypatch.setattr(interventions, "compare_phase1_distribution_reports", fake_compare_phase1_distribution_reports)

    payload = run_phase1_distribution_family_holdout(
        fp_home=tmp_path / "FM",
        intervention_spec_path=spec_path,
        family_ids=("transfer-composite",),
        holdout_variant_ids=("transfer-composite-small",),
        internal_only=True,
        report_tag="transfer-family-internal",
    )

    assert payload["directionality_core_all_pass"] is True
    variables = tuple(compare_calls[0]["variables"])
    for required in ("TRGH", "TRSH", "TRLOWZ", "IPOVALL", "IPOVCH", "UB", "YD", "GDPR", "RYDPC", "UR", "PCY"):
        assert required in variables


def test_assess_phase1_distribution_family_generalization_classifies_ui_vs_transfer(
    tmp_path: Path,
) -> None:
    ui_directionality = tmp_path / "ui-directionality.json"
    ui_directionality.write_text(
        json.dumps(
            {
                "rows": [{"variant_id": "ui-shock"}],
                "summary": {
                    "variant_count": 1,
                    "core_all_pass": True,
                    "optional_all_pass": False,
                    "all_pass": False,
                },
            }
        ),
        encoding="utf-8",
    )
    ui_holdout_a = tmp_path / "ui-holdout-a.json"
    ui_holdout_a.write_text(
        json.dumps(
            {
                "holdout_variant_ids": ["ui-shock"],
                "directionality_report_path": str(ui_directionality),
                "summary": {
                    "directionality_core_all_pass": True,
                    "directionality_optional_all_pass": False,
                    "directionality_all_pass": False,
                },
            }
        ),
        encoding="utf-8",
    )
    ui_holdout_b = tmp_path / "ui-holdout-b.json"
    ui_holdout_b.write_text(
        json.dumps(
            {
                "holdout_variant_ids": ["ui-large"],
                "directionality_report_path": str(ui_directionality),
                "summary": {
                    "directionality_core_all_pass": True,
                    "directionality_optional_all_pass": False,
                    "directionality_all_pass": False,
                },
            }
        ),
        encoding="utf-8",
    )

    ui_payload = assess_phase1_distribution_family_generalization(
        family_id="ui",
        holdout_report_paths=(ui_holdout_a, ui_holdout_b),
        report_path=tmp_path / "ui-family.json",
    )
    assert ui_payload["status"] == "core-generalized-with-optional-misses"
    assert ui_payload["core_all_pass"] is True
    assert ui_payload["optional_all_pass"] is False
    assert ui_payload["all_pass"] is False

    transfer_directionality = tmp_path / "transfer-directionality.json"
    transfer_directionality.write_text(
        json.dumps(
            {
                "rows": [{"variant_id": "transfer-composite-small"}],
                "summary": {
                    "variant_count": 1,
                    "core_all_pass": True,
                    "optional_all_pass": True,
                    "all_pass": True,
                },
            }
        ),
        encoding="utf-8",
    )
    transfer_holdout_a = tmp_path / "transfer-holdout-a.json"
    transfer_holdout_a.write_text(
        json.dumps(
            {
                "holdout_variant_ids": ["transfer-composite-small"],
                "directionality_report_path": str(transfer_directionality),
                "summary": {
                    "directionality_core_all_pass": True,
                    "directionality_optional_all_pass": True,
                    "directionality_all_pass": True,
                },
            }
        ),
        encoding="utf-8",
    )
    transfer_holdout_b = tmp_path / "transfer-holdout-b.json"
    transfer_holdout_b.write_text(
        json.dumps(
            {
                "holdout_variant_ids": ["transfer-composite-large"],
                "directionality_report_path": str(transfer_directionality),
                "summary": {
                    "directionality_core_all_pass": True,
                    "directionality_optional_all_pass": True,
                    "directionality_all_pass": True,
                },
            }
        ),
        encoding="utf-8",
    )

    transfer_payload = assess_phase1_distribution_family_generalization(
        family_id="transfer-composite",
        holdout_report_paths=(transfer_holdout_a, transfer_holdout_b),
        report_path=tmp_path / "transfer-family.json",
    )
    assert transfer_payload["status"] == "generalized"
    assert transfer_payload["core_all_pass"] is True
    assert transfer_payload["optional_all_pass"] is True
    assert transfer_payload["all_pass"] is True


def test_assess_phase1_distribution_generalization_readiness_summarizes_family_reports(
    tmp_path: Path,
) -> None:
    ui_report = tmp_path / "ui-family.json"
    ui_report.write_text(
        json.dumps(
            {
                "summary": {
                    "status": "core-generalized-with-optional-misses",
                    "core_all_pass": True,
                    "optional_all_pass": False,
                    "all_pass": False,
                }
            }
        ),
        encoding="utf-8",
    )
    transfer_report = tmp_path / "transfer-family.json"
    transfer_report.write_text(
        json.dumps(
            {
                "summary": {
                    "status": "generalized",
                    "core_all_pass": True,
                    "optional_all_pass": True,
                    "all_pass": True,
                }
            }
        ),
        encoding="utf-8",
    )

    payload = assess_phase1_distribution_generalization_readiness(
        ui_family_report_path=ui_report,
        transfer_family_report_path=transfer_report,
        report_path=tmp_path / "generalization-readiness.json",
    )

    assert payload["status"] == "generalized-on-core-surface"
    assert payload["ui_status"] == "core-generalized-with-optional-misses"
    assert payload["transfer_status"] == "generalized"
    assert payload["ui_macro_policy"] == "optional"
    assert payload["scenario_scope"] == "canonical-public-scenario-surface"
    assert payload["final_parity_admissible"] is True
    report = json.loads((tmp_path / "generalization-readiness.json").read_text(encoding="utf-8"))
    assert report["overall"]["status"] == "generalized-on-core-surface"
    assert report["overall"]["ui_macro_policy"] == "optional"
    assert report["overall"]["scenario_scope"] == "canonical-public-scenario-surface"
    assert report["overall"]["final_parity_admissible"] is True
    assert any("optional PCY/UR macro side lane" in blocker for blocker in report["overall"]["blockers"])

    strict_payload = assess_phase1_distribution_generalization_readiness(
        ui_family_report_path=ui_report,
        transfer_family_report_path=transfer_report,
        ui_macro_policy="required",
        report_path=tmp_path / "generalization-readiness-strict.json",
    )
    assert strict_payload["status"] == "core-generalized-with-ui-macro-caveat"
    assert strict_payload["ui_macro_policy"] == "required"
    strict_report = json.loads((tmp_path / "generalization-readiness-strict.json").read_text(encoding="utf-8"))
    assert strict_report["overall"]["status"] == "core-generalized-with-ui-macro-caveat"
    assert any("required PCY/UR macro side lane" in blocker for blocker in strict_report["overall"]["blockers"])


def test_assess_phase1_distribution_package_readiness_classifies_combined_candidate(
    tmp_path: Path,
) -> None:
    experiment_report = tmp_path / "run_phase1_distribution_intervention_experiment.json"
    effect_report = tmp_path / "assess_phase1_distribution_intervention_effect.json"
    control_run_report = tmp_path / "control-run.json"
    intervention_run_report = tmp_path / "intervention-run.json"
    reference_run_report = tmp_path / "reference-run.json"
    ui_family_report = tmp_path / "ui-family.json"
    transfer_family_report = tmp_path / "transfer-family.json"
    restricted_run_payload = {
        "scenarios": {
            "ui-relief": {"backend": "fp-r"},
            "transfer-composite-medium": {"backend": "fp-r"},
        }
    }
    control_run_report.write_text(json.dumps(restricted_run_payload), encoding="utf-8")
    intervention_run_report.write_text(json.dumps(restricted_run_payload), encoding="utf-8")
    reference_run_report.write_text(json.dumps(restricted_run_payload), encoding="utf-8")
    ui_family_report.write_text(
        json.dumps(
            {
                "summary": {
                    "status": "core-generalized-with-optional-misses",
                    "core_all_pass": True,
                    "optional_all_pass": False,
                    "all_pass": False,
                }
            }
        ),
        encoding="utf-8",
    )
    transfer_family_report.write_text(
        json.dumps(
            {
                "summary": {
                    "status": "generalized",
                    "core_all_pass": True,
                    "optional_all_pass": True,
                    "all_pass": True,
                }
            }
        ),
        encoding="utf-8",
    )
    experiment_report.write_text(
        json.dumps(
            {
                "intervention_effect_report_path": str(effect_report),
                "control_run_report_path": str(control_run_report),
                "intervention_run_report_path": str(intervention_run_report),
                "reference_run_report_path": str(reference_run_report),
                "summary": {
                    "control_passes": False,
                    "intervention_passes": True,
                    "reference_passes": False,
                    "intervention_identity_passes": True,
                },
            }
        ),
        encoding="utf-8",
    )
    effect_report.write_text(
        json.dumps(
            {
                "rows": [
                    {"variant_id": "ui-relief", "variable": "UB", "gap_closure_ratio": 0.083, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "ui-relief", "variable": "YD", "gap_closure_ratio": 0.087, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "INTG", "gap_closure_ratio": 0.16, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "YD", "gap_closure_ratio": 0.014, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "GDPR", "gap_closure_ratio": 0.015, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "RYDPC", "gap_closure_ratio": 0.014, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "IPOVALL", "gap_closure_ratio": 0.0003, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "IPOVCH", "gap_closure_ratio": 0.0002, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "JF", "gap_closure_ratio": 0.0149, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "TRLOWZ", "gap_closure_ratio": -0.00001, "gap_delta_abs": -0.1, "improved": False},
                    {"variant_id": "transfer-composite-medium", "variable": "LUB", "gap_closure_ratio": -0.00004, "gap_delta_abs": -0.1, "improved": False},
                    {"variant_id": "transfer-composite-medium", "variable": "UB", "gap_closure_ratio": -0.00001, "gap_delta_abs": -0.1, "improved": False},
                ]
            }
        ),
        encoding="utf-8",
    )

    payload = assess_phase1_distribution_package_readiness(
        experiment_report_path=experiment_report,
        ui_family_report_path=ui_family_report,
        transfer_family_report_path=transfer_family_report,
        report_path=tmp_path / "package.json",
    )

    assert payload["status"] == "working-package-candidate-on-core-surface"
    assert payload["package_internal_coexistence_passes"] is True
    assert payload["replacement_ready"] is False
    assert payload["ui_macro_policy"] == "optional"
    assert payload["scenario_scope"] == "experimental-intervention-surface"
    assert payload["final_parity_admissible"] is False
    report = json.loads((tmp_path / "package.json").read_text(encoding="utf-8"))
    assert report["overall"]["ui_macro_policy"] == "optional"
    assert report["overall"]["evidence_kind"] == "component-composed-or-partial"
    assert report["overall"]["scenario_scope"] == "experimental-intervention-surface"
    assert report["overall"]["final_parity_admissible"] is False
    assert report["variant_summaries"]["ui-relief"]["status"] == "working-repair-candidate-on-core-surface"
    assert report["variant_summaries"]["transfer-composite-medium"]["status"] == "promising-repair-candidate"
    assert any(
        "current package evidence is composed or acceptance-incomplete" in blocker
        for blocker in report["overall"]["blockers"]
    )
    assert any(
        "transfer macro repair is still modest" in blocker
        for blocker in report["overall"]["blockers"]
    )

    strict_payload = assess_phase1_distribution_package_readiness(
        experiment_report_path=experiment_report,
        ui_family_report_path=ui_family_report,
        transfer_family_report_path=transfer_family_report,
        ui_macro_policy="required",
        report_path=tmp_path / "package-strict.json",
    )
    assert strict_payload["status"] == "partial-package-candidate"
    strict_report = json.loads((tmp_path / "package-strict.json").read_text(encoding="utf-8"))
    assert strict_report["overall"]["ui_macro_policy"] == "required"
    assert any(
        "required PCY/UR macro lane" in blocker
        for blocker in strict_report["overall"]["blockers"]
    )


def test_compose_phase1_distribution_package_evidence_marks_composed_candidate(
    tmp_path: Path,
) -> None:
    ui_effect_report = tmp_path / "ui-effect.json"
    transfer_effect_report = tmp_path / "transfer-effect.json"
    control_compare_report = tmp_path / "control-compare.json"
    control_run_report = tmp_path / "control-run.json"
    reference_run_report = tmp_path / "reference-run.json"
    intervention_spec = tmp_path / "combined-spec.yaml"
    ui_family_report = tmp_path / "ui-family.json"
    transfer_family_report = tmp_path / "transfer-family.json"
    intervention_spec.write_text("id: combined-three-seam-balanced-2026q4\n", encoding="utf-8")

    restricted_run_payload = {
        "scenarios": {
            "ui-relief": {"backend": "fp-r"},
            "transfer-composite-medium": {"backend": "fp-r"},
        }
    }
    control_run_report.write_text(json.dumps(restricted_run_payload), encoding="utf-8")
    reference_run_report.write_text(json.dumps(restricted_run_payload), encoding="utf-8")
    ui_family_report.write_text(
        json.dumps(
            {
                "summary": {
                    "status": "core-generalized-with-optional-misses",
                    "core_all_pass": True,
                    "optional_all_pass": False,
                    "all_pass": False,
                }
            }
        ),
        encoding="utf-8",
    )
    transfer_family_report.write_text(
        json.dumps(
            {
                "summary": {
                    "status": "generalized",
                    "core_all_pass": True,
                    "optional_all_pass": True,
                    "all_pass": True,
                }
            }
        ),
        encoding="utf-8",
    )
    control_compare_report.write_text(json.dumps({"comparisons": []}), encoding="utf-8")
    ui_effect_report.write_text(
        json.dumps(
            {
                "rows": [
                    {"variant_id": "ui-relief", "variable": "LUB", "gap_closure_ratio": 0.20, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "ui-relief", "variable": "UB", "gap_closure_ratio": 0.08, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "ui-relief", "variable": "TRLOWZ", "gap_closure_ratio": 0.08, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "ui-relief", "variable": "IPOVALL", "gap_closure_ratio": 0.086, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "ui-relief", "variable": "IPOVCH", "gap_closure_ratio": 0.087, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "ui-relief", "variable": "RYDPC", "gap_closure_ratio": 0.083, "gap_delta_abs": 1.0, "improved": True},
                ]
            }
        ),
        encoding="utf-8",
    )
    transfer_effect_report.write_text(
        json.dumps(
            {
                "rows": [
                    {"variant_id": "transfer-composite-medium", "variable": "LUB", "gap_closure_ratio": 0.05, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "UB", "gap_closure_ratio": 0.019, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "INTG", "gap_closure_ratio": 0.127, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "JF", "gap_closure_ratio": 0.184, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "YD", "gap_closure_ratio": 0.035, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "GDPR", "gap_closure_ratio": 0.035, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "RYDPC", "gap_closure_ratio": 0.035, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "TRLOWZ", "gap_closure_ratio": 0.020, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "IPOVALL", "gap_closure_ratio": 0.024, "gap_delta_abs": 1.0, "improved": True},
                    {"variant_id": "transfer-composite-medium", "variable": "IPOVCH", "gap_closure_ratio": 0.023, "gap_delta_abs": 1.0, "improved": True},
                ]
            }
        ),
        encoding="utf-8",
    )

    package_root = tmp_path / "reports"
    payload = compose_phase1_distribution_package_evidence(
        package_id="combined-three-seam-balanced-2026q4",
        ui_effect_report_path=ui_effect_report,
        transfer_effect_report_path=transfer_effect_report,
        control_compare_report_path=control_compare_report,
        control_run_report_path=control_run_report,
        reference_run_report_path=reference_run_report,
        intervention_spec_path=intervention_spec,
        ui_family_report_path=ui_family_report,
        transfer_family_report_path=transfer_family_report,
        ui_macro_policy="optional",
        report_dir=package_root,
    )
    report = json.loads((package_root / "assess_phase1_distribution_package_readiness.composed.json").read_text(encoding="utf-8"))
    assert payload["status"] == "component-composed-supported-on-core-surface"
    assert payload["package_internal_coexistence_passes"] is True
    assert payload["replacement_ready"] is False
    assert payload["ui_macro_policy"] == "optional"
    assert payload["scenario_scope"] == "experimental-intervention-surface"
    assert payload["final_parity_admissible"] is False
    assert report["overall"]["ui_macro_policy"] == "optional"
    assert report["overall"]["evidence_kind"] == "component-composed-or-partial"
    assert report["overall"]["scenario_scope"] == "experimental-intervention-surface"
    assert report["overall"]["final_parity_admissible"] is False
    assert report["variant_summaries"]["ui-relief"]["status"] == "working-repair-candidate-on-core-surface"
    assert report["variant_summaries"]["transfer-composite-medium"]["status"] == "working-repair-candidate"
    assert any(
        "current package evidence is composed or acceptance-incomplete" in blocker
        for blocker in report["overall"]["blockers"]
    )

    reassessed = assess_phase1_distribution_package_readiness(
        experiment_report_path=package_root / "run_phase1_distribution_intervention_experiment.composed.json",
        effect_report_path=package_root / "assess_phase1_distribution_intervention_effect.composed.json",
        ui_family_report_path=ui_family_report,
        transfer_family_report_path=transfer_family_report,
        ui_macro_policy="required",
        report_path=package_root / "assess_phase1_distribution_package_readiness.composed.strict.json",
    )
    assert reassessed["status"] == "partial-package-candidate"
    strict_report = json.loads((package_root / "assess_phase1_distribution_package_readiness.composed.strict.json").read_text(encoding="utf-8"))
    assert strict_report["overall"]["ui_macro_policy"] == "required"


def test_assess_phase1_canonical_freeze_reports_canonical_vs_experimental(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _repo_paths_for_test(tmp_path)
    monkeypatch.setattr(canonical_freeze_module, "repo_paths", lambda: paths)
    monkeypatch.setattr(
        canonical_freeze_module,
        "_path_exists_in_ref",
        lambda repo_root, ref, path: True,
    )

    changed_paths = {
        "src/fp_ineq/phase1_ui.py",
        "src/fp_ineq/phase1_distribution_block.py",
    }
    monkeypatch.setattr(
        canonical_freeze_module,
        "_changed_against_ref",
        lambda repo_root, ref, path: path in changed_paths,
    )
    monkeypatch.setattr(
        canonical_freeze_module,
        "_untracked_paths",
        lambda repo_root, pathspec: [f"{pathspec}/example.yaml"],
    )

    payload = canonical_freeze_module.assess_phase1_canonical_freeze(
        remote_ref="origin/main",
        report_path=tmp_path / "canonical-freeze.json",
    )

    assert payload["canonical_distribution_catalog_preserved"] is True
    assert payload["canonical_transfer_core_preserved"] is True
    assert payload["experimental_intervention_specs_present"] is True
    assert payload["experimental_intervention_artifacts_present"] is True
    report = json.loads((tmp_path / "canonical-freeze.json").read_text(encoding="utf-8"))
    assert report["freeze_summary"]["canonical_distribution_catalog_preserved"] is True
    assert report["freeze_summary"]["experimental_intervention_specs_present"] is True
    assert "prohibit intervention specs" in report["freeze_summary"]["default_freeze_rule"]


def test_assess_phase1_distribution_canonical_parity_reports_blocked_behavior(
    tmp_path: Path,
) -> None:
    freeze_report = tmp_path / "freeze.json"
    compare_report = tmp_path / "compare.json"
    first_levels_report = tmp_path / "first-levels.json"
    boundary_report = tmp_path / "boundary.json"
    fp_r_identity_report = tmp_path / "fp-r-identity.json"
    fpexe_identity_report = tmp_path / "fpexe-identity.json"

    freeze_report.write_text(
        json.dumps(
            {
                "freeze_summary": {
                    "canonical_distribution_catalog_preserved": True,
                    "canonical_transfer_core_preserved": True,
                    "public_distribution_variants": [
                        "baseline-observed",
                        "ui-relief",
                        "transfer-composite-medium",
                        "ui-shock",
                    ],
                }
            }
        ),
        encoding="utf-8",
    )
    compare_report.write_text(
        json.dumps(
            {
                "left_backend": "fp-r",
                "right_backend": "fpexe",
                "summary": {"max_abs_diff": 1.0},
                "variant_ids": ["baseline-observed", "ui-relief", "transfer-composite-medium"],
                "comparisons": [
                    {"variant_id": "baseline-observed"},
                    {"variant_id": "ui-relief"},
                    {"variant_id": "transfer-composite-medium"},
                ],
            }
        ),
        encoding="utf-8",
    )
    first_levels_report.write_text(
        json.dumps(
            {
                "variants": [
                    {
                        "variant_id": "ui-relief",
                        "variables": {
                            "UB": {"delta_ratio_abs": 0.003, "delta_sign_match": True, "delta_abs_diff": 1.0, "level_abs_diff": 10.0},
                            "YD": {"delta_ratio_abs": 0.003, "delta_sign_match": True, "delta_abs_diff": 1.0, "level_abs_diff": 11.0},
                        },
                    },
                    {
                        "variant_id": "transfer-composite-medium",
                        "variables": {
                            "TRGH": {"delta_ratio_abs": 0.97, "delta_sign_match": True, "delta_abs_diff": 0.1, "level_abs_diff": 0.2},
                            "YD": {"delta_ratio_abs": 0.05, "delta_sign_match": False, "delta_abs_diff": 5.0, "level_abs_diff": 8.0},
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    boundary_report.write_text(
        json.dumps(
            {
                "overall": {"replacement_readiness": "not_ready", "recommendation": "Do not treat fp-r as a full replacement yet."},
                "ui_relief": {
                    "assessment": "open_attenuation",
                    "reason": "attenuated",
                    "identity_passes_required": True,
                },
                "transfer_composite_medium": {
                    "direct_channel_assessment": "pass_direct_channels",
                    "macro_channel_assessment": "block_macro_sign_flip",
                    "reason": "macro sign flip",
                    "identity_passes_required": True,
                },
            }
        ),
        encoding="utf-8",
    )
    fp_r_identity_report.write_text(
        json.dumps({"summary": {"passes": True}}),
        encoding="utf-8",
    )
    fpexe_identity_report.write_text(
        json.dumps({"summary": {"passes": False}}),
        encoding="utf-8",
    )

    payload = assess_phase1_distribution_canonical_parity(
        freeze_report_path=freeze_report,
        compare_report_path=compare_report,
        first_levels_report_path=first_levels_report,
        backend_boundary_report_path=boundary_report,
        fp_r_identity_report_path=fp_r_identity_report,
        fpexe_identity_report_path=fpexe_identity_report,
        report_path=tmp_path / "canonical-parity.json",
    )

    assert payload["status"] == "canonical-parity-blocked-by-behavior"
    assert payload["canonical_parity_ready"] is False
    assert payload["covered_variant_count"] == 3
    assert payload["missing_variant_count"] == 1
    report = json.loads((tmp_path / "canonical-parity.json").read_text(encoding="utf-8"))
    assert report["coverage"]["compare_surface_valid"] is True
    assert report["overall"]["canonical_surface_preserved"] is True
    assert report["overall"]["fp_r_identity_passes"] is True
    assert any("ui-relief remains strongly attenuated" in blocker for blocker in report["overall"]["blockers"])
    assert any("macro-income sign flip" in blocker for blocker in report["overall"]["blockers"])


def test_assess_phase1_distribution_canonical_parity_prefers_clean_transfer_attenuation_signal(
    tmp_path: Path,
) -> None:
    freeze_report = tmp_path / "freeze.json"
    compare_report = tmp_path / "compare.json"
    first_levels_report = tmp_path / "first-levels.json"
    boundary_report = tmp_path / "boundary.json"
    fp_r_identity_report = tmp_path / "fp-r-identity.json"
    fpexe_identity_report = tmp_path / "fpexe-identity.json"

    freeze_report.write_text(
        json.dumps(
            {
                "freeze_summary": {
                    "canonical_distribution_catalog_preserved": True,
                    "canonical_transfer_core_preserved": True,
                    "public_distribution_variants": [
                        "baseline-observed",
                        "ui-relief",
                        "transfer-composite-medium",
                    ],
                }
            }
        ),
        encoding="utf-8",
    )
    compare_report.write_text(
        json.dumps(
            {
                "left_backend": "fp-r",
                "right_backend": "fpexe",
                "summary": {"max_abs_diff": 1.0},
                "variant_ids": ["baseline-observed", "ui-relief", "transfer-composite-medium"],
                "comparisons": [
                    {"variant_id": "baseline-observed"},
                    {"variant_id": "ui-relief"},
                    {"variant_id": "transfer-composite-medium"},
                ],
            }
        ),
        encoding="utf-8",
    )
    first_levels_report.write_text(
        json.dumps(
            {
                "variants": [
                    {
                        "variant_id": "ui-relief",
                        "variables": {
                            "UB": {"delta_ratio_abs": 0.01, "delta_sign_match": True, "delta_abs_diff": 1.0, "level_abs_diff": 10.0},
                        },
                    },
                    {
                        "variant_id": "transfer-composite-medium",
                        "variables": {
                            "TRGH": {"delta_ratio_abs": 0.12, "delta_sign_match": True, "delta_abs_diff": 0.1, "level_abs_diff": 0.2},
                            "TRSH": {"delta_ratio_abs": 0.11, "delta_sign_match": True, "delta_abs_diff": 0.1, "level_abs_diff": 0.2},
                            "YD": {"delta_ratio_abs": 0.01, "delta_sign_match": True, "delta_abs_diff": 5.0, "level_abs_diff": 8.0},
                            "GDPR": {"delta_ratio_abs": 0.01, "delta_sign_match": True, "delta_abs_diff": 4.0, "level_abs_diff": 7.0},
                            "RYDPC": {"delta_ratio_abs": 0.01, "delta_sign_match": True, "delta_abs_diff": 0.5, "level_abs_diff": 1.0},
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    boundary_report.write_text(
        json.dumps(
            {
                "overall": {"replacement_readiness": "not_ready", "recommendation": "Do not treat fp-r as a full replacement yet."},
                "ui_relief": {
                    "assessment": "open_attenuation",
                    "reason": "attenuated",
                    "identity_passes_required": True,
                },
                "transfer_composite_medium": {
                    "direct_channel_assessment": "pass_direct_channels",
                    "macro_channel_assessment": "block_macro_sign_flip",
                    "reason": "macro sign flip",
                    "identity_passes_required": True,
                },
            }
        ),
        encoding="utf-8",
    )
    fp_r_identity_report.write_text(json.dumps({"summary": {"passes": True}}), encoding="utf-8")
    fpexe_identity_report.write_text(json.dumps({"summary": {"passes": False}}), encoding="utf-8")

    payload = assess_phase1_distribution_canonical_parity(
        freeze_report_path=freeze_report,
        compare_report_path=compare_report,
        first_levels_report_path=first_levels_report,
        backend_boundary_report_path=boundary_report,
        fp_r_identity_report_path=fp_r_identity_report,
        fpexe_identity_report_path=fpexe_identity_report,
        report_path=tmp_path / "canonical-parity.json",
    )

    assert payload["status"] == "canonical-parity-blocked-by-behavior"
    report = json.loads((tmp_path / "canonical-parity.json").read_text(encoding="utf-8"))
    assert any("transfer-composite-medium remains strongly attenuated" in blocker for blocker in report["overall"]["blockers"])
    assert not any("macro-income sign flip" in blocker for blocker in report["overall"]["blockers"])
    assert report["transfer_composite_medium"]["first_level_signs_match"] is True
    assert report["transfer_composite_medium"]["first_level_max_delta_ratio_abs"] == pytest.approx(0.12)


def test_assess_phase1_distribution_canonical_parity_flags_ui_shock_wrong_sign(
    tmp_path: Path,
) -> None:
    freeze_report = tmp_path / "freeze.json"
    compare_report = tmp_path / "compare.json"
    first_levels_report = tmp_path / "first-levels.json"
    boundary_report = tmp_path / "boundary.json"
    fp_r_identity_report = tmp_path / "fp-r-identity.json"
    fpexe_identity_report = tmp_path / "fpexe-identity.json"

    freeze_report.write_text(
        json.dumps(
            {
                "freeze_summary": {
                    "canonical_distribution_catalog_preserved": True,
                    "canonical_transfer_core_preserved": True,
                    "public_distribution_variants": [
                        "baseline-observed",
                        "ui-relief",
                        "ui-shock",
                    ],
                }
            }
        ),
        encoding="utf-8",
    )
    compare_report.write_text(
        json.dumps(
            {
                "left_backend": "fp-r",
                "right_backend": "fpexe",
                "summary": {"max_abs_diff": 1.0},
                "variant_ids": ["baseline-observed", "ui-relief", "ui-shock"],
                "comparisons": [
                    {"variant_id": "baseline-observed"},
                    {"variant_id": "ui-relief"},
                    {"variant_id": "ui-shock"},
                ],
            }
        ),
        encoding="utf-8",
    )
    first_levels_report.write_text(
        json.dumps(
            {
                "variants": [
                    {
                        "variant_id": "ui-relief",
                        "variables": {
                            "UB": {"delta_ratio_abs": 0.01, "delta_sign_match": True, "delta_abs_diff": 1.0, "level_abs_diff": 10.0},
                        },
                    },
                    {
                        "variant_id": "ui-shock",
                        "variables": {
                            "UB": {"delta_ratio_abs": 0.015, "delta_sign_match": False, "delta_abs_diff": 8.0, "level_abs_diff": 8.1},
                            "YD": {"delta_ratio_abs": 0.015, "delta_sign_match": False, "delta_abs_diff": 8.0, "level_abs_diff": 8.1},
                            "GDPR": {"delta_ratio_abs": 0.015, "delta_sign_match": False, "delta_abs_diff": 0.7, "level_abs_diff": 0.7},
                            "RYDPC": {"delta_ratio_abs": 0.015, "delta_sign_match": False, "delta_abs_diff": 0.02, "level_abs_diff": 0.02},
                            "TRLOWZ": {"delta_ratio_abs": 0.015, "delta_sign_match": False, "delta_abs_diff": 0.02, "level_abs_diff": 0.02},
                            "IPOVALL": {"delta_ratio_abs": 0.015, "delta_sign_match": False, "delta_abs_diff": 0.0004, "level_abs_diff": 0.0004},
                            "IPOVCH": {"delta_ratio_abs": 0.015, "delta_sign_match": False, "delta_abs_diff": 0.0011, "level_abs_diff": 0.0011},
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    boundary_report.write_text(
        json.dumps(
            {
                "overall": {"replacement_readiness": "not_ready", "recommendation": "Do not treat fp-r as a full replacement yet."},
                "ui_relief": {
                    "assessment": "open_attenuation",
                    "reason": "attenuated",
                    "identity_passes_required": True,
                },
                "transfer_composite_medium": {
                    "direct_channel_assessment": "pass_direct_channels",
                    "macro_channel_assessment": "pass_macro_direction",
                    "reason": "not used here",
                    "identity_passes_required": True,
                },
            }
        ),
        encoding="utf-8",
    )
    fp_r_identity_report.write_text(json.dumps({"summary": {"passes": True}}), encoding="utf-8")
    fpexe_identity_report.write_text(json.dumps({"summary": {"passes": False}}), encoding="utf-8")

    payload = assess_phase1_distribution_canonical_parity(
        freeze_report_path=freeze_report,
        compare_report_path=compare_report,
        first_levels_report_path=first_levels_report,
        backend_boundary_report_path=boundary_report,
        fp_r_identity_report_path=fp_r_identity_report,
        fpexe_identity_report_path=fpexe_identity_report,
        report_path=tmp_path / "canonical-parity.json",
    )

    assert payload["status"] == "canonical-parity-blocked-by-behavior"
    report = json.loads((tmp_path / "canonical-parity.json").read_text(encoding="utf-8"))
    assert any("ui-shock remains opposite-sign" in blocker for blocker in report["overall"]["blockers"])
    assert report["ui_shock"]["first_level_signs_match"] is False
    assert report["ui_shock"]["first_level_max_delta_ratio_abs"] == pytest.approx(0.015)


def test_analyze_phase1_distribution_canonical_blocker_traces_reports_ui_and_transfer_traces(
    tmp_path: Path,
) -> None:
    def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def write_run_report(root: Path, variant_id: str) -> Path:
        output_dir = root / variant_id
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "LOADFORMAT.DAT").write_text("", encoding="utf-8")
        report_path = root / f"{variant_id}.json"
        report_path.write_text(
            json.dumps(
                {
                    "scenarios": {
                        variant_id: {
                            "variant_id": variant_id,
                            "loadformat_path": str(output_dir / "LOADFORMAT.DAT"),
                            "output_dir": str(output_dir),
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        return report_path

    baseline_report = write_run_report(tmp_path / "reports", "baseline-observed")
    ui_relief_report = write_run_report(tmp_path / "reports", "ui-relief")
    ui_shock_report = write_run_report(tmp_path / "reports", "ui-shock")
    transfer_report = write_run_report(tmp_path / "reports", "transfer-composite-medium")
    ui_relief_exclude_report = write_run_report(tmp_path / "reports-exclude", "ui-relief")
    ui_shock_exclude_report = write_run_report(tmp_path / "reports-exclude", "ui-shock")
    transfer_exclude_report = write_run_report(tmp_path / "reports-exclude", "transfer-composite-medium")

    equation_fields = ["period", "target", "iteration", "trace_kind", "variable", "lag", "value"]
    estimation_fields = ["target", "equation_number", "method", "fsr_reference_names", "active_fsr_reference_names"]
    solve_trace_fields = ["phase", "period", "variable", "value"]

    for report_path, variant_id, uifac_value, solved_lub in [
        (ui_relief_report, "ui-relief", "1.02", "2.23"),
        (ui_shock_report, "ui-shock", "0.98", "2.22"),
    ]:
        work = Path(json.loads(report_path.read_text(encoding="utf-8"))["scenarios"][variant_id]["output_dir"]) / "work"
        write_csv(
            work / "EQUATION_INPUT_SNAPSHOT.csv",
            equation_fields,
            [
                {"period": "2026.1", "target": "LUB", "iteration": "1", "trace_kind": "previous_value", "variable": "", "lag": "0", "value": "2.25"},
                {"period": "2026.1", "target": "LUB", "iteration": "1", "trace_kind": "evaluated_structural", "variable": "", "lag": "0", "value": solved_lub},
                {"period": "2026.1", "target": "LUB", "iteration": "1", "trace_kind": "evaluated_value", "variable": "", "lag": "0", "value": solved_lub},
                {"period": "2026.1", "target": "LUB", "iteration": "1", "trace_kind": "compiled_reference", "variable": "C", "lag": "0", "value": "1.0"},
            ],
        )
        write_csv(
            work / "ESTIMATION_EQUATIONS.csv",
            estimation_fields,
            [
                {
                    "target": "LUB",
                    "equation_number": "16",
                    "method": "FSR",
                    "fsr_reference_names": "C LUB",
                    "active_fsr_reference_names": "NA",
                }
            ],
        )
        write_csv(
            work / "SOLVE_INPUT_TRACE.csv",
            solve_trace_fields,
            [
                {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "LUB", "value": "2.25"},
                {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "UIFAC", "value": "1.00"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "LUB", "value": "2.25"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "UIFAC", "value": uifac_value},
            ],
        )
        write_csv(
            work / "EXOGENOUS_PATH_TRACE.csv",
            solve_trace_fields,
            [
                {"phase": "post_extrapolate", "period": "2026.1", "variable": "LUB", "value": "2.25"},
                {"phase": "post_extrapolate", "period": "2026.1", "variable": "UIFAC", "value": "1.00"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "LUB", "value": "2.25"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "UIFAC", "value": uifac_value},
            ],
        )

    for report_path, variant_id, uifac_value, solved_lub in [
        (ui_relief_exclude_report, "ui-relief", "1.02", "2.25"),
        (ui_shock_exclude_report, "ui-shock", "0.98", "2.25"),
    ]:
        work = Path(json.loads(report_path.read_text(encoding="utf-8"))["scenarios"][variant_id]["output_dir"]) / "work"
        write_csv(
            work / "EQUATION_INPUT_SNAPSHOT.csv",
            equation_fields,
            [
                {"period": "2026.1", "target": "LUB", "iteration": "1", "trace_kind": "previous_value", "variable": "", "lag": "0", "value": "2.25"},
                {"period": "2026.1", "target": "LUB", "iteration": "1", "trace_kind": "evaluated_structural", "variable": "", "lag": "0", "value": solved_lub},
                {"period": "2026.1", "target": "LUB", "iteration": "1", "trace_kind": "evaluated_value", "variable": "", "lag": "0", "value": solved_lub},
                {"period": "2026.1", "target": "LUB", "iteration": "1", "trace_kind": "compiled_reference", "variable": "C", "lag": "0", "value": "1.0"},
            ],
        )
        write_csv(
            work / "ESTIMATION_EQUATIONS.csv",
            estimation_fields,
            [
                {
                    "target": "LUB",
                    "equation_number": "16",
                    "method": "FSR",
                    "fsr_reference_names": "C LUB",
                    "active_fsr_reference_names": "NA",
                }
            ],
        )
        write_csv(
            work / "SOLVE_INPUT_TRACE.csv",
            solve_trace_fields,
            [
                {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "LUB", "value": "2.25"},
                {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "UIFAC", "value": "1.00"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "LUB", "value": "2.25"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "UIFAC", "value": uifac_value},
            ],
        )
        write_csv(
            work / "EXOGENOUS_PATH_TRACE.csv",
            solve_trace_fields,
            [
                {"phase": "post_extrapolate", "period": "2026.1", "variable": "LUB", "value": "2.25"},
                {"phase": "post_extrapolate", "period": "2026.1", "variable": "UIFAC", "value": "1.00"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "LUB", "value": "2.25"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "UIFAC", "value": uifac_value},
            ],
        )

    baseline_work = Path(json.loads(baseline_report.read_text(encoding="utf-8"))["scenarios"]["baseline-observed"]["output_dir"]) / "work"
    transfer_work = Path(json.loads(transfer_report.read_text(encoding="utf-8"))["scenarios"]["transfer-composite-medium"]["output_dir"]) / "work"
    transfer_exclude_work = Path(json.loads(transfer_exclude_report.read_text(encoding="utf-8"))["scenarios"]["transfer-composite-medium"]["output_dir"]) / "work"
    write_csv(
        baseline_work / "EQUATION_INPUT_SNAPSHOT.csv",
        equation_fields,
        [
            {"period": "2026.1", "target": "INTGZ", "iteration": "1", "trace_kind": "previous_value", "variable": "", "lag": "0", "value": "0.01"},
            {"period": "2026.1", "target": "INTGZ", "iteration": "1", "trace_kind": "evaluated_structural", "variable": "", "lag": "0", "value": "0.01"},
            {"period": "2026.1", "target": "INTGZ", "iteration": "1", "trace_kind": "evaluated_value", "variable": "", "lag": "0", "value": "0.01"},
            {"period": "2026.1", "target": "INTGZ", "iteration": "1", "trace_kind": "compiled_reference", "variable": "AAG", "lag": "0", "value": "100.0"},
            {"period": "2026.1", "target": "LJF1", "iteration": "1", "trace_kind": "previous_value", "variable": "", "lag": "0", "value": "0.001"},
            {"period": "2026.1", "target": "LJF1", "iteration": "1", "trace_kind": "evaluated_structural", "variable": "", "lag": "0", "value": "0.001"},
            {"period": "2026.1", "target": "LJF1", "iteration": "1", "trace_kind": "evaluated_value", "variable": "", "lag": "0", "value": "0.001"},
            {"period": "2026.1", "target": "LJF1", "iteration": "1", "trace_kind": "compiled_reference", "variable": "LY1", "lag": "0", "value": "1.0"},
            {"period": "2026.1", "target": "INTG", "iteration": "1", "trace_kind": "previous_value", "variable": "", "lag": "0", "value": "289.2"},
            {"period": "2026.1", "target": "INTG", "iteration": "1", "trace_kind": "evaluated_structural", "variable": "", "lag": "0", "value": "289.2"},
            {"period": "2026.1", "target": "INTG", "iteration": "1", "trace_kind": "evaluated_value", "variable": "", "lag": "0", "value": "289.2"},
            {"period": "2026.1", "target": "JF", "iteration": "1", "trace_kind": "previous_value", "variable": "", "lag": "0", "value": "151.8"},
            {"period": "2026.1", "target": "JF", "iteration": "1", "trace_kind": "evaluated_structural", "variable": "", "lag": "0", "value": "151.8"},
            {"period": "2026.1", "target": "JF", "iteration": "1", "trace_kind": "evaluated_value", "variable": "", "lag": "0", "value": "151.8"},
        ],
    )
    write_csv(
        baseline_work / "ESTIMATION_EQUATIONS.csv",
        estimation_fields,
        [
            {"target": "INTGZ", "equation_number": "29", "method": "OLS", "fsr_reference_names": "AAG", "active_fsr_reference_names": "NA"},
            {"target": "LJF1", "equation_number": "13", "method": "FSR", "fsr_reference_names": "LY1", "active_fsr_reference_names": "D20201"},
        ],
    )
    write_csv(
        baseline_work / "SOLVE_INPUT_TRACE.csv",
        solve_trace_fields,
        [
            {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "INTG", "value": "289.2"},
            {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "JF", "value": "151.8"},
            {"phase": "final_pre_solve", "period": "2026.1", "variable": "INTG", "value": "289.2"},
            {"phase": "final_pre_solve", "period": "2026.1", "variable": "JF", "value": "151.8"},
        ],
    )

    write_csv(
        transfer_work / "EQUATION_INPUT_SNAPSHOT.csv",
        equation_fields,
        [
            {"period": "2026.1", "target": "INTGZ", "iteration": "1", "trace_kind": "previous_value", "variable": "", "lag": "0", "value": "0.01"},
            {"period": "2026.1", "target": "INTGZ", "iteration": "1", "trace_kind": "evaluated_structural", "variable": "", "lag": "0", "value": "0.0101"},
            {"period": "2026.1", "target": "INTGZ", "iteration": "1", "trace_kind": "evaluated_value", "variable": "", "lag": "0", "value": "0.0101"},
            {"period": "2026.1", "target": "INTGZ", "iteration": "1", "trace_kind": "compiled_reference", "variable": "AAG", "lag": "0", "value": "101.0"},
            {"period": "2026.1", "target": "LJF1", "iteration": "1", "trace_kind": "previous_value", "variable": "", "lag": "0", "value": "0.001"},
            {"period": "2026.1", "target": "LJF1", "iteration": "1", "trace_kind": "evaluated_structural", "variable": "", "lag": "0", "value": "0.0011"},
            {"period": "2026.1", "target": "LJF1", "iteration": "1", "trace_kind": "evaluated_value", "variable": "", "lag": "0", "value": "0.0011"},
            {"period": "2026.1", "target": "LJF1", "iteration": "1", "trace_kind": "compiled_reference", "variable": "LY1", "lag": "0", "value": "1.2"},
            {"period": "2026.1", "target": "INTG", "iteration": "1", "trace_kind": "previous_value", "variable": "", "lag": "0", "value": "289.2"},
            {"period": "2026.1", "target": "INTG", "iteration": "1", "trace_kind": "evaluated_structural", "variable": "", "lag": "0", "value": "289.2"},
            {"period": "2026.1", "target": "INTG", "iteration": "1", "trace_kind": "evaluated_value", "variable": "", "lag": "0", "value": "289.2"},
            {"period": "2026.1", "target": "JF", "iteration": "1", "trace_kind": "previous_value", "variable": "", "lag": "0", "value": "151.8"},
            {"period": "2026.1", "target": "JF", "iteration": "1", "trace_kind": "evaluated_structural", "variable": "", "lag": "0", "value": "151.8"},
            {"period": "2026.1", "target": "JF", "iteration": "1", "trace_kind": "evaluated_value", "variable": "", "lag": "0", "value": "151.8"},
        ],
    )
    write_csv(
        transfer_work / "ESTIMATION_EQUATIONS.csv",
        estimation_fields,
        [
            {"target": "INTGZ", "equation_number": "29", "method": "OLS", "fsr_reference_names": "AAG", "active_fsr_reference_names": "NA"},
            {"target": "LJF1", "equation_number": "13", "method": "FSR", "fsr_reference_names": "LY1", "active_fsr_reference_names": "D20201"},
        ],
    )
    write_csv(
        transfer_work / "SOLVE_INPUT_TRACE.csv",
        solve_trace_fields,
        [
            {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "INTG", "value": "289.2"},
            {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "JF", "value": "151.8"},
            {"phase": "final_pre_solve", "period": "2026.1", "variable": "INTG", "value": "289.2"},
            {"phase": "final_pre_solve", "period": "2026.1", "variable": "JF", "value": "151.8"},
        ],
    )
    write_csv(
        transfer_exclude_work / "EQUATION_INPUT_SNAPSHOT.csv",
        equation_fields,
        [
            {"period": "2026.1", "target": "INTGZ", "iteration": "1", "trace_kind": "previous_value", "variable": "", "lag": "0", "value": "0.01"},
            {"period": "2026.1", "target": "INTGZ", "iteration": "1", "trace_kind": "evaluated_structural", "variable": "", "lag": "0", "value": "0.0101"},
            {"period": "2026.1", "target": "INTGZ", "iteration": "1", "trace_kind": "evaluated_value", "variable": "", "lag": "0", "value": "0.0101"},
            {"period": "2026.1", "target": "INTGZ", "iteration": "1", "trace_kind": "compiled_reference", "variable": "AAG", "lag": "0", "value": "101.0"},
            {"period": "2026.1", "target": "LJF1", "iteration": "1", "trace_kind": "previous_value", "variable": "", "lag": "0", "value": "0.001"},
            {"period": "2026.1", "target": "LJF1", "iteration": "1", "trace_kind": "evaluated_structural", "variable": "", "lag": "0", "value": "0.0011"},
            {"period": "2026.1", "target": "LJF1", "iteration": "1", "trace_kind": "evaluated_value", "variable": "", "lag": "0", "value": "0.0011"},
            {"period": "2026.1", "target": "LJF1", "iteration": "1", "trace_kind": "compiled_reference", "variable": "LY1", "lag": "0", "value": "1.2"},
            {"period": "2026.1", "target": "INTG", "iteration": "1", "trace_kind": "previous_value", "variable": "", "lag": "0", "value": "289.2"},
            {"period": "2026.1", "target": "INTG", "iteration": "1", "trace_kind": "evaluated_structural", "variable": "", "lag": "0", "value": "0.0"},
            {"period": "2026.1", "target": "INTG", "iteration": "1", "trace_kind": "evaluated_value", "variable": "", "lag": "0", "value": "0.0"},
            {"period": "2026.1", "target": "JF", "iteration": "1", "trace_kind": "previous_value", "variable": "", "lag": "0", "value": "151.8"},
            {"period": "2026.1", "target": "JF", "iteration": "1", "trace_kind": "evaluated_structural", "variable": "", "lag": "0", "value": "0.0"},
            {"period": "2026.1", "target": "JF", "iteration": "1", "trace_kind": "evaluated_value", "variable": "", "lag": "0", "value": "0.0"},
        ],
    )
    write_csv(
        transfer_exclude_work / "ESTIMATION_EQUATIONS.csv",
        estimation_fields,
        [
            {"target": "INTGZ", "equation_number": "29", "method": "OLS", "fsr_reference_names": "AAG", "active_fsr_reference_names": "NA"},
            {"target": "LJF1", "equation_number": "13", "method": "FSR", "fsr_reference_names": "LY1", "active_fsr_reference_names": "D20201"},
        ],
    )
    write_csv(
        transfer_exclude_work / "SOLVE_INPUT_TRACE.csv",
        solve_trace_fields,
        [
            {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "INTG", "value": "289.2"},
            {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "JF", "value": "151.8"},
            {"phase": "final_pre_solve", "period": "2026.1", "variable": "INTG", "value": "289.2"},
            {"phase": "final_pre_solve", "period": "2026.1", "variable": "JF", "value": "151.8"},
        ],
    )

    first_levels_report = tmp_path / "first-levels.json"
    first_levels_report.write_text(
        json.dumps(
            {
                "variants": [
                    {"variant_id": "ui-relief", "variables": {"UB": {"delta_ratio_abs": 0.01, "delta_sign_match": True}}},
                    {"variant_id": "ui-shock", "variables": {"UB": {"delta_ratio_abs": 0.015, "delta_sign_match": False}}},
                    {
                        "variant_id": "transfer-composite-medium",
                        "variables": {
                            "INTG": {"delta_ratio_abs": 0.007, "delta_sign_match": True},
                            "JF": {"delta_ratio_abs": 0.007, "delta_sign_match": True},
                            "YD": {"delta_ratio_abs": 0.006, "delta_sign_match": True},
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    ui_relief_exclude_first_levels_report = tmp_path / "ui-relief-exclude-first-levels.json"
    ui_relief_exclude_first_levels_report.write_text(
        json.dumps(
            {
                "variants": [
                    {
                        "variant_id": "ui-relief",
                        "variables": {"UB": {"delta_ratio_abs": 0.003, "delta_sign_match": True}},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    ui_shock_exclude_first_levels_report = tmp_path / "ui-shock-exclude-first-levels.json"
    ui_shock_exclude_first_levels_report.write_text(
        json.dumps(
            {
                "variants": [
                    {
                        "variant_id": "ui-shock",
                        "variables": {"UB": {"delta_ratio_abs": 0.023, "delta_sign_match": True}},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    transfer_exclude_first_levels_report = tmp_path / "transfer-exclude-first-levels.json"
    transfer_exclude_first_levels_report.write_text(
        json.dumps(
            {
                "variants": [
                    {
                        "variant_id": "transfer-composite-medium",
                        "variables": {
                            "INTG": {"delta_ratio_abs": 0.0, "delta_sign_match": False},
                            "JF": {"delta_ratio_abs": 0.0, "delta_sign_match": False},
                            "TRGH": {"delta_ratio_abs": 37.9, "delta_sign_match": True},
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    payload = analyze_phase1_distribution_canonical_blocker_traces(
        baseline_run_report_path=baseline_report,
        ui_relief_run_report_path=ui_relief_report,
        ui_shock_run_report_path=ui_shock_report,
        transfer_medium_run_report_path=transfer_report,
        first_levels_report_path=first_levels_report,
        ui_relief_exclude_run_report_path=ui_relief_exclude_report,
        ui_shock_exclude_run_report_path=ui_shock_exclude_report,
        transfer_medium_exclude_run_report_path=transfer_exclude_report,
        ui_relief_exclude_first_levels_report_path=ui_relief_exclude_first_levels_report,
        ui_shock_exclude_first_levels_report_path=ui_shock_exclude_first_levels_report,
        transfer_medium_exclude_first_levels_report_path=transfer_exclude_first_levels_report,
        report_path=tmp_path / "canonical-blockers.json",
    )

    assert payload["ui_relief_has_uifac_reference"] is False
    assert payload["ui_shock_has_uifac_reference"] is False
    assert payload["transfer_pre_solve_changed_variables"] == []
    assert payload["transfer_intgz_policy_branch_identical"] is True
    assert payload["transfer_ljf1_policy_branch_identical"] is True
    report = json.loads((tmp_path / "canonical-blockers.json").read_text(encoding="utf-8"))
    assert report["ui_relief"]["uifac_changes_before_solve"] is True
    assert report["ui_relief"]["lub_changes_before_solve"] is False
    assert report["transfer_composite_medium"]["intgz_equation_summary"]["top_compiled_reference_deltas"]["AAG"] == pytest.approx(1.0)
    assert "no UIFAC reference" in report["overall_findings"][0]
    assert report["ui_shock_branch_comparison"]["first_level_focus_comparison"]["variables"]["UB"]["exclude_delta_sign_match"] is True
    assert report["transfer_composite_medium_branch_comparison"]["intgz_equation_branch_comparison"]["first_iteration"]["identical"] is True


def test_analyze_phase1_distribution_canonical_solved_path_reports_inside_solve_gaps(
    tmp_path: Path,
) -> None:
    def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def write_run_report(root: Path, variant_id: str) -> Path:
        output_dir = root / variant_id
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "LOADFORMAT.DAT").write_text("", encoding="utf-8")
        report_path = root / f"{variant_id}.json"
        report_path.write_text(
            json.dumps(
                {
                    "scenarios": {
                        variant_id: {
                            "variant_id": variant_id,
                            "loadformat_path": str(output_dir / "LOADFORMAT.DAT"),
                            "output_dir": str(output_dir),
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        return report_path

    baseline_report = write_run_report(tmp_path / "reports", "baseline-observed")
    ui_relief_report = write_run_report(tmp_path / "reports", "ui-relief")
    ui_shock_report = write_run_report(tmp_path / "reports", "ui-shock")
    transfer_report = write_run_report(tmp_path / "reports", "transfer-composite-medium")

    trace_fields = ["phase", "period", "variable", "value"]
    for report_path, variant_id, ub_post, yd_post, gdpr_post in [
        (baseline_report, "baseline-observed", "9.575", "5555.475", "6027.975"),
        (ui_relief_report, "ui-relief", "9.18", "5556.0", "6028.2"),
        (ui_shock_report, "ui-shock", "8.82", "5554.8", "6027.7"),
    ]:
        work = Path(json.loads(report_path.read_text(encoding="utf-8"))["scenarios"][variant_id]["output_dir"]) / "work"
        write_csv(
            work / "EXOGENOUS_PATH_TRACE.csv",
            trace_fields,
            [
                {"phase": "forecast_window_entry", "period": "2026.1", "variable": "LUB", "value": "2.2591"},
                {"phase": "forecast_window_entry", "period": "2026.1", "variable": "UB", "value": "9.575"},
                {"phase": "forecast_window_entry", "period": "2026.1", "variable": "YD", "value": "5555.475"},
                {"phase": "forecast_window_entry", "period": "2026.1", "variable": "GDPR", "value": "6027.975"},
                {"phase": "forecast_window_entry", "period": "2026.1", "variable": "UR", "value": "0.044"},
                {"phase": "forecast_window_entry", "period": "2026.1", "variable": "RS", "value": "3.73"},
                {"phase": "forecast_window_entry", "period": "2026.1", "variable": "PCY", "value": "2.42"},
                {"phase": "pre_solve_stage_1", "period": "2026.1", "variable": "LUB", "value": "2.2591"},
                {"phase": "pre_solve_stage_1", "period": "2026.1", "variable": "UB", "value": "9.575"},
                {"phase": "pre_solve_stage_1", "period": "2026.1", "variable": "YD", "value": "5555.475"},
                {"phase": "pre_solve_stage_1", "period": "2026.1", "variable": "GDPR", "value": "6027.975"},
                {"phase": "pre_solve_stage_1", "period": "2026.1", "variable": "UR", "value": "0.044"},
                {"phase": "pre_solve_stage_1", "period": "2026.1", "variable": "RS", "value": "3.73"},
                {"phase": "pre_solve_stage_1", "period": "2026.1", "variable": "PCY", "value": "2.42"},
                {"phase": "post_solve_stage_1", "period": "2026.1", "variable": "LUB", "value": "2.23"},
                {"phase": "post_solve_stage_1", "period": "2026.1", "variable": "UB", "value": ub_post},
                {"phase": "post_solve_stage_1", "period": "2026.1", "variable": "YD", "value": yd_post},
                {"phase": "post_solve_stage_1", "period": "2026.1", "variable": "GDPR", "value": gdpr_post},
                {"phase": "post_solve_stage_1", "period": "2026.1", "variable": "UR", "value": "0.0441"},
                {"phase": "post_solve_stage_1", "period": "2026.1", "variable": "RS", "value": "3.731"},
                {"phase": "post_solve_stage_1", "period": "2026.1", "variable": "PCY", "value": "2.421"},
            ],
        )

    for report_path, variant_id in [
        (baseline_report, "baseline-observed"),
        (transfer_report, "transfer-composite-medium"),
    ]:
        work = Path(json.loads(report_path.read_text(encoding="utf-8"))["scenarios"][variant_id]["output_dir"]) / "work"
        write_csv(
            work / "SOLVE_INPUT_TRACE.csv",
            trace_fields,
            [
                {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "INTG", "value": "301.2"},
                {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "JF", "value": "152.1"},
                {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "YD", "value": "5555.475"},
                {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "GDPR", "value": "6027.975"},
                {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "RYDPC", "value": "15.61"},
                {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "RS", "value": "3.73"},
                {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "PCY", "value": "2.42"},
                {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "UR", "value": "0.044"},
                {"phase": "incoming_bundle_state", "period": "2026.1", "variable": "GDPD", "value": "1.31"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "INTG", "value": "301.2"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "JF", "value": "152.1"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "YD", "value": "5555.475"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "GDPR", "value": "6027.975"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "RYDPC", "value": "15.61"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "RS", "value": "3.73"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "PCY", "value": "2.42"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "UR", "value": "0.044"},
                {"phase": "final_pre_solve", "period": "2026.1", "variable": "GDPD", "value": "1.31"},
            ],
        )

    first_levels_report = tmp_path / "first-levels.json"
    first_levels_report.write_text(
        json.dumps(
            {
                "variants": [
                    {
                        "variant_id": "ui-relief",
                        "variables": {
                            "LUB": {"delta_ratio_abs": 0.016, "delta_sign_match": True, "fp-r": {"delta_first": -0.0291}, "fpexe": {"delta_first": -1.8}},
                            "UB": {"delta_ratio_abs": 0.009, "delta_sign_match": True, "fp-r": {"delta_first": -0.395}, "fpexe": {"delta_first": -44.0}},
                            "YD": {"delta_ratio_abs": 0.009, "delta_sign_match": True, "fp-r": {"delta_first": 0.525}, "fpexe": {"delta_first": 58.0}},
                            "GDPR": {"delta_ratio_abs": 0.009, "delta_sign_match": True, "fp-r": {"delta_first": 0.225}, "fpexe": {"delta_first": 25.0}},
                            "UR": {"delta_ratio_abs": 0.009, "delta_sign_match": True, "fp-r": {"delta_first": 0.0001}, "fpexe": {"delta_first": 0.011}},
                            "RS": {"delta_ratio_abs": 0.009, "delta_sign_match": True, "fp-r": {"delta_first": 0.001}, "fpexe": {"delta_first": 0.11}},
                            "PCY": {"delta_ratio_abs": 0.009, "delta_sign_match": True, "fp-r": {"delta_first": 0.001}, "fpexe": {"delta_first": 0.11}},
                        },
                    },
                    {
                        "variant_id": "ui-shock",
                        "variables": {
                            "LUB": {"delta_ratio_abs": 0.016, "delta_sign_match": False, "fp-r": {"delta_first": -0.0291}, "fpexe": {"delta_first": 1.8}},
                            "UB": {"delta_ratio_abs": 0.015, "delta_sign_match": False, "fp-r": {"delta_first": -0.755}, "fpexe": {"delta_first": 49.0}},
                            "YD": {"delta_ratio_abs": 0.015, "delta_sign_match": False, "fp-r": {"delta_first": -0.675}, "fpexe": {"delta_first": 43.0}},
                            "GDPR": {"delta_ratio_abs": 0.015, "delta_sign_match": False, "fp-r": {"delta_first": -0.275}, "fpexe": {"delta_first": 18.0}},
                            "UR": {"delta_ratio_abs": 0.015, "delta_sign_match": False, "fp-r": {"delta_first": 0.0001}, "fpexe": {"delta_first": -0.0065}},
                            "RS": {"delta_ratio_abs": 0.015, "delta_sign_match": False, "fp-r": {"delta_first": 0.001}, "fpexe": {"delta_first": -0.065}},
                            "PCY": {"delta_ratio_abs": 0.015, "delta_sign_match": False, "fp-r": {"delta_first": 0.001}, "fpexe": {"delta_first": -0.065}},
                        },
                    },
                    {
                        "variant_id": "transfer-composite-medium",
                        "variables": {
                            "INTG": {"delta_ratio_abs": 0.0067, "delta_sign_match": True, "fp-r": {"delta_first": 0.05}, "fpexe": {"delta_first": 7.4}},
                            "JF": {"delta_ratio_abs": 0.0070, "delta_sign_match": True, "fp-r": {"delta_first": 0.01}, "fpexe": {"delta_first": 1.43}},
                            "YD": {"delta_ratio_abs": 0.0065, "delta_sign_match": True, "fp-r": {"delta_first": 0.325}, "fpexe": {"delta_first": 49.7}},
                            "GDPR": {"delta_ratio_abs": 0.0068, "delta_sign_match": True, "fp-r": {"delta_first": 0.125}, "fpexe": {"delta_first": 18.3}},
                            "RYDPC": {"delta_ratio_abs": 0.0065, "delta_sign_match": True, "fp-r": {"delta_first": 0.01}, "fpexe": {"delta_first": 1.53}},
                            "RS": {"delta_ratio_abs": 0.0071, "delta_sign_match": True, "fp-r": {"delta_first": 0.001}, "fpexe": {"delta_first": 0.14}},
                            "PCY": {"delta_ratio_abs": 0.0068, "delta_sign_match": True, "fp-r": {"delta_first": 0.001}, "fpexe": {"delta_first": 0.146}},
                            "UR": {"delta_ratio_abs": 0.0070, "delta_sign_match": True, "fp-r": {"delta_first": 0.0001}, "fpexe": {"delta_first": 0.014}},
                            "GDPD": {"delta_ratio_abs": 0.0070, "delta_sign_match": True, "fp-r": {"delta_first": 0.0001}, "fpexe": {"delta_first": 0.014}},
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    payload = analyze_phase1_distribution_canonical_solved_path(
        baseline_run_report_path=baseline_report,
        ui_relief_run_report_path=ui_relief_report,
        ui_shock_run_report_path=ui_shock_report,
        transfer_medium_run_report_path=transfer_report,
        first_levels_report_path=first_levels_report,
        report_path=tmp_path / "canonical-solved-path.json",
    )

    assert "UB" in payload["ui_relief_inside_solve_variables"]
    assert "INTG" in payload["transfer_inside_solve_variables"]
    report = json.loads((tmp_path / "canonical-solved-path.json").read_text(encoding="utf-8"))
    assert report["ui_shock"]["phase_gap_profile"]["variables"]["UB"]["pre_solve_gap"] == pytest.approx(0.0)
    assert report["ui_shock"]["phase_gap_profile"]["variables"]["UB"]["fp_r_final_gap_ratio_abs"] == pytest.approx(0.015)
    assert report["transfer_composite_medium"]["phase_gap_profile"]["variables"]["INTG"]["pre_solve_gap"] == pytest.approx(0.0)
    assert report["transfer_composite_medium"]["phase_gap_profile"]["variables"]["INTG"]["solve_added_gap"] == pytest.approx(0.05)
