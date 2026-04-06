from __future__ import annotations

import csv
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from fp_ineq.export import _phase1_solved_dictionary, _safe_dictionary_payload, _visible_export_series, export_phase1_full_bundle, publish_phase1_bundle_to_docs
from fp_ineq.phase1_catalog import phase1_scenario_by_variant


def test_visible_export_series_filters_controls_and_duplicate_public_names() -> None:
    visible = _visible_export_series(
        {
            "IPOVALL": [0.1, 0.2],
            "PV0": [1.0, 1.0],
            "UIFAC": [1.0, 1.0],
            "TFEDSHR": [1.0, 1.0],
            "PKGGROSS": [2.0, 3.0],
            "ITRCOMP": [0.3, 0.4],
            "RSAEFF": [1.0, 1.0],
            "IWG1050": [1.2, 1.3],
            "IWGAP1050": [1.2, 1.3],
            "GDPR": [100.0, 101.0],
        }
    )
    assert visible == ["IPOVALL", "GDPR"]


def test_phase1_solved_dictionary_backfills_unknown_variable_metadata() -> None:
    run_ids = ["ineq-baseline-observed", "ineq-ui-relief"]
    payload = _phase1_solved_dictionary(
        ["GDPR", "TRLOWZ", "PCPF", "ZZZFAKE"],
        bundle_run_ids=run_ids,
        variable_run_ids={name: run_ids for name in ["GDPR", "TRLOWZ", "PCPF", "ZZZFAKE"]},
    )
    assert payload["variables"]["GDPR"]["short_name"] == "Real GDP"
    assert payload["variables"]["GDPR"]["defined_by_equation"] == 83
    assert payload["variables"]["GDPR"]["source_runs"] == run_ids
    assert payload["variables"]["TRLOWZ"]["short_name"] == "Low-Income Transfer Bridge"
    assert str(payload["variables"]["PCPF"]["defined_by_equation"]).startswith("genr:PCPF:")
    assert "PF" in payload["variables"]["PCPF"]["description"]
    assert payload["variables"]["PCPF"]["source_runs"] == run_ids
    assert payload["variables"]["ZZZFAKE"]["short_name"] == "ZZZFAKE"
    assert payload["variables"]["ZZZFAKE"]["category"] == "model"
    assert payload["equations"]["83"]["source_runs"] == run_ids
    pcpf_eq_id = str(payload["variables"]["PCPF"]["defined_by_equation"])
    assert payload["equations"][pcpf_eq_id]["source_runs"] == run_ids
    assert "83" in payload["equations"]


def test_safe_dictionary_payload_falls_back_to_checked_in_docs_dictionary(
    tmp_path: Path, monkeypatch
) -> None:
    docs_root = tmp_path / "docs"
    docs_root.mkdir(parents=True, exist_ok=True)
    (docs_root / "dictionary.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "variables": {
                    "GDPR": {
                        "code": "GDPR",
                        "short_name": "Real GDP",
                        "description": "Fallback dictionary payload",
                        "defined_by_equation": "83",
                    }
                },
                "equations": {
                    "83": {
                        "lhs": "GDPR",
                        "formula": "GDPR=GDPR(-1);",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    overlay_root = tmp_path / "overlay" / "stock_fm"
    overlay_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "fp_ineq.export.repo_paths",
        lambda: SimpleNamespace(
            overlay_source_root=overlay_root,
            docs_root=docs_root,
        ),
    )
    monkeypatch.setattr("fp_ineq.export._stock_dictionary_path", lambda: tmp_path / "missing-stock.json")
    monkeypatch.setattr("fp_ineq.export._dictionary_base_path", lambda: tmp_path / "missing-base.json")

    payload = _safe_dictionary_payload()

    assert payload["variables"]["GDPR"]["short_name"] == "Real GDP"
    assert payload["variables"]["GDPR"]["description"] == "Fallback dictionary payload"
    assert payload["equations"]["83"]["lhs_expr"] == "GDPR"


def test_export_phase1_full_bundle_writes_broad_solved_payloads(
    tmp_path: Path, monkeypatch
) -> None:
    report_path = tmp_path / "run_phase1_distribution_block.json"
    report_path.write_text(
        json.dumps(
            {
                "track_variables": [
                    "IPOVALL",
                    "IPOVCH",
                    "IGINIHH",
                    "IMEDRINC",
                    "UB",
                    "TRGH",
                    "TRSH",
                    "YD",
                    "GDPR",
                    "UR",
                    "PCY",
                    "RS",
                ],
                "scenarios": {
                    "baseline-observed": {
                        "scenario_name": "ineq_phase1_distribution_baseline_observed",
                        "output_dir": "/tmp/ineq_phase1_distribution_baseline_observed_20260404_150000",
                        "loadformat_path": "/tmp/baseline/LOADFORMAT.DAT",
                    },
                    "ui-relief": {
                        "scenario_name": "ineq_phase1_distribution_ui_relief",
                        "output_dir": "/tmp/ineq_phase1_distribution_ui_relief_20260404_150005",
                        "loadformat_path": "/tmp/ui_relief/LOADFORMAT.DAT",
                    },
                    "ui-shock": {
                        "scenario_name": "ineq_phase1_distribution_ui_shock",
                        "output_dir": "/tmp/ineq_phase1_distribution_ui_shock_20260404_150007",
                        "loadformat_path": "/tmp/ui_shock/LOADFORMAT.DAT",
                    },
                    "ui-small": {
                        "scenario_name": "ineq_phase1_distribution_ui_small",
                        "output_dir": "/tmp/ineq_phase1_distribution_ui_small_20260404_150006",
                        "loadformat_path": "/tmp/ui_small/LOADFORMAT.DAT",
                    },
                    "ui-medium": {
                        "scenario_name": "ineq_phase1_distribution_ui_medium",
                        "output_dir": "/tmp/ineq_phase1_distribution_ui_medium_20260404_150006",
                        "loadformat_path": "/tmp/ui_medium/LOADFORMAT.DAT",
                    },
                    "ui-large": {
                        "scenario_name": "ineq_phase1_distribution_ui_large",
                        "output_dir": "/tmp/ineq_phase1_distribution_ui_large_20260404_150006",
                        "loadformat_path": "/tmp/ui_large/LOADFORMAT.DAT",
                    },
                    "federal-transfer-relief": {
                        "scenario_name": "ineq_phase1_distribution_federal_transfer_relief",
                        "output_dir": "/tmp/ineq_phase1_distribution_federal_transfer_relief_20260404_150008",
                        "loadformat_path": "/tmp/federal_transfer_relief/LOADFORMAT.DAT",
                    },
                    "federal-transfer-shock": {
                        "scenario_name": "ineq_phase1_distribution_federal_transfer_shock",
                        "output_dir": "/tmp/ineq_phase1_distribution_federal_transfer_shock_20260404_150009",
                        "loadformat_path": "/tmp/federal_transfer_shock/LOADFORMAT.DAT",
                    },
                    "state-local-transfer-relief": {
                        "scenario_name": "ineq_phase1_distribution_state_local_transfer_relief",
                        "output_dir": "/tmp/ineq_phase1_distribution_state_local_transfer_relief_20260404_150011",
                        "loadformat_path": "/tmp/state_local_transfer_relief/LOADFORMAT.DAT",
                    },
                    "state-local-transfer-shock": {
                        "scenario_name": "ineq_phase1_distribution_state_local_transfer_shock",
                        "output_dir": "/tmp/ineq_phase1_distribution_state_local_transfer_shock_20260404_150012",
                        "loadformat_path": "/tmp/state_local_transfer_shock/LOADFORMAT.DAT",
                    },
                    "transfer-package-relief": {
                        "scenario_name": "ineq_phase1_distribution_transfer_package_relief",
                        "output_dir": "/tmp/ineq_phase1_distribution_transfer_package_relief_20260404_150010",
                        "loadformat_path": "/tmp/relief/LOADFORMAT.DAT",
                    },
                    "transfer-package-shock": {
                        "scenario_name": "ineq_phase1_distribution_transfer_package_shock",
                        "output_dir": "/tmp/ineq_phase1_distribution_transfer_package_shock_20260404_150020",
                        "loadformat_path": "/tmp/shock/LOADFORMAT.DAT",
                    },
                    "transfer-composite-small": {
                        "scenario_name": "ineq_phase1_distribution_transfer_composite_small",
                        "output_dir": "/tmp/ineq_phase1_distribution_transfer_composite_small_20260404_150021",
                        "loadformat_path": "/tmp/transfer_composite_small/LOADFORMAT.DAT",
                    },
                    "transfer-composite-medium": {
                        "scenario_name": "ineq_phase1_distribution_transfer_composite_medium",
                        "output_dir": "/tmp/ineq_phase1_distribution_transfer_composite_medium_20260404_150022",
                        "loadformat_path": "/tmp/transfer_composite_medium/LOADFORMAT.DAT",
                    },
                    "transfer-composite-large": {
                        "scenario_name": "ineq_phase1_distribution_transfer_composite_large",
                        "output_dir": "/tmp/ineq_phase1_distribution_transfer_composite_large_20260404_150023",
                        "loadformat_path": "/tmp/transfer_composite_large/LOADFORMAT.DAT",
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_loadformat_window(
        loadformat_path: Path,
        *,
        variables: list[str] | None,
        forecast_start: str,
        forecast_end: str,
    ) -> tuple[list[str], dict[str, list[float]]]:
        assert variables is None
        label = loadformat_path.parts[-2]
        deltas = {
            "baseline": 0.0,
            "ui_relief": 0.25,
            "ui_shock": -0.25,
            "ui_small": 0.125,
            "ui_medium": 0.25,
            "ui_large": 0.375,
            "federal_transfer_relief": 0.5,
            "federal_transfer_shock": -0.5,
            "state_local_transfer_relief": 0.75,
            "state_local_transfer_shock": -0.75,
            "relief": 1.0,
            "shock": -1.0,
            "transfer_composite_small": 1.125,
            "transfer_composite_medium": 1.25,
            "transfer_composite_large": 1.375,
        }
        delta = deltas[label]
        periods = [
            "2026.1",
            "2026.2",
            "2026.3",
            "2026.4",
            "2027.1",
            "2027.2",
            "2027.3",
            "2027.4",
            "2028.1",
            "2028.2",
            "2028.3",
            "2028.4",
            "2029.1",
            "2029.2",
            "2029.3",
            "2029.4",
        ]
        assert periods[0] == forecast_start
        assert periods[-1] == forecast_end
        names = [
            "IPOVALL",
            "IPOVCH",
            "IGINIHH",
            "IMEDRINC",
            "UB",
            "TRGH",
            "TRSH",
            "YD",
            "GDPR",
            "UR",
            "PCY",
            "RS",
            "AS",
            "SGP",
            "TRLOWZ",
            "RYDPC",
            "ITRCOMP",
            "RSAEFF",
            "IHOMEQ",
            "LWGAP150",
        ]
        series = {
            name: [10.0 + delta + idx for idx, _period in enumerate(periods)]
            for name in names
        }
        return periods, series

    monkeypatch.setattr("fp_ineq.export._loadformat_window", fake_loadformat_window)
    monkeypatch.setattr("fp_ineq.export._copy_static_shell", lambda out_dir: None)

    out_dir = tmp_path / "bundle"
    payload = export_phase1_full_bundle(report_path=report_path, out_dir=out_dir)

    assert payload["run_count"] == 14
    assert payload["variable_count"] == 16
    assert payload["bridge_row_count"] == 27

    manifest = json.loads((out_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["default_run_ids"] == [
        "ineq-baseline-observed",
        "ineq-federal-transfer-relief",
        "ineq-federal-transfer-shock",
        "ineq-state-local-transfer-relief",
        "ineq-state-local-transfer-shock",
    ]
    assert manifest["default_preset_ids"] == ["headline-poverty-resources"]
    assert manifest["default_run_ids"] == [
        "ineq-baseline-observed",
        "ineq-federal-transfer-relief",
        "ineq-federal-transfer-shock",
        "ineq-state-local-transfer-relief",
        "ineq-state-local-transfer-shock",
    ]
    assert manifest["included_family_maturities"] == ["public"]
    assert manifest["included_family_ids"] == [
        "baseline",
        "ui",
        "federal-transfers",
        "state-local-transfers",
        "transfer-package",
        "transfer-composite",
    ]
    assert "public-default-safe fp-r results set" in manifest["run_panel_note"]
    assert "shared-modern labeling" in manifest["run_panel_note"]
    assert manifest["forecast_window_start"] == "2026.1"
    assert manifest["forecast_window_end"] == "2029.4"
    assert manifest["history_seeded_through"] == "2025.4"
    assert manifest["forecast_only_series"] == ["IPOVALL", "IPOVCH", "IGINIHH", "IMEDRINC"]
    assert "seeds history through 2025.4" in manifest["forecast_window_note"]
    assert manifest["bridge_results_path"] == "bridge_results.csv"
    assert manifest["bridge_metadata_path"] == "bridge_metadata.json"
    assert [item["family_id"] for item in manifest["families"]] == manifest["included_family_ids"]
    ui_family = next(item for item in manifest["families"] if item["family_id"] == "ui")
    composite_family = next(item for item in manifest["families"] if item["family_id"] == "transfer-composite")
    assert ui_family["run_ids"] == [
        "ineq-ui-relief",
        "ineq-ui-shock",
        "ineq-ui-small",
        "ineq-ui-large",
    ]
    assert composite_family["maturity"] == "public"
    assert composite_family["run_ids"] == [
        "ineq-transfer-composite-small",
        "ineq-transfer-composite-medium",
        "ineq-transfer-composite-large",
    ]
    assert "AS" in manifest["available_variables"]
    assert "SGP" in manifest["available_variables"]
    assert "TRLOWZ" in manifest["available_variables"]
    assert "RYDPC" in manifest["available_variables"]
    assert "ITRCOMP" not in manifest["available_variables"]
    assert "RSAEFF" not in manifest["available_variables"]
    assert "IHOMEQ" not in manifest["available_variables"]
    assert "LWGAP150" not in manifest["available_variables"]

    presets = json.loads((out_dir / "presets.json").read_text(encoding="utf-8"))
    preset_ids = [item["id"] for item in presets["presets"]]
    assert "headline-poverty-resources" in preset_ids
    assert "transfer-channels" in preset_ids
    assert "household-resources" in preset_ids
    assert "fiscal-closure" in preset_ids
    assert "provisional-distribution-diagnostics" in preset_ids
    household_resources = next(item for item in presets["presets"] if item["id"] == "household-resources")
    provisional = next(
        item for item in presets["presets"] if item["id"] == "provisional-distribution-diagnostics"
    )
    assert "IMEDRINC" not in household_resources["variables"]
    assert "IMEDRINC" in provisional["variables"]

    run_payload = json.loads((out_dir / "runs" / "ineq-transfer-composite-small.json").read_text(encoding="utf-8"))
    assert run_payload["periods"] == [
        "2026.1",
        "2026.2",
        "2026.3",
        "2026.4",
        "2027.1",
        "2027.2",
        "2027.3",
        "2027.4",
        "2028.1",
        "2028.2",
        "2028.3",
        "2028.4",
        "2029.1",
        "2029.2",
        "2029.3",
        "2029.4",
    ]
    assert run_payload["series"]["IPOVALL"][:3] == [11.125, 12.125, 13.125]
    assert run_payload["series"]["AS"][:3] == [11.125, 12.125, 13.125]
    assert run_payload["history_seeded_through"] == "2025.4"
    assert run_payload["forecast_only_series"] == ["IPOVALL", "IPOVCH", "IGINIHH", "IMEDRINC"]
    assert "ITRCOMP" not in run_payload["series"]
    assert run_payload["timestamp"] == "20260404_150021"

    bridge_metadata = json.loads((out_dir / "bridge_metadata.json").read_text(encoding="utf-8"))
    assert bridge_metadata["bridge_version"] == "v1"
    assert bridge_metadata["channels_by_family"] == {
        "federal-transfers": "broad_federal_transfers",
        "transfer-composite": "transfer_composite",
        "ui": "ui",
    }
    assert bridge_metadata["horizons"] == [2, 4, 8]
    assert bridge_metadata["headline_metrics"] == ["TRLOWZ", "IPOVALL", "IPOVCH", "RYDPC"]
    assert bridge_metadata["secondary_metrics"] == ["IGINIHH", "IMEDRINC"]

    with (out_dir / "bridge_results.csv").open(encoding="utf-8", newline="") as handle:
        bridge_rows = list(csv.DictReader(handle))
    assert len(bridge_rows) == 27
    sample = next(
        row
        for row in bridge_rows
        if row["scenario_id"] == "ineq-transfer-composite-medium" and row["h"] == "4"
    )
    assert sample["repo"] == "fp"
    assert sample["channel"] == "transfer_composite"
    assert sample["family"] == "transfer-composite"
    assert sample["baseline_id"] == "ineq-baseline-observed"
    assert sample["dose_metric"] == "delta_trlowz"
    assert float(sample["dose_value"]) == pytest.approx(1.25)
    assert float(sample["delta_ipovall"]) == pytest.approx(1.25)
    assert float(sample["delta_ipovch"]) == pytest.approx(1.25)
    assert float(sample["delta_rydpc"]) == pytest.approx(1.25)
    assert float(sample["delta_iginihh"]) == pytest.approx(1.25)
    assert float(sample["delta_imedrinc"]) == pytest.approx(1.25)
    assert sample["notes"] == "secondary_metrics_provisional; financed_transfer_package"

    dictionary = json.loads((out_dir / "dictionary.json").read_text(encoding="utf-8"))
    bundle_run_ids = [item["run_id"] for item in manifest["runs"]]
    for hidden in [
        "IWGAP150",
        "LWGAP150",
        "UBZ",
        "TRGHZ",
        "TRSHZ",
        "UIDEV",
        "GHSHDV",
        "TFEDSHR",
        "TSLSHR",
        "PKGGROSS",
        "PKGFIN",
        "PKGNET",
        "PKGGRZ",
        "PKGNETZ",
        "UBPOL",
        "TRGHPOL",
        "TRSHPOL",
        "TXPKGF",
        "TXPKGS",
    ]:
        assert hidden not in dictionary["variables"]
    assert dictionary["variables"]["AS"]["short_name"] == "State Local Net Assets"
    assert dictionary["variables"]["GDPR"]["defined_by_equation"] == 83
    assert dictionary["variables"]["GDPR"]["source_runs"] == bundle_run_ids
    assert str(dictionary["variables"]["PCPF"]["defined_by_equation"]).startswith("genr:PCPF:")
    assert "PF" in dictionary["variables"]["PCPF"]["description"]
    assert dictionary["equations"]["83"]["source_runs"] == bundle_run_ids
    pcpf_eq_id = str(dictionary["variables"]["PCPF"]["defined_by_equation"])
    assert dictionary["equations"][pcpf_eq_id]["source_runs"] == bundle_run_ids
    assert all("pse2025_" not in run_id for run_id in dictionary["equations"][pcpf_eq_id]["source_runs"])
    assert "83" in dictionary["equations"]
    assert "ITRCOMP" not in dictionary["variables"]


def test_export_phase1_full_bundle_supports_family_filtering(
    tmp_path: Path, monkeypatch
) -> None:
    report_path = tmp_path / "run_phase1_distribution_block.json"
    report_path.write_text(
        json.dumps(
            {
                "scenarios": {
                    "baseline-observed": {
                        "scenario_name": "ineq_phase1_distribution_baseline_observed",
                        "output_dir": "/tmp/ineq_phase1_distribution_baseline_observed_20260404_150000",
                        "loadformat_path": "/tmp/baseline/LOADFORMAT.DAT",
                    },
                    "transfer-composite-small": {
                        "scenario_name": "ineq_phase1_distribution_transfer_composite_small",
                        "output_dir": "/tmp/ineq_phase1_distribution_transfer_composite_small_20260404_150021",
                        "loadformat_path": "/tmp/transfer_composite_small/LOADFORMAT.DAT",
                    },
                    "transfer-composite-medium": {
                        "scenario_name": "ineq_phase1_distribution_transfer_composite_medium",
                        "output_dir": "/tmp/ineq_phase1_distribution_transfer_composite_medium_20260404_150022",
                        "loadformat_path": "/tmp/transfer_composite_medium/LOADFORMAT.DAT",
                    },
                    "transfer-composite-large": {
                        "scenario_name": "ineq_phase1_distribution_transfer_composite_large",
                        "output_dir": "/tmp/ineq_phase1_distribution_transfer_composite_large_20260404_150023",
                        "loadformat_path": "/tmp/transfer_composite_large/LOADFORMAT.DAT",
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_loadformat_window(
        loadformat_path: Path,
        *,
        variables: list[str] | None,
        forecast_start: str,
        forecast_end: str,
    ) -> tuple[list[str], dict[str, list[float]]]:
        assert variables is None
        return [forecast_start, forecast_end], {"IPOVALL": [1.0, 2.0], "TRLOWZ": [3.0, 4.0]}

    monkeypatch.setattr("fp_ineq.export._loadformat_window", fake_loadformat_window)
    monkeypatch.setattr("fp_ineq.export._copy_static_shell", lambda out_dir: None)

    out_dir = tmp_path / "bundle"
    payload = export_phase1_full_bundle(
        report_path=report_path,
        out_dir=out_dir,
        family_ids=("transfer-composite",),
    )

    assert payload["family_ids"] == ["transfer-composite"]
    manifest = json.loads((out_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["included_family_ids"] == ["transfer-composite"]
    assert manifest["history_seeded_through"] == "2025.4"
    assert manifest["default_run_ids"] == [
        "ineq-transfer-composite-small",
        "ineq-transfer-composite-medium",
        "ineq-transfer-composite-large",
    ]
    assert [item["run_id"] for item in manifest["runs"]] == [
        "ineq-transfer-composite-small",
        "ineq-transfer-composite-medium",
        "ineq-transfer-composite-large",
    ]
    assert manifest["bridge_results_path"] == "bridge_results.csv"
    assert payload["bridge_row_count"] == 0


def test_export_phase1_full_bundle_rejects_unknown_family_filter(tmp_path: Path) -> None:
    report_path = tmp_path / "run_phase1_distribution_block.json"
    report_path.write_text(json.dumps({"scenarios": {}}, indent=2), encoding="utf-8")

    with pytest.raises(ValueError, match="No public phase-1 scenarios matched"):
        export_phase1_full_bundle(
            report_path=report_path,
            out_dir=tmp_path / "bundle",
            family_ids=("not-a-family",),
        )


def test_publish_phase1_bundle_to_docs_replaces_existing_docs_bundle(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    docs_dir = tmp_path / "docs"
    source_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "manifest.json").write_text(
        json.dumps(
            {
                "available_variables": ["IPOVALL"],
                "default_run_ids": ["ineq-baseline-observed", "ineq-transfer-composite-small"],
                "included_family_ids": ["baseline", "transfer-composite"],
                "included_family_maturities": ["public"],
                "run_panel_note": "Default selection shows the published transfer-composite results set.",
                "runs": [
                    {
                        "run_id": "ineq-baseline-observed",
                        "family_id": "baseline",
                        "family_label": "Baseline",
                        "family_maturity": "public",
                        "data_path": "runs/ineq-baseline-observed.json",
                    },
                    {
                        "run_id": "ineq-transfer-composite-small",
                        "family_id": "transfer-composite",
                        "family_label": "Transfer Composite",
                        "family_maturity": "public",
                        "data_path": "runs/ineq-transfer-composite-small.json",
                    },
                ],
                "families": [
                    {
                        "family_id": "baseline",
                        "label": "Baseline",
                        "maturity": "public",
                        "run_ids": ["ineq-baseline-observed"],
                    },
                    {
                        "family_id": "transfer-composite",
                        "label": "Transfer Composite",
                        "maturity": "public",
                        "run_ids": ["ineq-transfer-composite-small"],
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    (source_dir / "dictionary.json").write_text("{}", encoding="utf-8")
    (source_dir / "presets.json").write_text("{}", encoding="utf-8")
    (source_dir / "bridge_results.csv").write_text("scenario_id,h\nineq-transfer-composite-small,2\n", encoding="utf-8")
    (source_dir / "bridge_metadata.json").write_text('{"bridge_version":"v1"}', encoding="utf-8")
    (source_dir / "app.js").write_text("", encoding="utf-8")
    (source_dir / "index.html").write_text("", encoding="utf-8")
    (source_dir / "styles.css").write_text("", encoding="utf-8")
    (source_dir / ".nojekyll").write_text("", encoding="utf-8")
    (source_dir / "runs").mkdir()
    (source_dir / "runs" / "ineq-baseline-observed.json").write_text("{}", encoding="utf-8")
    (source_dir / "runs" / "ineq-transfer-composite-small.json").write_text("{}", encoding="utf-8")
    (docs_dir / "runs").mkdir()
    (docs_dir / "runs" / "ineq-old-comparison.json").write_text("{}", encoding="utf-8")

    publish_phase1_bundle_to_docs(source_dir=source_dir, docs_dir=docs_dir)

    manifest = json.loads((docs_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["included_family_ids"] == ["baseline", "transfer-composite"]
    assert manifest["included_family_maturities"] == ["public"]
    assert [item["run_id"] for item in manifest["runs"]] == [
        "ineq-baseline-observed",
        "ineq-transfer-composite-small",
    ]
    assert (docs_dir / "bridge_results.csv").exists()
    assert (docs_dir / "bridge_metadata.json").exists()
    assert not (docs_dir / "runs" / "ineq-old-comparison.json").exists()


def test_checked_in_public_docs_match_repaired_default_bundle() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    readme = (repo_root / "README.md").read_text(encoding="utf-8")
    methodology = (repo_root / "reference" / "methodology.md").read_text(encoding="utf-8")
    manifest = json.loads((repo_root / "docs" / "manifest.json").read_text(encoding="utf-8"))
    scenarios = phase1_scenario_by_variant()

    expected_levels = {
        "transfer-composite-small": (1.0125839776982168, 1.2583977698216835, 1.0125839776982168),
        "transfer-composite-medium": (1.018637285379202, 1.8637285379202133, 1.018637285379202),
        "transfer-composite-large": (1.0225154841560722, 2.2515484156072145, 1.0225154841560722),
    }
    for variant_id, (ui_factor, trgh_delta_q, trsh_factor) in expected_levels.items():
        spec = scenarios[variant_id]
        assert spec.ui_factor == pytest.approx(ui_factor)
        assert spec.trgh_delta_q == pytest.approx(trgh_delta_q)
        assert spec.trsh_factor == pytest.approx(trsh_factor)
        assert str(ui_factor) in readme
        assert str(trgh_delta_q) in readme
        assert str(trsh_factor) in readme

    assert manifest["default_run_ids"] == [
        "ineq-baseline-observed",
        "ineq-federal-transfer-relief",
        "ineq-federal-transfer-shock",
        "ineq-state-local-transfer-relief",
        "ineq-state-local-transfer-shock",
    ]
    assert len(manifest["runs"]) == 14
    assert manifest["included_family_ids"] == [
        "baseline",
        "ui",
        "federal-transfers",
        "state-local-transfers",
        "transfer-package",
        "transfer-composite",
    ]
    assert manifest["included_family_maturities"] == ["public"]
    lowered_readme = readme.lower()
    lowered_methodology = methodology.lower()
    manifest_text = (repo_root / "docs" / "manifest.json").read_text(encoding="utf-8").lower()
    assert "legacy" not in lowered_readme
    assert "phase-1" not in lowered_readme
    assert "phase1" not in lowered_readme
    assert "legacy" not in lowered_methodology
    assert "phase-1" not in lowered_methodology
    assert "phase1" not in lowered_methodology
    assert "legacy split" in manifest_text
    assert "shared modern" in manifest_text
    assert "phase-1" not in manifest_text
    assert "phase1" not in manifest_text
