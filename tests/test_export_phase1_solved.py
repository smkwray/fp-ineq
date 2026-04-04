from __future__ import annotations

import json
from pathlib import Path

from fp_ineq.export import _phase1_solved_dictionary, _visible_export_series, export_phase1_full_bundle


def test_visible_export_series_filters_controls_and_duplicate_public_names() -> None:
    visible = _visible_export_series(
        {
            "IPOVALL": [0.1, 0.2],
            "PV0": [1.0, 1.0],
            "UIFAC": [1.0, 1.0],
            "ITRCOMP": [0.3, 0.4],
            "RSAEFF": [1.0, 1.0],
            "IWG1050": [1.2, 1.3],
            "IWGAP1050": [1.2, 1.3],
            "GDPR": [100.0, 101.0],
        }
    )
    assert visible == ["IPOVALL", "GDPR"]


def test_phase1_solved_dictionary_backfills_unknown_variable_metadata() -> None:
    run_ids = ["ineq-phase1-baseline-observed", "ineq-phase1-ui-relief"]
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
                    "snap-relief": {
                        "scenario_name": "ineq_phase1_distribution_snap_relief",
                        "output_dir": "/tmp/ineq_phase1_distribution_snap_relief_20260404_150008",
                        "loadformat_path": "/tmp/snap_relief/LOADFORMAT.DAT",
                    },
                    "snap-shock": {
                        "scenario_name": "ineq_phase1_distribution_snap_shock",
                        "output_dir": "/tmp/ineq_phase1_distribution_snap_shock_20260404_150009",
                        "loadformat_path": "/tmp/snap_shock/LOADFORMAT.DAT",
                    },
                    "social-security-relief": {
                        "scenario_name": "ineq_phase1_distribution_social_security_relief",
                        "output_dir": "/tmp/ineq_phase1_distribution_social_security_relief_20260404_150011",
                        "loadformat_path": "/tmp/ss_relief/LOADFORMAT.DAT",
                    },
                    "social-security-shock": {
                        "scenario_name": "ineq_phase1_distribution_social_security_shock",
                        "output_dir": "/tmp/ineq_phase1_distribution_social_security_shock_20260404_150012",
                        "loadformat_path": "/tmp/ss_shock/LOADFORMAT.DAT",
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
            "snap_relief": 0.5,
            "snap_shock": -0.5,
            "ss_relief": 0.75,
            "ss_shock": -0.75,
            "relief": 1.0,
            "shock": -1.0,
        }
        delta = deltas[label]
        periods = [forecast_start, "2026.2", forecast_end]
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
        ]
        series = {
            name: [10.0 + delta, 11.0 + delta, 12.0 + delta]
            for name in names
        }
        return periods, series

    monkeypatch.setattr("fp_ineq.export._loadformat_window", fake_loadformat_window)
    monkeypatch.setattr("fp_ineq.export._copy_static_shell", lambda out_dir: None)

    out_dir = tmp_path / "bundle"
    payload = export_phase1_full_bundle(report_path=report_path, out_dir=out_dir)

    assert payload["run_count"] == 9
    assert payload["variable_count"] == 16

    manifest = json.loads((out_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["default_run_ids"] == [
        "ineq-phase1-baseline-observed",
        "ineq-phase1-ui-relief",
        "ineq-phase1-ui-shock",
        "ineq-phase1-snap-relief",
        "ineq-phase1-snap-shock",
        "ineq-phase1-social-security-relief",
        "ineq-phase1-social-security-shock",
        "ineq-phase1-transfer-package-relief",
        "ineq-phase1-transfer-package-shock",
    ]
    assert manifest["default_preset_ids"] == ["headline-poverty-resources"]
    assert [item["run_id"] for item in manifest["runs"]] == manifest["default_run_ids"]
    assert "AS" in manifest["available_variables"]
    assert "SGP" in manifest["available_variables"]
    assert "TRLOWZ" in manifest["available_variables"]
    assert "RYDPC" in manifest["available_variables"]
    assert "ITRCOMP" not in manifest["available_variables"]
    assert "RSAEFF" not in manifest["available_variables"]
    assert "IHOMEQ" not in manifest["available_variables"]

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

    run_payload = json.loads(
        (out_dir / "runs" / "ineq-phase1-transfer-package-relief.json").read_text(encoding="utf-8")
    )
    assert run_payload["periods"] == ["2026.1", "2026.2", "2029.4"]
    assert run_payload["series"]["IPOVALL"] == [11.0, 12.0, 13.0]
    assert run_payload["series"]["AS"] == [11.0, 12.0, 13.0]
    assert "ITRCOMP" not in run_payload["series"]
    assert run_payload["timestamp"] == "20260404_150010"

    dictionary = json.loads((out_dir / "dictionary.json").read_text(encoding="utf-8"))
    assert dictionary["variables"]["AS"]["short_name"] == "State Local Net Assets"
    assert dictionary["variables"]["GDPR"]["defined_by_equation"] == 83
    assert dictionary["variables"]["GDPR"]["source_runs"] == manifest["default_run_ids"]
    assert str(dictionary["variables"]["PCPF"]["defined_by_equation"]).startswith("genr:PCPF:")
    assert "PF" in dictionary["variables"]["PCPF"]["description"]
    assert dictionary["equations"]["83"]["source_runs"] == manifest["default_run_ids"]
    pcpf_eq_id = str(dictionary["variables"]["PCPF"]["defined_by_equation"])
    assert dictionary["equations"][pcpf_eq_id]["source_runs"] == manifest["default_run_ids"]
    assert all("pse2025_" not in run_id for run_id in dictionary["equations"][pcpf_eq_id]["source_runs"])
    assert "83" in dictionary["equations"]
    assert "ITRCOMP" in dictionary["variables"]
