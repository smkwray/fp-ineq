from __future__ import annotations

import csv
import json
from pathlib import Path
from types import SimpleNamespace

from fp_ineq.discrepancy_inventory import build_phase1_bridge_discrepancy_inventory


def _write_bridge_surface(root: Path, *, rows: list[dict[str, object]], report_name: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "bridge_version",
        "repo",
        "scenario_id",
        "scenario_label",
        "channel",
        "family",
        "h",
        "baseline_id",
        "dose_metric",
        "dose_value",
        "delta_trlowz",
        "delta_ipovall",
        "delta_ipovch",
        "delta_rydpc",
        "delta_iginihh",
        "delta_imedrinc",
        "notes",
    ]
    with (root / "bridge_results.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    (root / "bridge_metadata.json").write_text(
        json.dumps({"bridge_version": "v1", "repo": "fp", "row_count": len(rows)}, indent=2) + "\n",
        encoding="utf-8",
    )
    (root / "bridge_export_report.json").write_text(
        json.dumps({"report_path": f"runtime/{report_name}.json", "bridge_row_count": len(rows)}, indent=2) + "\n",
        encoding="utf-8",
    )


def test_build_phase1_bridge_discrepancy_inventory_tracks_identical_and_divergent_pairs(
    tmp_path: Path, monkeypatch
) -> None:
    reports_root = tmp_path / "reports"
    base_rows = [
        {
            "bridge_version": "v1",
            "repo": "fp",
            "scenario_id": "ineq-ui-relief",
            "scenario_label": "UI Medium",
            "channel": "ui",
            "family": "ui",
            "h": 2,
            "baseline_id": "ineq-baseline-observed",
            "dose_metric": "delta_trlowz",
            "dose_value": 0.15,
            "delta_trlowz": 0.15,
            "delta_ipovall": -0.003,
            "delta_ipovch": -0.007,
            "delta_rydpc": 0.17,
            "delta_iginihh": 0.002,
            "delta_imedrinc": 0.25,
            "notes": "secondary_metrics_provisional",
        },
        {
            "bridge_version": "v1",
            "repo": "fp",
            "scenario_id": "ineq-transfer-composite-medium",
            "scenario_label": "Transfer Composite Medium",
            "channel": "transfer_composite",
            "family": "transfer-composite",
            "h": 4,
            "baseline_id": "ineq-baseline-observed",
            "dose_metric": "delta_trlowz",
            "dose_value": 0.151,
            "delta_trlowz": 0.151,
            "delta_ipovall": -0.0029,
            "delta_ipovch": -0.0071,
            "delta_rydpc": 0.155,
            "delta_iginihh": 0.002,
            "delta_imedrinc": 0.228,
            "notes": "secondary_metrics_provisional; financed_transfer_package",
        },
    ]
    remote_rows = [
        dict(base_rows[0]),
        {
            **dict(base_rows[1]),
            "delta_trlowz": 0.023,
            "delta_ipovall": -0.0002,
            "delta_ipovch": -0.0006,
            "delta_rydpc": -0.011,
            "delta_iginihh": 0.0002,
            "delta_imedrinc": -0.019,
        },
    ]

    _write_bridge_surface(reports_root / "phase1_distribution_block", rows=base_rows, report_name="run_phase1_distribution_block")
    _write_bridge_surface(
        reports_root / "phase1_distribution_block_fpexe",
        rows=base_rows,
        report_name="run_phase1_distribution_block_fpexe",
    )
    _write_bridge_surface(
        reports_root / "phase1_distribution_block_fprremote2",
        rows=remote_rows,
        report_name="run_phase1_distribution_block_fprremote2",
    )

    monkeypatch.setattr(
        "fp_ineq.discrepancy_inventory.repo_paths",
        lambda: SimpleNamespace(repo_root=tmp_path),
    )

    payload = build_phase1_bridge_discrepancy_inventory(
        out_json_path=reports_root / "phase1_bridge_discrepancy_inventory.json",
        out_md_path=reports_root / "phase1_bridge_discrepancy_inventory.md",
    )

    comparisons = {item["comparison_id"]: item for item in payload["comparisons"]}
    assert comparisons["tracked_default_vs_fpexe"]["identical_shared_rows"] is True
    assert comparisons["tracked_default_vs_fpexe"]["family_summary"] == {}

    remote = comparisons["tracked_default_vs_fprremote2"]
    assert remote["identical_shared_rows"] is False
    assert remote["family_summary"]["transfer-composite"]["row_difference_count"] == 1
    assert remote["family_summary"]["transfer-composite"]["sign_flip_metrics"] == ["delta_rydpc", "delta_imedrinc"]
    assert remote["row_differences"][0]["scenario_id"] == "ineq-transfer-composite-medium"

    actual_fp_r = comparisons["fprremote2_vs_fpexe"]
    assert actual_fp_r["identical_shared_rows"] is False
    assert actual_fp_r["family_summary"]["transfer-composite"]["row_difference_count"] == 1

    markdown = (reports_root / "phase1_bridge_discrepancy_inventory.md").read_text(encoding="utf-8")
    assert "tracked_default_vs_fpexe" in markdown
    assert "No differing rows." in markdown
    assert "tracked_default_vs_fprremote2" in markdown
    assert "fprremote2_vs_fpexe" in markdown
    assert "transfer-composite" in markdown
