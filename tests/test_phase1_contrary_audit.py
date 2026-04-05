from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from fp_ineq.phase1_contrary_audit import assess_phase1_contrary_channels


def test_assess_phase1_contrary_channels_writes_family_summary(tmp_path, monkeypatch) -> None:
    runtime_reports_root = tmp_path / "runtime" / "phase1_distribution_block" / "reports"
    runtime_reports_root.mkdir(parents=True, exist_ok=True)
    report_path = runtime_reports_root / "run_phase1_distribution_block.json"
    report_path.write_text(
        json.dumps(
            {
                "acceptance": {
                    "comparisons": {
                        "baseline-observed": {},
                        "ui-relief": {"TRLOWZ": 0.2, "UR": -0.001, "GDPR": 10.0, "YD": 9.0, "RS": 0.1, "IPOVALL": -0.01, "IPOVCH": -0.02},
                        "ui-shock": {"TRLOWZ": -0.2, "UR": 0.001, "GDPR": -10.0, "YD": -9.0, "RS": -0.1, "IPOVALL": 0.01, "IPOVCH": 0.02},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    ui_offset_report_path = tmp_path / "runtime" / "phase2_ui_offset_family" / "reports" / "run_phase2_ui_offset.json"
    ui_offset_report_path.parent.mkdir(parents=True, exist_ok=True)
    ui_offset_report_path.write_text(
        json.dumps(
            {
                "metrics": {
                    "target_clawback_share": 0.25,
                    "clawback_share": 0.249,
                    "trlowz_relative_gap": 0.001,
                },
                "acceptance": {
                    "diagnostics": {
                        "offset_has_lower_gdpr": False,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "fp_ineq.phase1_contrary_audit.repo_paths",
        lambda: SimpleNamespace(
            runtime_distribution_reports_root=runtime_reports_root,
            runtime_ui_offset_reports_root=ui_offset_report_path.parent,
        ),
    )

    payload = assess_phase1_contrary_channels(
        report_path=report_path,
        ui_offset_report_path=ui_offset_report_path,
    )

    written = json.loads(Path(payload["report_path"]).read_text(encoding="utf-8"))
    assert "ui" in written["families"]
    assert written["families"]["ui"]["scenario_count"] == 4
    assert written["ui_offset_context"]["offset_has_lower_gdpr"] is False
    assert "Do not add synthetic contrary families" in written["recommendations"]["non_ui"]

