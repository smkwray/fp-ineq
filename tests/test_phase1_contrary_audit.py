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
                        "transfer-composite-small": {
                            "TRLOWZ": 0.2,
                            "UR": -0.001,
                            "GDPR": -10.0,
                            "YD": -9.0,
                            "RS": 0.1,
                            "IPOVALL": -0.01,
                            "IPOVCH": -0.02,
                        },
                        "transfer-composite-medium": {
                            "TRLOWZ": 0.25,
                            "UR": -0.0015,
                            "GDPR": 8.0,
                            "YD": -7.0,
                            "RS": 0.08,
                            "IPOVALL": -0.012,
                            "IPOVCH": -0.021,
                        },
                        "transfer-composite-large": {
                            "TRLOWZ": 0.3,
                            "UR": -0.0018,
                            "GDPR": 12.0,
                            "YD": -5.0,
                            "RS": 0.05,
                            "IPOVALL": -0.013,
                            "IPOVCH": -0.023,
                        },
                    },
                    "private_package_gates": {
                        "transfer-composite-small": {
                            "passes": True,
                            "diagnostics": {"package_balance_ok": True},
                            "selected_levels": {"PKGGROSS": 22.0, "PKGFIN": 19.1, "PKGNET": 2.9},
                            "selected_deltas": {"PKGGROSS": 1.0, "PKGFIN": 0.8, "PKGNET": 0.2},
                        },
                        "transfer-composite-medium": {
                            "passes": False,
                            "diagnostics": {"package_balance_ok": False},
                            "selected_levels": {"PKGGROSS": 21.0, "PKGFIN": 19.5, "PKGNET": 1.2},
                            "selected_deltas": {"PKGGROSS": 0.5, "PKGFIN": 0.4, "PKGNET": -0.1},
                        },
                        "transfer-composite-large": {
                            "passes": True,
                            "diagnostics": {"package_balance_ok": True},
                            "selected_levels": {"PKGGROSS": 23.0, "PKGFIN": 20.0, "PKGNET": 3.0},
                            "selected_deltas": {"PKGGROSS": 1.3, "PKGFIN": 1.0, "PKGNET": 0.3},
                        },
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
    assert "baseline" in written["families"]
    assert "transfer-composite" in written["families"]
    assert written["families"]["transfer-composite"]["scenario_count"] == 3
    assert written["families"]["transfer-composite"]["private_package_gate_passes"] == 2
    assert written["scenarios"]["transfer-composite-small"]["private_package_gate"]["passes"] is True
    assert written["ui_offset_context"]["offset_has_lower_gdpr"] is False
    assert "baseline plus transfer-composite ladder" in written["summary"]
    assert "Public ladder" in Path(payload["markdown_path"]).read_text(encoding="utf-8")
    assert "private fallback" in written["recommendations"]["private_fallback"]
