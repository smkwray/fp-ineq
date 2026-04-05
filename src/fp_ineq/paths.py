from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

__all__ = ["RepoPaths", "repo_paths"]


@dataclass(frozen=True)
class RepoPaths:
    repo_root: Path
    data_root: Path
    data_series_root: Path
    data_reports_root: Path
    overlay_source_root: Path
    runtime_root: Path
    runtime_overlay_root: Path
    runtime_bundle_root: Path
    runtime_artifacts_root: Path
    runtime_phase1_root: Path
    runtime_phase1_overlay_root: Path
    runtime_phase1_scenarios_root: Path
    runtime_phase1_artifacts_root: Path
    runtime_phase1_reports_root: Path
    runtime_transfer_root: Path
    runtime_transfer_scenarios_root: Path
    runtime_transfer_artifacts_root: Path
    runtime_transfer_reports_root: Path
    runtime_distribution_root: Path
    runtime_distribution_overlay_root: Path
    runtime_distribution_scenarios_root: Path
    runtime_distribution_artifacts_root: Path
    runtime_distribution_reports_root: Path
    runtime_credit_root: Path
    runtime_credit_overlay_root: Path
    runtime_credit_scenarios_root: Path
    runtime_credit_artifacts_root: Path
    runtime_credit_reports_root: Path
    runtime_ui_offset_root: Path
    runtime_ui_offset_overlay_root: Path
    runtime_ui_offset_scenarios_root: Path
    runtime_ui_offset_artifacts_root: Path
    runtime_ui_offset_reports_root: Path
    runtime_solved_public_root: Path
    docs_root: Path
    specs_root: Path
    reference_root: Path


def repo_paths() -> RepoPaths:
    repo_root = Path(__file__).resolve().parents[2]
    data_root = repo_root / "data"
    runtime_root = repo_root / "runtime"
    phase1_root = runtime_root / "phase1_ui"
    transfer_root = runtime_root / "phase1_transfer_core"
    distribution_root = runtime_root / "phase1_distribution_block"
    credit_root = runtime_root / "phase2_credit_family"
    ui_offset_root = runtime_root / "phase2_ui_offset_family"
    solved_public_root = runtime_root / "phase1_solved_public"
    return RepoPaths(
        repo_root=repo_root,
        data_root=data_root,
        data_series_root=data_root / "series",
        data_reports_root=data_root / "reports",
        overlay_source_root=repo_root / "overlay" / "stock_fm",
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
        docs_root=repo_root / "docs",
        specs_root=repo_root / "specs",
        reference_root=repo_root / "reference",
    )
