from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from .paths import repo_paths
from .phase1_catalog import phase1_distribution_specs

__all__ = ["assess_phase1_canonical_freeze"]


_CANONICAL_REMOTE_REF = "origin/main"
_EXPERIMENTAL_PATH_PREFIXES = (
    "specs/phase1_distribution_interventions",
    "runtime/phase1_distribution_block/interventions",
)
_SCENARIO_SURFACE_PATHS = {
    "phase1_catalog": "src/fp_ineq/phase1_catalog.py",
    "phase1_transfer_core": "src/fp_ineq/phase1_transfer_core.py",
    "phase1_ui": "src/fp_ineq/phase1_ui.py",
    "phase1_distribution_block": "src/fp_ineq/phase1_distribution_block.py",
}


def _git(repo_root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo_root), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _path_exists_in_ref(repo_root: Path, ref: str, path: str) -> bool:
    output = _git(repo_root, "ls-tree", "-r", "--name-only", ref, "--", path)
    return any(line.strip() == path for line in output.splitlines())


def _changed_against_ref(repo_root: Path, ref: str, path: str) -> bool:
    output = _git(repo_root, "diff", "--name-only", ref, "--", path)
    return any(line.strip() == path for line in output.splitlines())


def _untracked_paths(repo_root: Path, pathspec: str) -> list[str]:
    output = _git(repo_root, "ls-files", "--others", "--exclude-standard", "--", pathspec)
    return [line.strip() for line in output.splitlines() if line.strip()]


def assess_phase1_canonical_freeze(
    *,
    remote_ref: str = _CANONICAL_REMOTE_REF,
    report_path: Path | None = None,
) -> dict[str, Any]:
    paths = repo_paths()
    repo_root = paths.repo_root

    scenario_surface: dict[str, Any] = {}
    for label, relpath in _SCENARIO_SURFACE_PATHS.items():
        exists_in_ref = _path_exists_in_ref(repo_root, remote_ref, relpath)
        changed = _changed_against_ref(repo_root, remote_ref, relpath) if exists_in_ref else True
        scenario_surface[label] = {
            "path": relpath,
            "exists_in_canonical_ref": exists_in_ref,
            "changed_against_canonical_ref": changed,
        }

    experimental_paths: dict[str, list[str]] = {}
    for prefix in _EXPERIMENTAL_PATH_PREFIXES:
        experimental_paths[prefix] = _untracked_paths(repo_root, prefix)

    public_distribution_variants = [spec.variant_id for spec in phase1_distribution_specs()]
    phase1_catalog_preserved = not bool(scenario_surface["phase1_catalog"]["changed_against_canonical_ref"])
    transfer_core_preserved = not bool(scenario_surface["phase1_transfer_core"]["changed_against_canonical_ref"])
    ui_surface_changed = bool(scenario_surface["phase1_ui"]["changed_against_canonical_ref"])
    distribution_surface_changed = bool(scenario_surface["phase1_distribution_block"]["changed_against_canonical_ref"])

    freeze_summary = {
        "canonical_distribution_catalog_preserved": phase1_catalog_preserved,
        "canonical_transfer_core_preserved": transfer_core_preserved,
        "public_distribution_variants": public_distribution_variants,
        "experimental_intervention_specs_present": bool(experimental_paths["specs/phase1_distribution_interventions"]),
        "experimental_intervention_artifacts_present": bool(
            experimental_paths["runtime/phase1_distribution_block/interventions"]
        ),
        "default_freeze_rule": (
            "Use only canonical phase1_catalog scenarios and prohibit intervention specs, "
            "equation-term overrides, and runtime text appends for final fp-r parity claims."
        ),
        "notes": [
            (
                "phase1_catalog.py matches canonical public origin/main, so the public distribution "
                "variant definitions themselves are preserved."
            )
            if phase1_catalog_preserved
            else (
                "phase1_catalog.py differs from canonical public origin/main and must be audited before "
                "claiming scenario parity."
            ),
            (
                "phase1_transfer_core.py matches canonical public origin/main, so the public transfer-core "
                "scenario surface is preserved."
            )
            if transfer_core_preserved
            else (
                "phase1_transfer_core.py differs from canonical public origin/main and must be audited before "
                "claiming scenario parity."
            ),
            (
                "phase1_ui.py differs from canonical public origin/main, but the current drift is in loadformat/"
                "report extraction helpers rather than public scenario parameter definitions."
            )
            if ui_surface_changed
            else "phase1_ui.py matches canonical public origin/main.",
            (
                "phase1_distribution_block.py differs heavily from canonical public origin/main; treat its "
                "intervention/holdout/readiness additions as validation infrastructure, not canonical scenario definitions."
            )
            if distribution_surface_changed
            else "phase1_distribution_block.py matches canonical public origin/main.",
        ],
    }

    payload = {
        "canonical_remote_ref": remote_ref,
        "scenario_surface": scenario_surface,
        "experimental_paths": experimental_paths,
        "freeze_summary": freeze_summary,
    }
    if report_path is None:
        report_path = paths.runtime_distribution_reports_root / "assess_phase1_canonical_freeze.json"
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    Path(report_path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "canonical_distribution_catalog_preserved": phase1_catalog_preserved,
        "canonical_transfer_core_preserved": transfer_core_preserved,
        "experimental_intervention_specs_present": bool(experimental_paths["specs/phase1_distribution_interventions"]),
        "experimental_intervention_artifacts_present": bool(
            experimental_paths["runtime/phase1_distribution_block/interventions"]
        ),
    }
