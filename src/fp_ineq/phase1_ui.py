from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .bridge import ensure_fp_wraptr_importable, locate_fp_home
from .paths import repo_paths
from .phase1_compose import (
    _apply_manifest_patches,
    _load_phase1_manifest,
    compose_phase1_overlay,
)

__all__ = [
    "PHASE1_UI_SCENARIOS",
    "build_phase1_private_overlay",
    "run_phase1_ui_prototype",
    "write_phase1_ui_scenarios",
]


PHASE1_UI_SCENARIOS = (
    ("baseline-observed", 1.00, "Baseline integrated UI prototype."),
    ("ui-relief", 1.02, "Higher UI generosity through the stock UB channel."),
    ("ui-shock", 0.98, "Lower UI generosity through the stock UB channel."),
)

_PHASE1_TRACK_VARIABLES = [
    "UB",
    "YD",
    "GDPR",
    "UR",
    "PCY",
    "PIEF",
    "SG",
    "EXPG",
    "RS",
    "RB",
    "RM",
    "SH",
    "AH",
]
_PHASE1_REQUIRED_MOVES = ("UB", "YD", "GDPR")
_PHASE1_ONE_OF_MOVES = ("UR", "PCY")
_PHASE1_FLAT_TAIL_CHECKS = ("YD", "GDPR", "UR", "RS")


@dataclass(frozen=True)
class Phase1ScenarioSpec:
    variant_id: str
    ui_factor: float
    description: str


def build_phase1_private_overlay(*, fp_home: Path) -> dict[str, object]:
    paths = repo_paths()
    fp_home = locate_fp_home(fp_home)
    overlay_root = paths.runtime_phase1_overlay_root
    compose_payload = compose_phase1_overlay(
        fp_home=fp_home,
        overlay_root=overlay_root,
    )
    build_report = {
        "fp_home": str(fp_home),
        "overlay_root": str(overlay_root),
        "entry_files": compose_payload["entry_files"],
        "patch_ids": compose_payload["patch_ids"],
    }
    report_path = paths.runtime_phase1_reports_root / "compose_phase1_ui.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(build_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return build_report


def _phase1_specs() -> list[Phase1ScenarioSpec]:
    return [Phase1ScenarioSpec(*item) for item in PHASE1_UI_SCENARIOS]


def _scenario_name(variant_id: str) -> str:
    return f"ineq_phase1_ui_{variant_id.replace('-', '_')}"


def write_phase1_ui_scenarios(*, fp_home: Path) -> list[Path]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.scenarios.config import ScenarioConfig

    paths = repo_paths()
    fp_home = locate_fp_home(fp_home)
    build_phase1_private_overlay(fp_home=fp_home)
    paths.runtime_phase1_scenarios_root.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    overlay_dir = paths.runtime_phase1_overlay_root.resolve()
    for spec in _phase1_specs():
        input_patches: dict[str, str] = {}
        if abs(spec.ui_factor - 1.0) > 1e-12:
            input_patches["CREATE UIFAC=1;"] = f"CREATE UIFAC={spec.ui_factor:.12g};"
        config = ScenarioConfig(
            name=_scenario_name(spec.variant_id),
            description=spec.description,
            fp_home=fp_home,
            input_overlay_dir=overlay_dir,
            input_file="fminput.txt",
            forecast_start="2026.1",
            forecast_end="2029.4",
            backend="fpexe",
            track_variables=list(_PHASE1_TRACK_VARIABLES),
            input_patches=input_patches,
            artifacts_root=str(paths.runtime_phase1_artifacts_root),
        )
        path = paths.runtime_phase1_scenarios_root / f"{spec.variant_id}.yaml"
        config.to_yaml(path)
        written.append(path)
    return written


def _extract_last_levels(parsed_output: object, variables: list[str]) -> dict[str, float | None]:
    if parsed_output is None:
        return {name: None for name in variables}
    available = getattr(parsed_output, "variables", {})
    out: dict[str, float | None] = {}
    for name in variables:
        payload = available.get(name)
        levels = getattr(payload, "levels", None)
        out[name] = float(levels[-1]) if levels else None
    return out


def _extract_first_levels(parsed_output: object, variables: list[str]) -> dict[str, float | None]:
    if parsed_output is None:
        return {name: None for name in variables}
    available = getattr(parsed_output, "variables", {})
    out: dict[str, float | None] = {}
    for name in variables:
        payload = available.get(name)
        levels = getattr(payload, "levels", None)
        out[name] = float(levels[0]) if levels else None
    return out


def _extract_levels_from_loadformat(
    loadformat_path: Path,
    variables: list[str],
) -> tuple[dict[str, float | None], dict[str, float | None]]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.io.loadformat import add_derived_series, read_loadformat

    _periods, series = read_loadformat(loadformat_path)
    series = add_derived_series(series)
    first_levels: dict[str, float | None] = {}
    last_levels: dict[str, float | None] = {}
    for name in variables:
        values = series.get(name)
        if not values:
            first_levels[name] = None
            last_levels[name] = None
            continue
        first_levels[name] = float(values[0])
        last_levels[name] = float(values[-1])
    return first_levels, last_levels


def _movement_summary(results: dict[str, dict[str, float | None]]) -> dict[str, Any]:
    baseline = results.get("baseline-observed", {})
    comparisons: dict[str, dict[str, float | None]] = {}
    moved_any_required = {name: False for name in _PHASE1_REQUIRED_MOVES}
    moved_any_one_of = {name: False for name in _PHASE1_ONE_OF_MOVES}
    tolerance = 1e-9

    for variant_id, values in results.items():
        if variant_id == "baseline-observed":
            continue
        delta_map: dict[str, float | None] = {}
        for name in _PHASE1_TRACK_VARIABLES:
            base_value = baseline.get(name)
            scenario_value = values.get(name)
            if base_value is None or scenario_value is None:
                delta_map[name] = None
                continue
            delta = float(scenario_value - base_value)
            delta_map[name] = delta
            if name in moved_any_required and abs(delta) > tolerance:
                moved_any_required[name] = True
            if name in moved_any_one_of and abs(delta) > tolerance:
                moved_any_one_of[name] = True
        comparisons[variant_id] = delta_map

    return {
        "comparisons": comparisons,
        "required_moves": moved_any_required,
        "one_of_moves": moved_any_one_of,
        "passes_core": all(moved_any_required.values()) and any(moved_any_one_of.values()),
    }


def _read_loadformat_series(loadformat_path: Path) -> dict[str, list[float]]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.io.loadformat import add_derived_series, read_loadformat

    _periods, series = read_loadformat(loadformat_path)
    return add_derived_series(series)


def _flat_tail_flags(series: dict[str, list[float]], variables: tuple[str, ...], *, window: int = 8) -> dict[str, bool]:
    flags: dict[str, bool] = {}
    for name in variables:
        values = series.get(name)
        if not values:
            flags[name] = False
            continue
        tail = values[-window:]
        flags[name] = len({round(float(value), 10) for value in tail}) <= 1
    return flags


def run_phase1_ui_prototype(*, fp_home: Path) -> dict[str, object]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.scenarios.runner import load_scenario_config, run_scenario

    paths = repo_paths()
    scenario_paths = write_phase1_ui_scenarios(fp_home=fp_home)
    paths.runtime_phase1_artifacts_root.mkdir(parents=True, exist_ok=True)
    paths.runtime_phase1_reports_root.mkdir(parents=True, exist_ok=True)

    run_payloads: dict[str, dict[str, object]] = {}
    last_levels: dict[str, dict[str, float | None]] = {}
    for scenario_path in scenario_paths:
        variant_id = scenario_path.stem
        config = load_scenario_config(scenario_path)
        result = run_scenario(
            config=config,
            output_dir=paths.runtime_phase1_artifacts_root,
        )
        fmout_path = result.output_dir / "fmout.txt"
        fmout_text = fmout_path.read_text(encoding="utf-8", errors="replace") if fmout_path.exists() else ""
        solve_error_sol1 = "Solution error in SOL1." in fmout_text
        loadformat_path = result.output_dir / "LOADFORMAT.DAT"
        if loadformat_path.exists():
            loadformat_series = _read_loadformat_series(loadformat_path)
            first_levels, variant_last_levels = _extract_levels_from_loadformat(
                loadformat_path,
                _PHASE1_TRACK_VARIABLES,
            )
            flat_tail_flags = _flat_tail_flags(loadformat_series, _PHASE1_FLAT_TAIL_CHECKS)
        else:
            loadformat_series = {}
            first_levels = _extract_first_levels(result.parsed_output, _PHASE1_TRACK_VARIABLES)
            variant_last_levels = _extract_last_levels(result.parsed_output, _PHASE1_TRACK_VARIABLES)
            flat_tail_flags = {name: False for name in _PHASE1_FLAT_TAIL_CHECKS}
        last_levels[variant_id] = variant_last_levels
        run_payloads[variant_id] = {
            "scenario_name": config.name,
            "description": config.description,
            "success": bool(result.success),
            "output_dir": str(result.output_dir),
            "loadformat_path": str(loadformat_path) if loadformat_path.exists() else None,
            "return_code": (
                int(result.run_result.return_code) if result.run_result is not None else None
            ),
            "solve_error_sol1": solve_error_sol1,
            "flat_tail_flags": flat_tail_flags,
            "first_levels": first_levels,
            "last_levels": variant_last_levels,
            "backend_diagnostics": result.backend_diagnostics,
        }

    acceptance = _movement_summary(last_levels)
    unhealthy = {
        variant_id: {
            "solve_error_sol1": bool(payload["solve_error_sol1"]),
            "flat_tail_vars": [
                name for name, flagged in dict(payload["flat_tail_flags"]).items() if flagged
            ],
        }
        for variant_id, payload in run_payloads.items()
        if variant_id != "baseline-observed"
    }
    passes_health = all(
        (not detail["solve_error_sol1"]) and (not detail["flat_tail_vars"])
        for detail in unhealthy.values()
    )
    acceptance["scenario_health"] = unhealthy
    acceptance["passes_health"] = passes_health
    acceptance["passes"] = bool(acceptance["passes_core"]) and passes_health
    payload = {
        "scenarios": run_payloads,
        "acceptance": acceptance,
        "track_variables": list(_PHASE1_TRACK_VARIABLES),
        "scenario_paths": [str(path) for path in scenario_paths],
    }
    report_path = paths.runtime_phase1_reports_root / "run_phase1_ui.json"
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "overlay_root": str(paths.runtime_phase1_overlay_root),
        "scenarios_dir": str(paths.runtime_phase1_scenarios_root),
        "artifacts_dir": str(paths.runtime_phase1_artifacts_root),
        "passes": bool(acceptance["passes"]),
        "required_moves": acceptance["required_moves"],
        "one_of_moves": acceptance["one_of_moves"],
    }
