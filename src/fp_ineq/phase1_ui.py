from __future__ import annotations

import json
import math
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
from .scenario_ladders import (
    FIRST_YEAR_TRLOWZ_PERIODS,
    UI_LADDER_RUNGS,
    build_relative_ladder_targets,
    mean_delta_over_periods,
    solve_linear_lever_value,
)

__all__ = [
    "PHASE1_UI_SCENARIOS",
    "_apply_manifest_patches",
    "_load_phase1_manifest",
    "build_phase1_private_overlay",
    "run_phase1_ui_ladder",
    "run_phase1_ui_prototype",
    "write_phase1_ui_ladder_scenarios",
    "write_phase1_ui_scenarios",
]


PHASE1_UI_SCENARIOS = (
    ("baseline-observed", 1.00, "Baseline integrated UI prototype."),
    ("ui-relief", 1.02, "Higher UI generosity through the stock UB channel."),
    ("ui-shock", 0.98, "Lower UI generosity through the stock UB channel."),
)

_PHASE1_TRACK_VARIABLES = [
    "UB",
    "TRGH",
    "TRSH",
    "POP",
    "PH",
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
_UI_LADDER_TRIAL_VARIANT = "ui-relief"
_UI_LADDER_TOLERANCE = 0.02
_UI_LADDER_MAX_PASSES = 5


@dataclass(frozen=True)
class Phase1ScenarioSpec:
    variant_id: str
    ui_factor: float
    description: str


def _ui_ladder_paths() -> tuple[Path, Path, Path]:
    paths = repo_paths()
    root = paths.runtime_phase1_root / "ladder"
    return root / "scenarios", root / "artifacts", root / "reports"


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


def _write_ui_scenarios(
    *,
    fp_home: Path,
    scenario_specs: list[Phase1ScenarioSpec],
    scenarios_root: Path,
    artifacts_root: Path,
) -> list[Path]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.scenarios.config import ScenarioConfig

    paths = repo_paths()
    fp_home = locate_fp_home(fp_home)
    build_phase1_private_overlay(fp_home=fp_home)
    scenarios_root.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    overlay_dir = paths.runtime_phase1_overlay_root.resolve()
    for spec in scenario_specs:
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
            artifacts_root=str(artifacts_root),
        )
        path = scenarios_root / f"{spec.variant_id}.yaml"
        config.to_yaml(path)
        written.append(path)
    return written


def _scenario_name(variant_id: str) -> str:
    return f"ineq_phase1_ui_{variant_id.replace('-', '_')}"


def write_phase1_ui_scenarios(*, fp_home: Path) -> list[Path]:
    paths = repo_paths()
    return _write_ui_scenarios(
        fp_home=fp_home,
        scenario_specs=_phase1_specs(),
        scenarios_root=paths.runtime_phase1_scenarios_root,
        artifacts_root=paths.runtime_phase1_artifacts_root,
    )


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
    _periods, series = _read_loadformat_payload(loadformat_path)
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


def _is_missing_loadformat_value(value: float | None) -> bool:
    if value is None:
        return True
    numeric = float(value)
    if math.isnan(numeric):
        return True
    return abs(numeric + 99.0) <= 1e-9


def _merge_missing_series_values(
    primary: list[float] | None,
    fallback: list[float] | None,
) -> list[float] | None:
    if not fallback:
        return primary
    if not primary:
        return [float(value) for value in fallback]
    merged = [float(value) for value in primary]
    limit = min(len(merged), len(fallback))
    for idx in range(limit):
        if _is_missing_loadformat_value(merged[idx]) and not _is_missing_loadformat_value(fallback[idx]):
            merged[idx] = float(fallback[idx])
    return merged


def _repair_loadformat_series(series: dict[str, list[float]]) -> dict[str, list[float]]:
    repaired = {name: [float(value) for value in values] for name, values in series.items()}
    derived_trlowz = _derive_trlowz_series(repaired)
    if derived_trlowz is not None:
        repaired["TRLOWZ"] = _merge_missing_series_values(repaired.get("TRLOWZ"), derived_trlowz)
    derived_rydpc = _derive_rydpc_series(repaired)
    if derived_rydpc is not None:
        repaired["RYDPC"] = _merge_missing_series_values(repaired.get("RYDPC"), derived_rydpc)
    return repaired


def _read_loadformat_series(loadformat_path: Path) -> dict[str, list[float]]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.io.loadformat import add_derived_series, read_loadformat

    _periods, series = read_loadformat(loadformat_path)
    return _repair_loadformat_series(add_derived_series(series))


def _derive_trlowz_series(series: dict[str, list[float]]) -> list[float] | None:
    required = ("UB", "TRGH", "TRSH", "POP", "PH")
    if not all(name in series for name in required):
        return None
    ub = series["UB"]
    trgh = series["TRGH"]
    trsh = series["TRSH"]
    pop = series["POP"]
    ph = series["PH"]
    n = min(len(ub), len(trgh), len(trsh), len(pop), len(ph))
    trlowz: list[float] = []
    for idx in range(n):
        denom = float(pop[idx]) * float(ph[idx])
        if abs(denom) <= 1e-12:
            raise ValueError("Cannot derive TRLOWZ from LOADFORMAT output with zero POP*PH denominator")
        trlowz.append((float(ub[idx]) + float(trgh[idx]) + float(trsh[idx])) / denom)
    return trlowz


def _derive_rydpc_series(series: dict[str, list[float]]) -> list[float] | None:
    required = ("YD", "POP", "PH")
    if not all(name in series for name in required):
        return None
    yd = series["YD"]
    pop = series["POP"]
    ph = series["PH"]
    n = min(len(yd), len(pop), len(ph))
    rydpc: list[float] = []
    for idx in range(n):
        denom = float(pop[idx]) * float(ph[idx])
        if abs(denom) <= 1e-12:
            raise ValueError("Cannot derive RYDPC from LOADFORMAT output with zero POP*PH denominator")
        rydpc.append(float(yd[idx]) / denom)
    return rydpc


def _read_loadformat_payload(loadformat_path: Path) -> tuple[list[str], dict[str, list[float]]]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.io.loadformat import add_derived_series, read_loadformat

    periods, series = read_loadformat(loadformat_path)
    return list(periods), _repair_loadformat_series(add_derived_series(series))


def _trial_ui_factor() -> float:
    for spec in _phase1_specs():
        if spec.variant_id == _UI_LADDER_TRIAL_VARIANT:
            return spec.ui_factor
    raise ValueError(f"UI ladder trial variant not found: {_UI_LADDER_TRIAL_VARIANT}")


def _ui_ladder_specs_from_trial_delta(trial_delta: float) -> tuple[list[Phase1ScenarioSpec], dict[str, dict[str, float]]]:
    targets = build_relative_ladder_targets(trial_metric=trial_delta, rung_specs=UI_LADDER_RUNGS)
    neutral_ui_factor = 1.0
    trial_ui_factor = _trial_ui_factor()
    specs = [Phase1ScenarioSpec("baseline-observed", neutral_ui_factor, "Baseline integrated UI ladder run.")]
    calibration: dict[str, dict[str, float]] = {}
    for target in targets:
        ui_factor = solve_linear_lever_value(
            neutral_lever=neutral_ui_factor,
            trial_lever=trial_ui_factor,
            trial_metric=trial_delta,
            target_metric=target.target_metric,
        )
        specs.append(
            Phase1ScenarioSpec(
                target.variant_id,
                ui_factor,
                (
                    "Matched UI ladder rung calibrated to the mean first-year "
                    f"ΔTRLOWZ target of {target.target_metric:.12g}."
                ),
            )
        )
        calibration[target.variant_id] = {
            "scale": target.scale,
            "target_mean_delta_trlowz": target.target_metric,
            "ui_factor": ui_factor,
        }
    return specs, calibration


def _write_ui_ladder_calibration_report(
    *,
    reports_root: Path,
    prototype_report_path: Path,
    calibration: dict[str, object],
    scenario_paths: list[Path],
    passes_completed: int | None = None,
) -> Path:
    reports_root.mkdir(parents=True, exist_ok=True)
    calibration_payload = {
        "prototype_report_path": str(prototype_report_path),
        "metric": calibration["metric"],
        "target_periods": calibration["target_periods"],
        "trial_variant": calibration["trial_variant"],
        "trial_ui_factor": calibration["trial_ui_factor"],
        "trial_mean_delta_trlowz": calibration["trial_mean_delta_trlowz"],
        "rungs": calibration["rungs"],
        "scenario_paths": [str(path) for path in scenario_paths],
    }
    if passes_completed is not None:
        calibration_payload["passes_completed"] = passes_completed
    report_path = reports_root / "calibrate_phase1_ui_ladder.json"
    report_path.write_text(json.dumps(calibration_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report_path


def _mean_trlowz_delta_from_loadformats(*, baseline_path: Path, scenario_path: Path) -> float:
    baseline_periods, baseline_series = _read_loadformat_payload(baseline_path)
    scenario_periods, scenario_series = _read_loadformat_payload(scenario_path)
    if baseline_periods != scenario_periods:
        raise ValueError("Baseline and scenario periods do not match for UI ladder calibration")
    baseline_values = baseline_series.get("TRLOWZ")
    scenario_values = scenario_series.get("TRLOWZ")
    if not baseline_values or not scenario_values:
        raise KeyError("TRLOWZ series missing from loadformat output")
    return mean_delta_over_periods(
        periods=baseline_periods,
        baseline_values=baseline_values,
        scenario_values=scenario_values,
        target_periods=FIRST_YEAR_TRLOWZ_PERIODS,
    )


def _collect_ui_ladder_points(
    *,
    calibration: dict[str, object],
    rung_results: dict[str, dict[str, float | bool]] | None = None,
) -> list[tuple[float, float]]:
    points: dict[float, float] = {
        1.0: 0.0,
        float(calibration["trial_ui_factor"]): float(calibration["trial_mean_delta_trlowz"]),
    }
    for detail in (rung_results or {}).values():
        points[float(detail["ui_factor"])] = float(detail["achieved_mean_delta_trlowz"])
    return sorted(points.items(), key=lambda item: item[1])


def _interpolate_ui_factor_for_target(
    *,
    points: list[tuple[float, float]],
    target_metric: float,
) -> float:
    if len(points) < 2:
        raise ValueError("At least two calibration points are required")
    sorted_points = sorted(points, key=lambda item: item[1])
    for factor, metric in sorted_points:
        if abs(metric - target_metric) <= 1e-12:
            return factor
    lower = sorted_points[0]
    upper = sorted_points[-1]
    for left, right in zip(sorted_points, sorted_points[1:]):
        if left[1] <= target_metric <= right[1]:
            lower, upper = left, right
            break
    else:
        if target_metric < sorted_points[0][1]:
            lower, upper = sorted_points[0], sorted_points[1]
        elif target_metric > sorted_points[-1][1]:
            lower, upper = sorted_points[-2], sorted_points[-1]
    metric_span = upper[1] - lower[1]
    if abs(metric_span) <= 1e-12:
        raise ValueError("Cannot interpolate UI factor from duplicate calibration metrics")
    weight = (target_metric - lower[1]) / metric_span
    return lower[0] + weight * (upper[0] - lower[0])


def _refined_ui_ladder_specs(
    *,
    calibration: dict[str, object],
    rung_results: dict[str, dict[str, float | bool]],
) -> tuple[list[Phase1ScenarioSpec], dict[str, object]]:
    points = _collect_ui_ladder_points(calibration=calibration, rung_results=rung_results)
    specs = [Phase1ScenarioSpec("baseline-observed", 1.0, "Baseline integrated UI ladder run.")]
    updated_rungs: dict[str, dict[str, float]] = {}
    for variant_id, rung in dict(calibration["rungs"]).items():
        target = float(dict(rung)["target_mean_delta_trlowz"])
        ui_factor = _interpolate_ui_factor_for_target(points=points, target_metric=target)
        specs.append(
            Phase1ScenarioSpec(
                variant_id,
                ui_factor,
                (
                    "Matched UI ladder rung calibrated to the mean first-year "
                    f"ΔTRLOWZ target of {target:.12g}."
                ),
            )
        )
        updated_rungs[variant_id] = {
            "scale": float(dict(rung)["scale"]),
            "target_mean_delta_trlowz": target,
            "ui_factor": ui_factor,
        }
    return specs, {
        **calibration,
        "rungs": updated_rungs,
    }


def _ui_ladder_specs_from_report(report_path: Path) -> tuple[list[Phase1ScenarioSpec], dict[str, object]]:
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    scenarios = dict(payload.get("scenarios", {}))
    baseline = dict(scenarios.get("baseline-observed", {}))
    trial = dict(scenarios.get(_UI_LADDER_TRIAL_VARIANT, {}))
    baseline_path = Path(str(baseline.get("loadformat_path", "") or ""))
    trial_path = Path(str(trial.get("loadformat_path", "") or ""))
    if not baseline_path.exists():
        raise FileNotFoundError("UI ladder calibration requires a baseline loadformat_path from run_phase1_ui")
    if not trial_path.exists():
        raise FileNotFoundError(
            f"UI ladder calibration requires a loadformat_path for {_UI_LADDER_TRIAL_VARIANT} from run_phase1_ui"
        )
    trial_delta = _mean_trlowz_delta_from_loadformats(baseline_path=baseline_path, scenario_path=trial_path)
    specs, calibration = _ui_ladder_specs_from_trial_delta(trial_delta)
    return specs, {
        "metric": "mean_first_year_delta_trlowz",
        "target_periods": list(FIRST_YEAR_TRLOWZ_PERIODS),
        "trial_variant": _UI_LADDER_TRIAL_VARIANT,
        "trial_ui_factor": _trial_ui_factor(),
        "trial_mean_delta_trlowz": trial_delta,
        "baseline_loadformat_path": str(baseline_path),
        "trial_loadformat_path": str(trial_path),
        "rungs": calibration,
    }


def write_phase1_ui_ladder_scenarios(*, fp_home: Path, prototype_report_path: Path | None = None) -> list[Path]:
    fp_home = locate_fp_home(fp_home)
    if prototype_report_path is None:
        prototype_payload = run_phase1_ui_prototype(fp_home=fp_home)
        prototype_report_path = Path(str(prototype_payload["report_path"]))
    specs, calibration = _ui_ladder_specs_from_report(prototype_report_path)
    scenarios_root, artifacts_root, reports_root = _ui_ladder_paths()
    written = _write_ui_scenarios(
        fp_home=fp_home,
        scenario_specs=specs,
        scenarios_root=scenarios_root,
        artifacts_root=artifacts_root,
    )
    _write_ui_ladder_calibration_report(
        reports_root=reports_root,
        prototype_report_path=prototype_report_path,
        calibration=calibration,
        scenario_paths=written,
        passes_completed=1,
    )
    return written


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


def _run_ui_scenarios(
    *,
    scenario_paths: list[Path],
    artifacts_root: Path,
    report_path: Path,
    scenarios_dir: Path,
) -> dict[str, object]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.scenarios.runner import load_scenario_config, run_scenario

    paths = repo_paths()
    artifacts_root.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    run_payloads: dict[str, dict[str, object]] = {}
    last_levels: dict[str, dict[str, float | None]] = {}
    for scenario_path in scenario_paths:
        variant_id = scenario_path.stem
        config = load_scenario_config(scenario_path)
        result = run_scenario(
            config=config,
            output_dir=artifacts_root,
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
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "overlay_root": str(paths.runtime_phase1_overlay_root),
        "scenarios_dir": str(scenarios_dir),
        "artifacts_dir": str(artifacts_root),
        "passes": bool(acceptance["passes"]),
        "required_moves": acceptance["required_moves"],
        "one_of_moves": acceptance["one_of_moves"],
    }


def run_phase1_ui_prototype(*, fp_home: Path) -> dict[str, object]:
    paths = repo_paths()
    scenario_paths = write_phase1_ui_scenarios(fp_home=fp_home)
    return _run_ui_scenarios(
        scenario_paths=scenario_paths,
        artifacts_root=paths.runtime_phase1_artifacts_root,
        report_path=paths.runtime_phase1_reports_root / "run_phase1_ui.json",
        scenarios_dir=paths.runtime_phase1_scenarios_root,
    )


def _evaluate_ui_ladder_run(
    *,
    calibration: dict[str, object],
    run_report: dict[str, object],
) -> tuple[dict[str, dict[str, float | bool]], bool]:
    baseline_loadformat_path = Path(str(dict(run_report["scenarios"])["baseline-observed"]["loadformat_path"]))
    rung_results: dict[str, dict[str, float | bool]] = {}
    for variant_id, rung in dict(calibration.get("rungs", {})).items():
        scenario_loadformat_path = Path(str(dict(run_report["scenarios"])[variant_id]["loadformat_path"]))
        achieved = _mean_trlowz_delta_from_loadformats(
            baseline_path=baseline_loadformat_path,
            scenario_path=scenario_loadformat_path,
        )
        target = float(dict(rung)["target_mean_delta_trlowz"])
        relative_error = abs(achieved - target) / abs(target) if abs(target) > 1e-12 else 0.0
        rung_results[variant_id] = {
            "ui_factor": float(dict(rung)["ui_factor"]),
            "target_mean_delta_trlowz": target,
            "achieved_mean_delta_trlowz": achieved,
            "relative_error": relative_error,
            "passes_target": relative_error <= _UI_LADDER_TOLERANCE,
        }
    passes_targets = all(detail["passes_target"] for detail in rung_results.values())
    return rung_results, passes_targets


def run_phase1_ui_ladder(*, fp_home: Path, prototype_report_path: Path | None = None) -> dict[str, object]:
    fp_home = locate_fp_home(fp_home)
    scenario_paths = write_phase1_ui_ladder_scenarios(
        fp_home=fp_home,
        prototype_report_path=prototype_report_path,
    )
    scenarios_root, artifacts_root, reports_root = _ui_ladder_paths()
    calibration_path = reports_root / "calibrate_phase1_ui_ladder.json"
    calibration = json.loads(calibration_path.read_text(encoding="utf-8"))
    payload: dict[str, object] | None = None
    run_report: dict[str, object] | None = None
    rung_results: dict[str, dict[str, float | bool]] = {}
    passes_targets = False
    passes_completed = 0

    for pass_index in range(1, _UI_LADDER_MAX_PASSES + 1):
        payload = _run_ui_scenarios(
            scenario_paths=scenario_paths,
            artifacts_root=artifacts_root,
            report_path=reports_root / "run_phase1_ui_ladder_raw.json",
            scenarios_dir=scenarios_root,
        )
        run_report = json.loads(Path(str(payload["report_path"])).read_text(encoding="utf-8"))
        rung_results, passes_targets = _evaluate_ui_ladder_run(
            calibration=calibration,
            run_report=run_report,
        )
        passes_completed = pass_index
        if passes_targets or pass_index == _UI_LADDER_MAX_PASSES:
            break
        refined_specs, calibration = _refined_ui_ladder_specs(
            calibration=calibration,
            rung_results=rung_results,
        )
        scenario_paths = _write_ui_scenarios(
            fp_home=fp_home,
            scenario_specs=refined_specs,
            scenarios_root=scenarios_root,
            artifacts_root=artifacts_root,
        )
        _write_ui_ladder_calibration_report(
            reports_root=reports_root,
            prototype_report_path=Path(str(calibration["prototype_report_path"])),
            calibration=calibration,
            scenario_paths=scenario_paths,
            passes_completed=pass_index + 1,
        )

    assert payload is not None
    assert run_report is not None
    _write_ui_ladder_calibration_report(
        reports_root=reports_root,
        prototype_report_path=Path(str(calibration["prototype_report_path"])),
        calibration=calibration,
        scenario_paths=scenario_paths,
        passes_completed=passes_completed,
    )
    ladder_report = {
        "prototype_report_path": calibration["prototype_report_path"],
        "metric": calibration["metric"],
        "target_periods": calibration["target_periods"],
        "trial_variant": calibration["trial_variant"],
        "trial_ui_factor": calibration["trial_ui_factor"],
        "trial_mean_delta_trlowz": calibration["trial_mean_delta_trlowz"],
        "passes_completed": passes_completed,
        "scenarios": run_report["scenarios"],
        "acceptance": run_report["acceptance"],
        "track_variables": run_report["track_variables"],
        "rungs": rung_results,
        "passes_targets": passes_targets,
        "passes": bool(run_report["acceptance"]["passes"]) and passes_targets,
        "scenario_paths": [str(path) for path in scenario_paths],
    }
    ladder_report_path = reports_root / "run_phase1_ui_ladder.json"
    ladder_report_path.write_text(json.dumps(ladder_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(ladder_report_path),
        "overlay_root": payload["overlay_root"],
        "scenarios_dir": str(scenarios_root),
        "artifacts_dir": str(artifacts_root),
        "passes": bool(ladder_report["passes"]),
        "passes_targets": passes_targets,
    }
