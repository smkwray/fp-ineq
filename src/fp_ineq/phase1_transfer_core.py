from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .bridge import ensure_fp_wraptr_importable, locate_fp_home
from .paths import repo_paths
from .phase1_catalog import phase1_transfer_core_specs
from .phase1_ui import (
    _PHASE1_FLAT_TAIL_CHECKS,
    _extract_first_levels,
    _extract_last_levels,
    _extract_levels_from_loadformat,
    _flat_tail_flags,
    _mean_trlowz_delta_from_loadformats,
    _read_loadformat_series,
    build_phase1_private_overlay,
)

__all__ = [
    "PHASE1_TRANSFER_SCENARIOS",
    "run_phase1_transfer_composite_ladder",
    "run_phase1_transfer_core",
    "write_phase1_transfer_composite_ladder_scenarios",
    "write_phase1_transfer_scenarios",
]


PHASE1_TRANSFER_SCENARIOS = tuple(
    (
        spec.variant_id,
        spec.ui_factor,
        spec.trgh_delta_q,
        spec.trsh_factor,
        spec.transfer_description,
    )
    for spec in phase1_transfer_core_specs()
)

_TRANSFER_TRACK_VARIABLES = [
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
_TRANSFER_ONE_OF_MOVES = ("UR", "PCY")
_TRANSFER_COMPOSITE_TRIAL_VARIANT = "transfer-package-relief"
_TRANSFER_COMPOSITE_TOLERANCE = 0.02
_TRANSFER_COMPOSITE_MAX_PASSES = 5
_TRANSFER_COMPOSITE_RUNG_IDS = (
    ("transfer-composite-small", 0.5),
    ("transfer-composite-medium", 1.0),
    ("transfer-composite-large", 1.5),
)
_TRANSFER_COMPOSITE_UI_STEP = 0.02
_TRANSFER_COMPOSITE_SNAP_STEP = 2.0
_TRANSFER_COMPOSITE_SS_STEP = 0.02


@dataclass(frozen=True)
class TransferScenarioSpec:
    variant_id: str
    ui_factor: float
    trgh_delta_q: float
    trsh_factor: float
    description: str


def _transfer_ladder_paths() -> tuple[Path, Path, Path]:
    paths = repo_paths()
    root = paths.runtime_transfer_root / "ladder"
    return root / "scenarios", root / "artifacts", root / "reports"


def _transfer_specs() -> list[TransferScenarioSpec]:
    return [TransferScenarioSpec(*item) for item in PHASE1_TRANSFER_SCENARIOS]


def _write_transfer_scenarios(
    *,
    fp_home: Path,
    scenario_specs: list[TransferScenarioSpec],
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
        if abs(spec.trgh_delta_q) > 1e-12:
            input_patches["CREATE SNAPDELTAQ=0;"] = f"CREATE SNAPDELTAQ={spec.trgh_delta_q:.12g};"
        if abs(spec.trsh_factor - 1.0) > 1e-12:
            input_patches["CREATE SSFAC=1;"] = f"CREATE SSFAC={spec.trsh_factor:.12g};"
        config = ScenarioConfig(
            name=_scenario_name(spec.variant_id),
            description=spec.description,
            fp_home=fp_home,
            input_overlay_dir=overlay_dir,
            input_file="fminput.txt",
            forecast_start="2026.1",
            forecast_end="2029.4",
            backend="fpexe",
            track_variables=list(_TRANSFER_TRACK_VARIABLES),
            input_patches=input_patches,
            artifacts_root=str(artifacts_root),
        )
        path = scenarios_root / f"{spec.variant_id}.yaml"
        config.to_yaml(path)
        written.append(path)
    return written


def _scenario_name(variant_id: str) -> str:
    return f"ineq_phase1_transfer_{variant_id.replace('-', '_')}"


def _required_moves_for_variant(variant_id: str) -> tuple[str, ...]:
    if variant_id.startswith("ui-"):
        return ("UB", "YD", "GDPR")
    if variant_id.startswith("federal-transfer-"):
        return ("TRGH", "YD", "GDPR")
    if variant_id.startswith("state-local-transfer-"):
        return ("TRSH", "YD", "GDPR")
    if variant_id.startswith("transfer-package-") or variant_id.startswith("transfer-composite-"):
        return ("UB", "TRGH", "TRSH", "YD", "GDPR")
    raise ValueError(f"Unsupported transfer-core variant: {variant_id}")


def _expected_sign_for_variant(variant_id: str) -> float:
    if variant_id.endswith("-relief"):
        return 1.0
    if variant_id.endswith("-shock"):
        return -1.0
    if variant_id.startswith("transfer-composite-"):
        return 1.0
    raise ValueError(f"Unsupported transfer-core variant: {variant_id}")


def write_phase1_transfer_scenarios(*, fp_home: Path) -> list[Path]:
    paths = repo_paths()
    return _write_transfer_scenarios(
        fp_home=fp_home,
        scenario_specs=_transfer_specs(),
        scenarios_root=paths.runtime_transfer_scenarios_root,
        artifacts_root=paths.runtime_transfer_artifacts_root,
    )


def _acceptance_summary(results: dict[str, dict[str, float | None]]) -> dict[str, Any]:
    baseline = results.get("baseline-observed", {})
    tolerance = 1e-9
    comparisons: dict[str, dict[str, float | None]] = {}
    scenario_checks: dict[str, dict[str, Any]] = {}

    for variant_id, values in results.items():
        if variant_id == "baseline-observed":
            continue
        required = _required_moves_for_variant(variant_id)
        expected_sign = _expected_sign_for_variant(variant_id)
        required_moves = {name: False for name in required}
        required_signs = {name: False for name in required}
        one_of_moves = {name: False for name in _TRANSFER_ONE_OF_MOVES}
        one_of_signs = {name: False for name in _TRANSFER_ONE_OF_MOVES}
        delta_map: dict[str, float | None] = {}
        for name in _TRANSFER_TRACK_VARIABLES:
            base_value = baseline.get(name)
            scenario_value = values.get(name)
            if base_value is None or scenario_value is None:
                delta_map[name] = None
                continue
            delta = float(scenario_value - base_value)
            delta_map[name] = delta
            if name in required_moves and abs(delta) > tolerance:
                required_moves[name] = True
            if name in required_signs and (delta * expected_sign) > tolerance:
                required_signs[name] = True
            if name in one_of_moves and abs(delta) > tolerance:
                one_of_moves[name] = True
            if name == "UR" and (delta * -expected_sign) > tolerance:
                one_of_signs[name] = True
            if name == "PCY" and (delta * expected_sign) > tolerance:
                one_of_signs[name] = True
        comparisons[variant_id] = delta_map
        scenario_checks[variant_id] = {
            "required_moves": required_moves,
            "required_signs": required_signs,
            "one_of_moves": one_of_moves,
            "one_of_signs": one_of_signs,
            "passes_core": (
                all(required_moves.values())
                and all(required_signs.values())
                and any(one_of_moves.values())
                and any(one_of_signs.values())
            ),
        }

    return {
        "comparisons": comparisons,
        "scenario_checks": scenario_checks,
        "passes_core": all(detail["passes_core"] for detail in scenario_checks.values()),
    }


def _run_transfer_scenarios(
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
                _TRANSFER_TRACK_VARIABLES,
            )
            flat_tail_flags = _flat_tail_flags(loadformat_series, _PHASE1_FLAT_TAIL_CHECKS)
        else:
            first_levels = _extract_first_levels(result.parsed_output, _TRANSFER_TRACK_VARIABLES)
            variant_last_levels = _extract_last_levels(result.parsed_output, _TRANSFER_TRACK_VARIABLES)
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

    acceptance = _acceptance_summary(last_levels)
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
        "track_variables": list(_TRANSFER_TRACK_VARIABLES),
        "scenario_paths": [str(path) for path in scenario_paths],
    }
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "overlay_root": str(paths.runtime_phase1_overlay_root),
        "scenarios_dir": str(scenarios_dir),
        "artifacts_dir": str(artifacts_root),
        "passes": bool(acceptance["passes"]),
        "passes_core": bool(acceptance["passes_core"]),
        "passes_health": bool(acceptance["passes_health"]),
    }


def run_phase1_transfer_core(*, fp_home: Path) -> dict[str, object]:
    paths = repo_paths()
    scenario_paths = write_phase1_transfer_scenarios(fp_home=fp_home)
    return _run_transfer_scenarios(
        scenario_paths=scenario_paths,
        artifacts_root=paths.runtime_transfer_artifacts_root,
        report_path=paths.runtime_transfer_reports_root / "run_phase1_transfer_core.json",
        scenarios_dir=paths.runtime_transfer_scenarios_root,
    )


def _transfer_composite_trial_levers() -> tuple[float, float, float]:
    for spec in _transfer_specs():
        if spec.variant_id == _TRANSFER_COMPOSITE_TRIAL_VARIANT:
            return spec.ui_factor, spec.trgh_delta_q, spec.trsh_factor
    raise ValueError(f"Transfer-composite trial variant not found: {_TRANSFER_COMPOSITE_TRIAL_VARIANT}")


def _load_ui_ladder_targets(ui_ladder_report_path: Path) -> dict[str, float]:
    payload = json.loads(ui_ladder_report_path.read_text(encoding="utf-8"))
    rungs = dict(payload.get("rungs", {}))
    return {
        "transfer-composite-small": float(rungs["ui-small"]["target_mean_delta_trlowz"]),
        "transfer-composite-medium": float(rungs["ui-medium"]["target_mean_delta_trlowz"]),
        "transfer-composite-large": float(rungs["ui-large"]["target_mean_delta_trlowz"]),
    }


def _transfer_spec_from_alpha(*, variant_id: str, alpha: float, target_metric: float) -> TransferScenarioSpec:
    ui_factor = 1.0 + alpha * _TRANSFER_COMPOSITE_UI_STEP
    trgh_delta_q = alpha * _TRANSFER_COMPOSITE_SNAP_STEP
    trsh_factor = 1.0 + alpha * _TRANSFER_COMPOSITE_SS_STEP
    return TransferScenarioSpec(
        variant_id=variant_id,
        ui_factor=ui_factor,
        trgh_delta_q=trgh_delta_q,
        trsh_factor=trsh_factor,
        description=(
            "Matched transfer-composite rung calibrated to the mean first-year "
            f"ΔTRLOWZ target of {target_metric:.12g}."
        ),
    )


def _transfer_composite_specs_from_targets(
    *,
    targets: dict[str, float],
    trial_delta: float,
) -> tuple[list[TransferScenarioSpec], dict[str, object]]:
    if abs(trial_delta) <= 1e-12:
        raise ValueError("Transfer-composite trial delta must be non-zero")
    trial_ui_factor, trial_trgh_delta_q, trial_trsh_factor = _transfer_composite_trial_levers()
    specs = [TransferScenarioSpec("baseline-observed", 1.0, 0.0, 1.0, "Baseline integrated transfer-composite ladder run.")]
    rungs: dict[str, dict[str, float]] = {}
    for variant_id, _scale in _TRANSFER_COMPOSITE_RUNG_IDS:
        target_metric = float(targets[variant_id])
        alpha = target_metric / trial_delta
        spec = _transfer_spec_from_alpha(
            variant_id=variant_id,
            alpha=alpha,
            target_metric=target_metric,
        )
        specs.append(spec)
        rungs[variant_id] = {
            "alpha": alpha,
            "target_mean_delta_trlowz": target_metric,
            "ui_factor": spec.ui_factor,
            "trgh_delta_q": spec.trgh_delta_q,
            "trsh_factor": spec.trsh_factor,
            "trial_ui_factor": trial_ui_factor,
            "trial_trgh_delta_q": trial_trgh_delta_q,
            "trial_trsh_factor": trial_trsh_factor,
        }
    return specs, rungs


def _write_transfer_composite_calibration_report(
    *,
    reports_root: Path,
    prototype_report_path: Path,
    ui_ladder_report_path: Path,
    calibration: dict[str, object],
    scenario_paths: list[Path],
    passes_completed: int | None = None,
) -> Path:
    reports_root.mkdir(parents=True, exist_ok=True)
    payload = {
        "prototype_report_path": str(prototype_report_path),
        "ui_ladder_report_path": str(ui_ladder_report_path),
        "metric": calibration["metric"],
        "target_periods": calibration["target_periods"],
        "trial_variant": calibration["trial_variant"],
        "trial_mean_delta_trlowz": calibration["trial_mean_delta_trlowz"],
        "rungs": calibration["rungs"],
        "scenario_paths": [str(path) for path in scenario_paths],
    }
    if passes_completed is not None:
        payload["passes_completed"] = passes_completed
    report_path = reports_root / "calibrate_phase1_transfer_composite_ladder.json"
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report_path


def _transfer_composite_specs_from_report(
    *,
    prototype_report_path: Path,
    ui_ladder_report_path: Path,
) -> tuple[list[TransferScenarioSpec], dict[str, object]]:
    payload = json.loads(prototype_report_path.read_text(encoding="utf-8"))
    scenarios = dict(payload.get("scenarios", {}))
    baseline_path = Path(str(dict(scenarios["baseline-observed"])["loadformat_path"]))
    trial_path = Path(str(dict(scenarios[_TRANSFER_COMPOSITE_TRIAL_VARIANT])["loadformat_path"]))
    trial_delta = _mean_trlowz_delta_from_loadformats(
        baseline_path=baseline_path,
        scenario_path=trial_path,
    )
    targets = _load_ui_ladder_targets(ui_ladder_report_path)
    specs, rungs = _transfer_composite_specs_from_targets(targets=targets, trial_delta=trial_delta)
    return specs, {
        "metric": "mean_first_year_delta_trlowz",
        "target_periods": ["2026.1", "2026.2", "2026.3", "2026.4"],
        "trial_variant": _TRANSFER_COMPOSITE_TRIAL_VARIANT,
        "trial_mean_delta_trlowz": trial_delta,
        "prototype_report_path": str(prototype_report_path),
        "ui_ladder_report_path": str(ui_ladder_report_path),
        "rungs": rungs,
    }


def write_phase1_transfer_composite_ladder_scenarios(
    *,
    fp_home: Path,
    prototype_report_path: Path | None = None,
    ui_ladder_report_path: Path | None = None,
) -> list[Path]:
    fp_home = locate_fp_home(fp_home)
    paths = repo_paths()
    if prototype_report_path is None:
        prototype_payload = run_phase1_transfer_core(fp_home=fp_home)
        prototype_report_path = Path(str(prototype_payload["report_path"]))
    if ui_ladder_report_path is None:
        ui_ladder_report_path = paths.runtime_phase1_root / "ladder" / "reports" / "run_phase1_ui_ladder.json"
    specs, calibration = _transfer_composite_specs_from_report(
        prototype_report_path=prototype_report_path,
        ui_ladder_report_path=ui_ladder_report_path,
    )
    scenarios_root, artifacts_root, reports_root = _transfer_ladder_paths()
    written = _write_transfer_scenarios(
        fp_home=fp_home,
        scenario_specs=specs,
        scenarios_root=scenarios_root,
        artifacts_root=artifacts_root,
    )
    _write_transfer_composite_calibration_report(
        reports_root=reports_root,
        prototype_report_path=prototype_report_path,
        ui_ladder_report_path=ui_ladder_report_path,
        calibration=calibration,
        scenario_paths=written,
        passes_completed=1,
    )
    return written


def _collect_transfer_composite_points(
    *,
    calibration: dict[str, object],
    rung_results: dict[str, dict[str, float | bool]] | None = None,
) -> list[tuple[float, float]]:
    points: dict[float, float] = {
        0.0: 0.0,
        1.0: float(calibration["trial_mean_delta_trlowz"]),
    }
    for detail in (rung_results or {}).values():
        points[float(detail["alpha"])] = float(detail["achieved_mean_delta_trlowz"])
    return sorted(points.items(), key=lambda item: item[1])


def _interpolate_transfer_alpha_for_target(
    *,
    points: list[tuple[float, float]],
    target_metric: float,
) -> float:
    if len(points) < 2:
        raise ValueError("At least two calibration points are required")
    sorted_points = sorted(points, key=lambda item: item[1])
    for alpha, metric in sorted_points:
        if abs(metric - target_metric) <= 1e-12:
            return alpha
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
        raise ValueError("Cannot interpolate transfer-composite alpha from duplicate metrics")
    weight = (target_metric - lower[1]) / metric_span
    return lower[0] + weight * (upper[0] - lower[0])


def _refined_transfer_composite_specs(
    *,
    calibration: dict[str, object],
    rung_results: dict[str, dict[str, float | bool]],
) -> tuple[list[TransferScenarioSpec], dict[str, object]]:
    points = _collect_transfer_composite_points(calibration=calibration, rung_results=rung_results)
    specs = [TransferScenarioSpec("baseline-observed", 1.0, 0.0, 1.0, "Baseline integrated transfer-composite ladder run.")]
    updated_rungs: dict[str, dict[str, float]] = {}
    for variant_id, rung in dict(calibration["rungs"]).items():
        target = float(dict(rung)["target_mean_delta_trlowz"])
        alpha = _interpolate_transfer_alpha_for_target(points=points, target_metric=target)
        spec = _transfer_spec_from_alpha(variant_id=variant_id, alpha=alpha, target_metric=target)
        specs.append(spec)
        updated_rungs[variant_id] = {
            "alpha": alpha,
            "target_mean_delta_trlowz": target,
            "ui_factor": spec.ui_factor,
            "trgh_delta_q": spec.trgh_delta_q,
            "trsh_factor": spec.trsh_factor,
            "trial_ui_factor": float(dict(rung)["trial_ui_factor"]),
            "trial_trgh_delta_q": float(dict(rung)["trial_trgh_delta_q"]),
            "trial_trsh_factor": float(dict(rung)["trial_trsh_factor"]),
        }
    return specs, {**calibration, "rungs": updated_rungs}


def _evaluate_transfer_composite_run(
    *,
    calibration: dict[str, object],
    run_report: dict[str, object],
) -> tuple[dict[str, dict[str, float | bool]], bool]:
    baseline_path = Path(str(dict(run_report["scenarios"])["baseline-observed"]["loadformat_path"]))
    rung_results: dict[str, dict[str, float | bool]] = {}
    for variant_id, rung in dict(calibration["rungs"]).items():
        scenario_path = Path(str(dict(run_report["scenarios"])[variant_id]["loadformat_path"]))
        achieved = _mean_trlowz_delta_from_loadformats(
            baseline_path=baseline_path,
            scenario_path=scenario_path,
        )
        target = float(dict(rung)["target_mean_delta_trlowz"])
        relative_error = abs(achieved - target) / abs(target) if abs(target) > 1e-12 else 0.0
        rung_results[variant_id] = {
            "alpha": float(dict(rung)["alpha"]),
            "ui_factor": float(dict(rung)["ui_factor"]),
            "trgh_delta_q": float(dict(rung)["trgh_delta_q"]),
            "trsh_factor": float(dict(rung)["trsh_factor"]),
            "target_mean_delta_trlowz": target,
            "achieved_mean_delta_trlowz": achieved,
            "relative_error": relative_error,
            "passes_target": relative_error <= _TRANSFER_COMPOSITE_TOLERANCE,
        }
    passes_targets = all(detail["passes_target"] for detail in rung_results.values())
    return rung_results, passes_targets


def run_phase1_transfer_composite_ladder(
    *,
    fp_home: Path,
    prototype_report_path: Path | None = None,
    ui_ladder_report_path: Path | None = None,
) -> dict[str, object]:
    fp_home = locate_fp_home(fp_home)
    paths = repo_paths()
    if ui_ladder_report_path is None:
        ui_ladder_report_path = paths.runtime_phase1_root / "ladder" / "reports" / "run_phase1_ui_ladder.json"
    scenario_paths = write_phase1_transfer_composite_ladder_scenarios(
        fp_home=fp_home,
        prototype_report_path=prototype_report_path,
        ui_ladder_report_path=ui_ladder_report_path,
    )
    scenarios_root, artifacts_root, reports_root = _transfer_ladder_paths()
    calibration_path = reports_root / "calibrate_phase1_transfer_composite_ladder.json"
    calibration = json.loads(calibration_path.read_text(encoding="utf-8"))
    payload: dict[str, object] | None = None
    run_report: dict[str, object] | None = None
    rung_results: dict[str, dict[str, float | bool]] = {}
    passes_targets = False
    passes_completed = 0

    for pass_index in range(1, _TRANSFER_COMPOSITE_MAX_PASSES + 1):
        payload = _run_transfer_scenarios(
            scenario_paths=scenario_paths,
            artifacts_root=artifacts_root,
            report_path=reports_root / "run_phase1_transfer_composite_ladder_raw.json",
            scenarios_dir=scenarios_root,
        )
        run_report = json.loads(Path(str(payload["report_path"])).read_text(encoding="utf-8"))
        rung_results, passes_targets = _evaluate_transfer_composite_run(
            calibration=calibration,
            run_report=run_report,
        )
        passes_completed = pass_index
        if passes_targets or pass_index == _TRANSFER_COMPOSITE_MAX_PASSES:
            break
        refined_specs, calibration = _refined_transfer_composite_specs(
            calibration=calibration,
            rung_results=rung_results,
        )
        scenario_paths = _write_transfer_scenarios(
            fp_home=fp_home,
            scenario_specs=refined_specs,
            scenarios_root=scenarios_root,
            artifacts_root=artifacts_root,
        )
        _write_transfer_composite_calibration_report(
            reports_root=reports_root,
            prototype_report_path=Path(str(calibration["prototype_report_path"])),
            ui_ladder_report_path=Path(str(calibration["ui_ladder_report_path"])),
            calibration=calibration,
            scenario_paths=scenario_paths,
            passes_completed=pass_index + 1,
        )

    assert payload is not None
    assert run_report is not None
    _write_transfer_composite_calibration_report(
        reports_root=reports_root,
        prototype_report_path=Path(str(calibration["prototype_report_path"])),
        ui_ladder_report_path=Path(str(calibration["ui_ladder_report_path"])),
        calibration=calibration,
        scenario_paths=scenario_paths,
        passes_completed=passes_completed,
    )
    ladder_report = {
        "prototype_report_path": calibration["prototype_report_path"],
        "ui_ladder_report_path": calibration["ui_ladder_report_path"],
        "metric": calibration["metric"],
        "target_periods": calibration["target_periods"],
        "trial_variant": calibration["trial_variant"],
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
    report_path = reports_root / "run_phase1_transfer_composite_ladder.json"
    report_path.write_text(json.dumps(ladder_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "overlay_root": str(paths.runtime_phase1_overlay_root),
        "scenarios_dir": str(scenarios_root),
        "artifacts_dir": str(artifacts_root),
        "passes": bool(ladder_report["passes"]),
        "passes_targets": passes_targets,
    }
