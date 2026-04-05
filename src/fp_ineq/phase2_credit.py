from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .bridge import ensure_fp_wraptr_importable, locate_fp_home
from .paths import repo_paths
from .phase1_distribution_block import build_phase1_distribution_overlay
from .phase1_ui import (
    _extract_first_levels,
    _extract_last_levels,
    _extract_levels_from_loadformat,
    _flat_tail_flags,
    _read_loadformat_series,
)

__all__ = [
    "PHASE2_CREDIT_SCENARIOS",
    "build_phase2_credit_overlay",
    "run_phase2_credit",
    "run_phase2_credit_scale_sweep",
    "write_phase2_credit_scenarios",
]


PHASE2_CREDIT_SCENARIOS = (
    ("baseline-observed", 0.0, "Neutral effective-rate installation with the credit patch group enabled."),
    ("credit-easing", -1.0, "Credit easing via a lower household effective-rate wedge."),
    ("credit-tightening", 1.0, "Credit tightening via a higher household effective-rate wedge."),
)

_CREDIT_TRACK_VARIABLES = [
    "RSA",
    "RMA",
    "RSAEFF",
    "RMAEFF",
    "CS",
    "CN",
    "CD",
    "IHH",
    "CUR",
    "YD",
    "GDPR",
    "UR",
    "PCY",
    "IWGAP150",
]
_CREDIT_REQUIRED_SIGN_TRACKS = ("RSAEFF", "RMAEFF")
_CREDIT_DEMAND_MOVE_TRACKS = ("CS", "CN", "CD", "IHH", "CUR", "YD", "GDPR")
_CREDIT_FLAT_TAIL_CHECKS = ("RSAEFF", "RMAEFF", "CS", "CN", "CD", "IHH", "GDPR")
_CREDIT_EXPERIMENTAL_PATCH_IDS = ("credit_effective_rates",)
_CREDIT_SWEEP_DEFAULT_MAGNITUDES = (1.0, 5.0, 10.0)
_CREDIT_PUBLICATION_DEMAND_THRESHOLD = 1e-4


@dataclass(frozen=True)
class CreditScenarioSpec:
    variant_id: str
    crwedge: float
    description: str


def _credit_specs() -> list[CreditScenarioSpec]:
    return [CreditScenarioSpec(*item) for item in PHASE2_CREDIT_SCENARIOS]


def _credit_specs_for_magnitude(magnitude: float) -> list[CreditScenarioSpec]:
    return [
        CreditScenarioSpec("baseline-observed", 0.0, "Neutral effective-rate installation with the credit patch group enabled."),
        CreditScenarioSpec("credit-easing", -abs(float(magnitude)), f"Credit easing via household effective-rate wedge { -abs(float(magnitude)) :.12g}."),
        CreditScenarioSpec("credit-tightening", abs(float(magnitude)), f"Credit tightening via household effective-rate wedge { abs(float(magnitude)) :.12g}."),
    ]


def _scenario_name(variant_id: str) -> str:
    return f"ineq_phase2_credit_{variant_id.replace('-', '_')}"


def _credit_sweep_paths() -> tuple[Path, Path]:
    paths = repo_paths()
    root = paths.runtime_credit_root / "sweep"
    return root, root / "reports"


def _magnitude_slug(magnitude: float) -> str:
    return f"{float(magnitude):g}".replace("-", "m").replace(".", "p")


def _expected_sign_for_variant(variant_id: str) -> float:
    if variant_id == "credit-easing":
        return -1.0
    if variant_id == "credit-tightening":
        return 1.0
    raise ValueError(f"Unsupported credit variant: {variant_id}")


def build_phase2_credit_overlay(*, fp_home: Path) -> dict[str, object]:
    paths = repo_paths()
    fp_home = locate_fp_home(fp_home)
    payload = build_phase1_distribution_overlay(
        fp_home=fp_home,
        overlay_root=paths.runtime_credit_overlay_root,
        reports_root=paths.runtime_credit_reports_root,
        report_name="compose_phase2_credit.json",
        experimental_patch_ids=_CREDIT_EXPERIMENTAL_PATCH_IDS,
    )
    return payload


def _write_credit_scenarios(
    *,
    fp_home: Path,
    specs: list[CreditScenarioSpec],
    scenarios_root: Path,
    artifacts_root: Path,
) -> list[Path]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.scenarios.config import ScenarioConfig

    paths = repo_paths()
    fp_home = locate_fp_home(fp_home)
    build_phase2_credit_overlay(fp_home=fp_home)
    scenarios_root.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    overlay_dir = paths.runtime_credit_overlay_root.resolve()

    for spec in specs:
        input_patches: dict[str, str] = {}
        if abs(spec.crwedge) > 1e-12:
            input_patches["CREATE CRWEDGE=0;"] = f"CREATE CRWEDGE={spec.crwedge:.12g};"
        config = ScenarioConfig(
            name=_scenario_name(spec.variant_id),
            description=spec.description,
            fp_home=fp_home,
            input_overlay_dir=overlay_dir,
            input_file="fminput.txt",
            forecast_start="2026.1",
            forecast_end="2029.4",
            backend="fpexe",
            track_variables=list(_CREDIT_TRACK_VARIABLES),
            input_patches=input_patches,
            artifacts_root=str(artifacts_root),
        )
        path = scenarios_root / f"{spec.variant_id}.yaml"
        config.to_yaml(path)
        written.append(path)
    return written


def write_phase2_credit_scenarios(*, fp_home: Path) -> list[Path]:
    paths = repo_paths()
    return _write_credit_scenarios(
        fp_home=fp_home,
        specs=_credit_specs(),
        scenarios_root=paths.runtime_credit_scenarios_root,
        artifacts_root=paths.runtime_credit_artifacts_root,
    )


def _acceptance_summary(results: dict[str, dict[str, float | None]]) -> dict[str, Any]:
    baseline = results.get("baseline-observed", {})
    tolerance = 1e-9
    comparisons: dict[str, dict[str, float | None]] = {}
    scenario_checks: dict[str, dict[str, Any]] = {}

    for variant_id, values in results.items():
        if variant_id == "baseline-observed":
            continue
        expected_sign = _expected_sign_for_variant(variant_id)
        required_signs = {name: False for name in _CREDIT_REQUIRED_SIGN_TRACKS}
        demand_moves = {name: False for name in _CREDIT_DEMAND_MOVE_TRACKS}
        delta_map: dict[str, float | None] = {}
        for name in _CREDIT_TRACK_VARIABLES:
            base_value = baseline.get(name)
            scenario_value = values.get(name)
            if base_value is None or scenario_value is None:
                delta_map[name] = None
                continue
            delta = float(scenario_value - base_value)
            delta_map[name] = delta
            if name in required_signs and (delta * expected_sign) > tolerance:
                required_signs[name] = True
            if name in demand_moves and abs(delta) > tolerance:
                demand_moves[name] = True
        comparisons[variant_id] = delta_map
        scenario_checks[variant_id] = {
            "required_signs": required_signs,
            "demand_moves": demand_moves,
            "passes_core": all(required_signs.values()) and any(demand_moves.values()),
        }

    return {
        "comparisons": comparisons,
        "scenario_checks": scenario_checks,
        "passes_core": all(detail["passes_core"] for detail in scenario_checks.values()),
    }


def _max_abs_delta(delta_map: dict[str, float | None], variables: tuple[str, ...]) -> float:
    values = [abs(float(delta_map[name])) for name in variables if delta_map.get(name) is not None]
    return max(values) if values else 0.0


def _credit_scale_sweep_assessment(
    sweep_results: dict[float, dict[str, dict[str, float | None]]],
    *,
    demand_threshold: float = _CREDIT_PUBLICATION_DEMAND_THRESHOLD,
) -> dict[str, object]:
    magnitudes: dict[str, object] = {}
    best_abs_demand_delta = 0.0
    best_magnitude = 0.0

    for magnitude, comparisons in sorted(sweep_results.items()):
        easing = dict(comparisons.get("credit-easing", {}))
        tightening = dict(comparisons.get("credit-tightening", {}))
        easing_demand = _max_abs_delta(easing, _CREDIT_DEMAND_MOVE_TRACKS)
        tightening_demand = _max_abs_delta(tightening, _CREDIT_DEMAND_MOVE_TRACKS)
        easing_rate = _max_abs_delta(easing, _CREDIT_REQUIRED_SIGN_TRACKS)
        tightening_rate = _max_abs_delta(tightening, _CREDIT_REQUIRED_SIGN_TRACKS)
        max_abs_demand_delta = max(easing_demand, tightening_demand)
        max_abs_rate_delta = max(easing_rate, tightening_rate)
        if max_abs_demand_delta > best_abs_demand_delta:
            best_abs_demand_delta = max_abs_demand_delta
            best_magnitude = float(magnitude)
        magnitudes[f"{float(magnitude):g}"] = {
            "max_abs_demand_delta": max_abs_demand_delta,
            "max_abs_rate_delta": max_abs_rate_delta,
            "comparisons": comparisons,
            "passes_signal_threshold": max_abs_demand_delta >= demand_threshold,
        }

    publication_ready = best_abs_demand_delta >= demand_threshold
    return {
        "magnitudes": magnitudes,
        "demand_threshold": float(demand_threshold),
        "best_abs_demand_delta": float(best_abs_demand_delta),
        "best_magnitude": float(best_magnitude),
        "publication_ready": publication_ready,
        "recommended_action": (
            "build_private_credit_ladder"
            if publication_ready
            else "keep_private_and_do_not_build_credit_ladder_yet"
        ),
    }


def _run_credit_specs(
    *,
    fp_home: Path,
    specs: list[CreditScenarioSpec],
    scenarios_root: Path,
    artifacts_root: Path,
    reports_root: Path,
    report_name: str,
) -> dict[str, object]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.scenarios import runner as scenario_runner
    from fp_wraptr.scenarios.runner import load_scenario_config

    fp_home = locate_fp_home(fp_home)
    scenario_paths = _write_credit_scenarios(
        fp_home=fp_home,
        specs=specs,
        scenarios_root=scenarios_root,
        artifacts_root=artifacts_root,
    )
    artifacts_root.mkdir(parents=True, exist_ok=True)
    reports_root.mkdir(parents=True, exist_ok=True)

    run_payloads: dict[str, dict[str, object]] = {}
    last_levels: dict[str, dict[str, float | None]] = {}

    original_parse_fp_output = scenario_runner.parse_fp_output

    def _safe_parse_fp_output(path: Path) -> object | None:
        try:
            return original_parse_fp_output(path)
        except ValueError:
            return None

    scenario_runner.parse_fp_output = _safe_parse_fp_output
    try:
        for scenario_path in scenario_paths:
            variant_id = scenario_path.stem
            config = load_scenario_config(scenario_path)
            result = scenario_runner.run_scenario(
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
                    _CREDIT_TRACK_VARIABLES,
                )
                flat_tail_flags = _flat_tail_flags(loadformat_series, _CREDIT_FLAT_TAIL_CHECKS)
            else:
                first_levels = _extract_first_levels(result.parsed_output, _CREDIT_TRACK_VARIABLES)
                variant_last_levels = _extract_last_levels(result.parsed_output, _CREDIT_TRACK_VARIABLES)
                flat_tail_flags = {name: False for name in _CREDIT_FLAT_TAIL_CHECKS}
            last_levels[variant_id] = variant_last_levels
            run_payloads[variant_id] = {
                "scenario_name": config.name,
                "description": config.description,
                "success": bool(result.success),
                "output_dir": str(result.output_dir),
                "loadformat_path": str(loadformat_path) if loadformat_path.exists() else None,
                "return_code": int(result.run_result.return_code) if result.run_result is not None else None,
                "solve_error_sol1": solve_error_sol1,
                "flat_tail_flags": flat_tail_flags,
                "first_levels": first_levels,
                "last_levels": variant_last_levels,
                "backend_diagnostics": result.backend_diagnostics,
            }
    finally:
        scenario_runner.parse_fp_output = original_parse_fp_output

    acceptance = _acceptance_summary(last_levels)
    unhealthy = {
        variant_id: {
            "solve_error_sol1": bool(payload["solve_error_sol1"]),
            "flat_tail_vars": [name for name, flagged in dict(payload["flat_tail_flags"]).items() if flagged],
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
        "track_variables": list(_CREDIT_TRACK_VARIABLES),
        "scenario_paths": [str(path) for path in scenario_paths],
        "experimental_patch_ids": list(_CREDIT_EXPERIMENTAL_PATCH_IDS),
    }
    report_path = reports_root / report_name
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "overlay_root": str(repo_paths().runtime_credit_overlay_root),
        "scenarios_dir": str(scenarios_root),
        "artifacts_dir": str(artifacts_root),
        "passes": bool(acceptance["passes"]),
        "passes_core": bool(acceptance["passes_core"]),
        "passes_health": bool(acceptance["passes_health"]),
    }


def run_phase2_credit(*, fp_home: Path) -> dict[str, object]:
    paths = repo_paths()
    return _run_credit_specs(
        fp_home=fp_home,
        specs=_credit_specs(),
        scenarios_root=paths.runtime_credit_scenarios_root,
        artifacts_root=paths.runtime_credit_artifacts_root,
        reports_root=paths.runtime_credit_reports_root,
        report_name="run_phase2_credit.json",
    )


def run_phase2_credit_scale_sweep(
    *,
    fp_home: Path,
    magnitudes: tuple[float, ...] = _CREDIT_SWEEP_DEFAULT_MAGNITUDES,
) -> dict[str, object]:
    sweep_root, sweep_reports_root = _credit_sweep_paths()
    sweep_reports_root.mkdir(parents=True, exist_ok=True)
    magnitude_results: dict[float, dict[str, dict[str, float | None]]] = {}
    run_reports: dict[str, object] = {}

    for magnitude in magnitudes:
        slug = _magnitude_slug(magnitude)
        scenario_root = sweep_root / slug / "scenarios"
        artifacts_root = sweep_root / slug / "artifacts"
        run_payload = _run_credit_specs(
            fp_home=fp_home,
            specs=_credit_specs_for_magnitude(float(magnitude)),
            scenarios_root=scenario_root,
            artifacts_root=artifacts_root,
            reports_root=sweep_reports_root,
            report_name=f"run_phase2_credit_{slug}.json",
        )
        run_report_path = Path(str(run_payload["report_path"]))
        run_report = json.loads(run_report_path.read_text(encoding="utf-8"))
        magnitude_results[float(magnitude)] = dict(run_report["acceptance"]["comparisons"])
        run_reports[f"{float(magnitude):g}"] = {
            "report_path": str(run_report_path),
            "artifacts_dir": str(run_payload["artifacts_dir"]),
            "passes": bool(run_payload["passes"]),
        }

    assessment = _credit_scale_sweep_assessment(magnitude_results)
    payload = {
        "magnitudes": [float(item) for item in magnitudes],
        "experimental_patch_ids": list(_CREDIT_EXPERIMENTAL_PATCH_IDS),
        "assessment": assessment,
        "runs": run_reports,
    }
    report_path = sweep_reports_root / "run_phase2_credit_scale_sweep.json"
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "reports_dir": str(sweep_reports_root),
        "recommended_action": str(assessment["recommended_action"]),
        "publication_ready": bool(assessment["publication_ready"]),
    }
