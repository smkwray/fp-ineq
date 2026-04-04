from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .bridge import ensure_fp_wraptr_importable, locate_fp_home
from .paths import repo_paths
from .phase1_ui import (
    _PHASE1_FLAT_TAIL_CHECKS,
    _extract_first_levels,
    _extract_last_levels,
    _extract_levels_from_loadformat,
    _flat_tail_flags,
    _read_loadformat_series,
    build_phase1_private_overlay,
)

__all__ = [
    "PHASE1_TRANSFER_SCENARIOS",
    "run_phase1_transfer_core",
    "write_phase1_transfer_scenarios",
]


PHASE1_TRANSFER_SCENARIOS = (
    ("baseline-observed", 1.00, 0.0, 1.00, "Baseline integrated transfer-core prototype."),
    ("ui-relief", 1.02, 0.0, 1.00, "Higher UI generosity through the stock UB channel."),
    ("ui-shock", 0.98, 0.0, 1.00, "Lower UI generosity through the stock UB channel."),
    ("snap-relief", 1.00, 2.0, 1.00, "Higher household transfers through the stock TRGH channel."),
    ("snap-shock", 1.00, -2.0, 1.00, "Lower household transfers through the stock TRGH channel."),
    (
        "social-security-relief",
        1.00,
        0.0,
        1.02,
        "Higher Social Security benefits through the stock TRSH channel.",
    ),
    (
        "social-security-shock",
        1.00,
        0.0,
        0.99,
        "Lower Social Security benefits through the stock TRSH channel.",
    ),
    (
        "transfer-package-relief",
        1.02,
        2.0,
        1.02,
        "Combined transfer relief through the stock UB, TRGH, and TRSH channels.",
    ),
    (
        "transfer-package-shock",
        0.98,
        -2.0,
        0.99,
        "Combined transfer shock through the stock UB, TRGH, and TRSH channels.",
    ),
)

_TRANSFER_TRACK_VARIABLES = [
    "UB",
    "TRGH",
    "TRSH",
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


@dataclass(frozen=True)
class TransferScenarioSpec:
    variant_id: str
    ui_factor: float
    snap_delta_q: float
    ss_factor: float
    description: str


def _transfer_specs() -> list[TransferScenarioSpec]:
    return [TransferScenarioSpec(*item) for item in PHASE1_TRANSFER_SCENARIOS]


def _scenario_name(variant_id: str) -> str:
    return f"ineq_phase1_transfer_{variant_id.replace('-', '_')}"


def _required_moves_for_variant(variant_id: str) -> tuple[str, ...]:
    if variant_id.startswith("ui-"):
        return ("UB", "YD", "GDPR")
    if variant_id.startswith("snap-"):
        return ("TRGH", "YD", "GDPR")
    if variant_id.startswith("social-security-"):
        return ("TRSH", "YD", "GDPR")
    if variant_id.startswith("transfer-package-"):
        return ("UB", "TRGH", "TRSH", "YD", "GDPR")
    raise ValueError(f"Unsupported transfer-core variant: {variant_id}")


def _expected_sign_for_variant(variant_id: str) -> float:
    if variant_id.endswith("-relief"):
        return 1.0
    if variant_id.endswith("-shock"):
        return -1.0
    raise ValueError(f"Unsupported transfer-core variant: {variant_id}")


def write_phase1_transfer_scenarios(*, fp_home: Path) -> list[Path]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.scenarios.config import ScenarioConfig

    paths = repo_paths()
    fp_home = locate_fp_home(fp_home)
    build_phase1_private_overlay(fp_home=fp_home)
    paths.runtime_transfer_scenarios_root.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    overlay_dir = paths.runtime_phase1_overlay_root.resolve()
    for spec in _transfer_specs():
        input_patches: dict[str, str] = {}
        if abs(spec.ui_factor - 1.0) > 1e-12:
            input_patches["CREATE UIFAC=1;"] = f"CREATE UIFAC={spec.ui_factor:.12g};"
        if abs(spec.snap_delta_q) > 1e-12:
            input_patches["CREATE SNAPDELTAQ=0;"] = f"CREATE SNAPDELTAQ={spec.snap_delta_q:.12g};"
        if abs(spec.ss_factor - 1.0) > 1e-12:
            input_patches["CREATE SSFAC=1;"] = f"CREATE SSFAC={spec.ss_factor:.12g};"
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
            artifacts_root=str(paths.runtime_transfer_artifacts_root),
        )
        path = paths.runtime_transfer_scenarios_root / f"{spec.variant_id}.yaml"
        config.to_yaml(path)
        written.append(path)
    return written


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


def run_phase1_transfer_core(*, fp_home: Path) -> dict[str, object]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.scenarios.runner import load_scenario_config, run_scenario

    paths = repo_paths()
    scenario_paths = write_phase1_transfer_scenarios(fp_home=fp_home)
    paths.runtime_transfer_artifacts_root.mkdir(parents=True, exist_ok=True)
    paths.runtime_transfer_reports_root.mkdir(parents=True, exist_ok=True)

    run_payloads: dict[str, dict[str, object]] = {}
    last_levels: dict[str, dict[str, float | None]] = {}
    for scenario_path in scenario_paths:
        variant_id = scenario_path.stem
        config = load_scenario_config(scenario_path)
        result = run_scenario(
            config=config,
            output_dir=paths.runtime_transfer_artifacts_root,
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
    report_path = paths.runtime_transfer_reports_root / "run_phase1_transfer_core.json"
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "overlay_root": str(paths.runtime_phase1_overlay_root),
        "scenarios_dir": str(paths.runtime_transfer_scenarios_root),
        "artifacts_dir": str(paths.runtime_transfer_artifacts_root),
        "passes": bool(acceptance["passes"]),
        "passes_core": bool(acceptance["passes_core"]),
        "passes_health": bool(acceptance["passes_health"]),
    }
