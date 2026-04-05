from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from .bridge import ensure_fp_wraptr_importable, locate_fp_home
from .paths import repo_paths
from .phase1_distribution_block import build_phase1_distribution_overlay
from .phase1_ui import (
    _extract_first_levels,
    _extract_last_levels,
    _extract_levels_from_loadformat,
    _flat_tail_flags,
    _read_loadformat_payload,
    _read_loadformat_series,
)
from .scenario_ladders import FIRST_YEAR_TRLOWZ_PERIODS, mean_delta_over_periods

__all__ = [
    "PHASE2_UI_OFFSET_SCENARIOS",
    "build_phase2_ui_offset_overlay",
    "run_phase2_ui_offset_envelope",
    "run_phase2_ui_offset",
    "write_phase2_ui_offset_scenarios",
]


PHASE2_UI_OFFSET_SCENARIOS = (
    ("baseline-observed", 1.0, 0.0, "Neutral UI offset family baseline with the matching patch installed neutrally."),
    ("ui-relief-no-offset", 1.02, 0.0, "UI relief with the matching-offset patch installed neutrally."),
    ("ui-relief-offset-25", 1.02, 0.0, "UI relief plus a calibrated matching offset targeting a partial first-year unemployment clawback."),
)

_UI_OFFSET_TRACK_VARIABLES = [
    "IPOVALL",
    "IPOVCH",
    "IWGAP150",
    "TRLOWZ",
    "RYDPC",
    "UB",
    "TRGH",
    "TRSH",
    "YD",
    "GDPR",
    "UR",
    "PCY",
    "JF",
]
_UI_OFFSET_FLAT_TAIL_CHECKS = ("IPOVALL", "IPOVCH", "YD", "GDPR", "UR", "JF")
_UI_OFFSET_EXPERIMENTAL_PATCH_IDS = ("ui_matching_offset_on_private_employment",)
_UI_OFFSET_REFERENCE_VARIANTS = {
    "baseline-observed": "baseline-observed",
    "ui-relief-no-offset": "ui-relief",
}
_UI_OFFSET_REFERENCE_MATCH_VARIABLES = ("UB", "TRGH", "TRSH", "TRLOWZ", "YD", "GDPR", "UR")
_UI_OFFSET_REFERENCE_MATCH_TOLERANCE = 1e-9
_UI_OFFSET_TARGET_CLAWBACK_SHARE = 0.25
_UI_OFFSET_CLAWBACK_TOLERANCE = 0.05
_UI_OFFSET_TRLOWZ_RELATIVE_TOLERANCE = 0.02
_UI_OFFSET_INITIAL_TRIAL_UIMATCH = 0.001
_UI_OFFSET_MAX_UIMATCH = 0.032
_UI_OFFSET_MAX_PASSES = 4
_UI_OFFSET_DEFAULT_ENVELOPE_SHARES = (0.25, 0.50)


@dataclass(frozen=True)
class UiOffsetScenarioSpec:
    variant_id: str
    ui_factor: float
    uimatch: float
    description: str


def _format_decimal(value: float) -> str:
    text = f"{float(value):.12f}".rstrip("0").rstrip(".")
    return text if text else "0"


def _ui_offset_specs(offset_uimatch: float) -> list[UiOffsetScenarioSpec]:
    return [
        UiOffsetScenarioSpec(
            "baseline-observed",
            1.0,
            0.0,
            "Neutral UI offset family baseline with the matching patch installed neutrally.",
        ),
        UiOffsetScenarioSpec(
            "ui-relief-no-offset",
            1.02,
            0.0,
            "UI relief with the matching-offset patch installed neutrally.",
        ),
        UiOffsetScenarioSpec(
            "ui-relief-offset-25",
            1.02,
            float(offset_uimatch),
            (
                "UI relief plus a calibrated matching offset targeting a "
                f"{_UI_OFFSET_TARGET_CLAWBACK_SHARE:.0%} first-year unemployment clawback."
            ),
        ),
    ]


def _clawback_percent_label(target_clawback_share: float) -> int:
    return int(round(float(target_clawback_share) * 100.0))


def _offset_variant_id(target_clawback_share: float) -> str:
    return f"ui-relief-offset-{_clawback_percent_label(target_clawback_share)}"


def _scenario_name(variant_id: str) -> str:
    return f"ineq_phase2_ui_offset_{variant_id.replace('-', '_')}"


def build_phase2_ui_offset_overlay(*, fp_home: Path) -> dict[str, object]:
    paths = repo_paths()
    fp_home = locate_fp_home(fp_home)
    return build_phase1_distribution_overlay(
        fp_home=fp_home,
        overlay_root=paths.runtime_ui_offset_overlay_root,
        reports_root=paths.runtime_ui_offset_reports_root,
        report_name="compose_phase2_ui_offset.json",
        experimental_patch_ids=_UI_OFFSET_EXPERIMENTAL_PATCH_IDS,
    )


def _write_ui_offset_scenarios(
    *,
    fp_home: Path,
    specs: list[UiOffsetScenarioSpec],
    scenarios_root: Path,
    artifacts_root: Path,
) -> list[Path]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.scenarios.config import ScenarioConfig

    paths = repo_paths()
    fp_home = locate_fp_home(fp_home)
    build_phase2_ui_offset_overlay(fp_home=fp_home)
    scenarios_root.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    overlay_dir = paths.runtime_ui_offset_overlay_root.resolve()
    for spec in specs:
        input_patches: dict[str, str] = {}
        if abs(spec.ui_factor - 1.0) > 1e-12:
            input_patches["CREATE UIFAC=1;"] = f"CREATE UIFAC={_format_decimal(spec.ui_factor)};"
        if abs(spec.uimatch) > 1e-12:
            input_patches["CREATE UIMATCH=0;"] = f"CREATE UIMATCH={_format_decimal(spec.uimatch)};"
        config = ScenarioConfig(
            name=_scenario_name(spec.variant_id),
            description=spec.description,
            fp_home=fp_home,
            input_overlay_dir=overlay_dir,
            input_file="fminput.txt",
            forecast_start="2026.1",
            forecast_end="2029.4",
            backend="fpexe",
            track_variables=list(_UI_OFFSET_TRACK_VARIABLES),
            input_patches=input_patches,
            artifacts_root=str(artifacts_root),
        )
        path = scenarios_root / f"{spec.variant_id}.yaml"
        config.to_yaml(path)
        written.append(path)
    return written


def write_phase2_ui_offset_scenarios(*, fp_home: Path, uimatch: float = _UI_OFFSET_INITIAL_TRIAL_UIMATCH) -> list[Path]:
    paths = repo_paths()
    return _write_ui_offset_scenarios(
        fp_home=fp_home,
        specs=_ui_offset_specs(uimatch),
        scenarios_root=paths.runtime_ui_offset_scenarios_root,
        artifacts_root=paths.runtime_ui_offset_artifacts_root,
    )


def _reference_loadformat_path(reference_report_path: Path, variant_id: str) -> Path:
    payload = json.loads(reference_report_path.read_text(encoding="utf-8"))
    scenarios = dict(payload.get("scenarios", {}))
    reference_variant_id = _UI_OFFSET_REFERENCE_VARIANTS[variant_id]
    scenario = dict(scenarios.get(reference_variant_id, {}))
    loadformat_path = scenario.get("loadformat_path")
    if not loadformat_path:
        raise KeyError(
            f"Reference report is missing `scenarios.{reference_variant_id}.loadformat_path`."
        )
    path = Path(str(loadformat_path))
    if not path.exists():
        raise FileNotFoundError(f"Reference LOADFORMAT.DAT not found: {path}")
    return path


def _mean_first_year_delta(*, baseline_path: Path, scenario_path: Path, variable: str) -> float:
    baseline_periods, baseline_series = _read_loadformat_payload(baseline_path)
    scenario_periods, scenario_series = _read_loadformat_payload(scenario_path)
    if baseline_periods != scenario_periods:
        raise ValueError("Baseline and scenario periods do not match for UI offset calibration")
    baseline_values = baseline_series.get(variable)
    scenario_values = scenario_series.get(variable)
    if not baseline_values or not scenario_values:
        raise KeyError(f"{variable} series missing from LOADFORMAT output")
    return mean_delta_over_periods(
        periods=baseline_periods,
        baseline_values=baseline_values,
        scenario_values=scenario_values,
        target_periods=FIRST_YEAR_TRLOWZ_PERIODS,
    )


def _series_match_max_abs_diff(
    *,
    reference_path: Path,
    candidate_path: Path,
    variables: tuple[str, ...] = _UI_OFFSET_REFERENCE_MATCH_VARIABLES,
) -> float:
    reference_periods, reference_series = _read_loadformat_payload(reference_path)
    candidate_periods, candidate_series = _read_loadformat_payload(candidate_path)
    if reference_periods != candidate_periods:
        raise ValueError("Reference and candidate periods do not match for UI offset comparison")
    max_diff = 0.0
    for variable in variables:
        reference_values = reference_series.get(variable)
        candidate_values = candidate_series.get(variable)
        if not reference_values or not candidate_values:
            raise KeyError(f"{variable} series missing from LOADFORMAT output")
        for ref_value, candidate_value in zip(reference_values, candidate_values, strict=True):
            max_diff = max(max_diff, abs(float(candidate_value) - float(ref_value)))
    return max_diff


def _target_offset_delta_ur(no_offset_delta_ur: float, target_clawback_share: float) -> float:
    if float(no_offset_delta_ur) >= 0.0:
        raise ValueError("UI no-offset run must improve first-year UR before clawback can be targeted")
    return float(no_offset_delta_ur) * (1.0 - float(target_clawback_share))


def _clawback_share(no_offset_delta_ur: float, offset_delta_ur: float) -> float:
    improvement = -float(no_offset_delta_ur)
    if improvement <= 1e-12:
        raise ValueError("UI no-offset run must improve first-year UR before clawback can be measured")
    return (float(offset_delta_ur) - float(no_offset_delta_ur)) / improvement


def _interpolate_uimatch_for_target(
    *,
    points: list[tuple[float, float]],
    target_metric: float,
) -> float:
    if len(points) < 2:
        raise ValueError("At least two calibration points are required")
    sorted_points = sorted(points, key=lambda item: item[1])
    lower = sorted_points[0]
    upper = sorted_points[-1]
    for left, right in zip(sorted_points, sorted_points[1:]):
        if left[1] <= target_metric <= right[1]:
            lower, upper = left, right
            break
    metric_span = upper[1] - lower[1]
    if abs(metric_span) <= 1e-12:
        raise ValueError("Cannot interpolate UI matching offset from duplicate metrics")
    weight = (target_metric - lower[1]) / metric_span
    return lower[0] + weight * (upper[0] - lower[0])


def _next_uimatch_candidate(
    *,
    points: dict[float, float],
    target_metric: float,
    max_uimatch: float,
) -> float | None:
    sorted_points = sorted((float(uimatch), float(metric)) for uimatch, metric in points.items())
    if all(metric < target_metric for _uimatch, metric in sorted_points):
        largest_uimatch = max(uimatch for uimatch, _metric in sorted_points)
        if largest_uimatch >= max_uimatch - 1e-12:
            return None
        candidate = min(max_uimatch, largest_uimatch * 2 if largest_uimatch > 0 else _UI_OFFSET_INITIAL_TRIAL_UIMATCH)
        return None if any(abs(candidate - existing) <= 1e-12 for existing, _metric in sorted_points) else candidate
    candidate = max(0.0, min(max_uimatch, _interpolate_uimatch_for_target(points=sorted_points, target_metric=target_metric)))
    return None if any(abs(candidate - existing) <= 1e-12 for existing, _metric in sorted_points) else candidate


def _build_metrics(
    *,
    run_report: dict[str, object],
    reference_report_path: Path,
    target_clawback_share: float,
) -> dict[str, object]:
    scenarios = dict(run_report["scenarios"])
    baseline_path = Path(str(dict(scenarios["baseline-observed"])["loadformat_path"]))
    no_offset_path = Path(str(dict(scenarios["ui-relief-no-offset"])["loadformat_path"]))
    offset_variant_id = _offset_variant_id(target_clawback_share)
    offset_path = Path(str(dict(scenarios[offset_variant_id])["loadformat_path"]))

    reference_baseline_path = _reference_loadformat_path(reference_report_path, "baseline-observed")
    reference_ui_relief_path = _reference_loadformat_path(reference_report_path, "ui-relief-no-offset")

    no_offset_delta_ur = _mean_first_year_delta(
        baseline_path=baseline_path,
        scenario_path=no_offset_path,
        variable="UR",
    )
    offset_delta_ur = _mean_first_year_delta(
        baseline_path=baseline_path,
        scenario_path=offset_path,
        variable="UR",
    )
    no_offset_delta_trlowz = _mean_first_year_delta(
        baseline_path=baseline_path,
        scenario_path=no_offset_path,
        variable="TRLOWZ",
    )
    offset_delta_trlowz = _mean_first_year_delta(
        baseline_path=baseline_path,
        scenario_path=offset_path,
        variable="TRLOWZ",
    )

    trlowz_relative_gap = abs(offset_delta_trlowz - no_offset_delta_trlowz) / abs(no_offset_delta_trlowz)
    target_delta_ur = _target_offset_delta_ur(no_offset_delta_ur, target_clawback_share)

    return {
        "baseline_reference_max_abs_diff": _series_match_max_abs_diff(
            reference_path=reference_baseline_path,
            candidate_path=baseline_path,
        ),
        "no_offset_reference_max_abs_diff": _series_match_max_abs_diff(
            reference_path=reference_ui_relief_path,
            candidate_path=no_offset_path,
        ),
        "reference_match_tolerance": _UI_OFFSET_REFERENCE_MATCH_TOLERANCE,
        "target_periods": list(FIRST_YEAR_TRLOWZ_PERIODS),
        "target_clawback_share": float(target_clawback_share),
        "clawback_tolerance": _UI_OFFSET_CLAWBACK_TOLERANCE,
        "trlowz_relative_tolerance": _UI_OFFSET_TRLOWZ_RELATIVE_TOLERANCE,
        "target_mean_delta_ur": target_delta_ur,
        "no_offset_mean_delta_ur": no_offset_delta_ur,
        "offset_mean_delta_ur": offset_delta_ur,
        "clawback_share": _clawback_share(no_offset_delta_ur, offset_delta_ur),
        "no_offset_mean_delta_trlowz": no_offset_delta_trlowz,
        "offset_mean_delta_trlowz": offset_delta_trlowz,
        "trlowz_relative_gap": trlowz_relative_gap,
        "no_offset_last_levels": dict(dict(scenarios["ui-relief-no-offset"])["last_levels"]),
        "offset_last_levels": dict(dict(scenarios[offset_variant_id])["last_levels"]),
    }


def _acceptance_summary(metrics: dict[str, object]) -> dict[str, object]:
    no_offset_last_levels = dict(metrics["no_offset_last_levels"])
    offset_last_levels = dict(metrics["offset_last_levels"])
    checks = {
        "baseline_matches_reference": float(metrics["baseline_reference_max_abs_diff"]) <= float(metrics["reference_match_tolerance"]),
        "no_offset_matches_reference": float(metrics["no_offset_reference_max_abs_diff"]) <= float(metrics["reference_match_tolerance"]),
        "offset_has_higher_ur": float(offset_last_levels["UR"]) > float(no_offset_last_levels["UR"]),
        "offset_has_lower_yd": float(offset_last_levels["YD"]) < float(no_offset_last_levels["YD"]),
        "offset_preserves_trlowz": float(metrics["trlowz_relative_gap"]) <= float(metrics["trlowz_relative_tolerance"]),
        "offset_hits_target_clawback": abs(float(metrics["clawback_share"]) - float(metrics["target_clawback_share"]))
        <= float(metrics["clawback_tolerance"]),
    }
    diagnostics = {
        "offset_has_lower_gdpr": float(offset_last_levels["GDPR"]) < float(no_offset_last_levels["GDPR"]),
    }
    return {
        "checks": checks,
        "diagnostics": diagnostics,
        "passes_core": all(checks.values()),
    }


def _run_ui_offset_specs(
    *,
    fp_home: Path,
    specs: list[UiOffsetScenarioSpec],
    report_name: str,
) -> dict[str, object]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.scenarios import runner as scenario_runner
    from fp_wraptr.scenarios.runner import load_scenario_config

    paths = repo_paths()
    fp_home = locate_fp_home(fp_home)
    scenario_paths = _write_ui_offset_scenarios(
        fp_home=fp_home,
        specs=specs,
        scenarios_root=paths.runtime_ui_offset_scenarios_root,
        artifacts_root=paths.runtime_ui_offset_artifacts_root,
    )
    paths.runtime_ui_offset_artifacts_root.mkdir(parents=True, exist_ok=True)
    paths.runtime_ui_offset_reports_root.mkdir(parents=True, exist_ok=True)

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
                output_dir=paths.runtime_ui_offset_artifacts_root,
            )
            fmout_path = result.output_dir / "fmout.txt"
            fmout_text = fmout_path.read_text(encoding="utf-8", errors="replace") if fmout_path.exists() else ""
            solve_error_sol1 = "Solution error in SOL1." in fmout_text
            loadformat_path = result.output_dir / "LOADFORMAT.DAT"
            if loadformat_path.exists():
                loadformat_series = _read_loadformat_series(loadformat_path)
                first_levels, variant_last_levels = _extract_levels_from_loadformat(
                    loadformat_path,
                    _UI_OFFSET_TRACK_VARIABLES,
                )
                flat_tail_flags = _flat_tail_flags(loadformat_series, _UI_OFFSET_FLAT_TAIL_CHECKS)
            else:
                first_levels = _extract_first_levels(result.parsed_output, _UI_OFFSET_TRACK_VARIABLES)
                variant_last_levels = _extract_last_levels(result.parsed_output, _UI_OFFSET_TRACK_VARIABLES)
                flat_tail_flags = {name: False for name in _UI_OFFSET_FLAT_TAIL_CHECKS}
            last_levels[variant_id] = variant_last_levels
            run_payloads[variant_id] = {
                "scenario_name": config.name,
                "description": config.description,
                "uimatch": next(spec.uimatch for spec in specs if spec.variant_id == variant_id),
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

    unhealthy = {
        variant_id: {
            "solve_error_sol1": bool(payload["solve_error_sol1"]),
            "flat_tail_vars": [name for name, flagged in dict(payload["flat_tail_flags"]).items() if flagged],
        }
        for variant_id, payload in run_payloads.items()
        if variant_id != "baseline-observed"
    }
    report_payload = {
        "scenarios": run_payloads,
        "scenario_health": unhealthy,
        "track_variables": list(_UI_OFFSET_TRACK_VARIABLES),
        "scenario_paths": [str(path) for path in scenario_paths],
        "experimental_patch_ids": list(_UI_OFFSET_EXPERIMENTAL_PATCH_IDS),
    }
    report_path = paths.runtime_ui_offset_reports_root / report_name
    report_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "scenarios": run_payloads,
        "scenario_paths": [str(path) for path in scenario_paths],
    }


def run_phase2_ui_offset(
    *,
    fp_home: Path,
    reference_report_path: Path | None = None,
    target_clawback_share: float = _UI_OFFSET_TARGET_CLAWBACK_SHARE,
    write_latest_report: bool = True,
) -> dict[str, object]:
    paths = repo_paths()
    fp_home = locate_fp_home(fp_home)
    reference_report_path = reference_report_path or (
        paths.runtime_distribution_reports_root / "run_phase1_distribution_block.json"
    )
    if not reference_report_path.exists():
        raise FileNotFoundError(
            f"Reference distribution report missing: {reference_report_path}. "
            "Run `fp-ineq run-phase1-distribution-block` first or pass --reference-report-path."
        )

    candidate_uimatch = _UI_OFFSET_INITIAL_TRIAL_UIMATCH
    points: dict[float, float] = {}
    metrics: dict[str, object] | None = None
    acceptance: dict[str, object] | None = None
    run_report: dict[str, object] | None = None
    passes_completed = 0
    share_label = _clawback_percent_label(target_clawback_share)

    for pass_index in range(1, _UI_OFFSET_MAX_PASSES + 1):
        specs = [
            UiOffsetScenarioSpec("baseline-observed", 1.0, 0.0, "Neutral UI offset family baseline with the matching patch installed neutrally."),
            UiOffsetScenarioSpec("ui-relief-no-offset", 1.02, 0.0, "UI relief with the matching-offset patch installed neutrally."),
            UiOffsetScenarioSpec(
                _offset_variant_id(target_clawback_share),
                1.02,
                float(candidate_uimatch),
                (
                    "UI relief plus a calibrated matching offset targeting a "
                    f"{share_label}% first-year unemployment clawback."
                ),
            ),
        ]
        payload = _run_ui_offset_specs(
            fp_home=fp_home,
            specs=specs,
            report_name=f"run_phase2_ui_offset_{share_label}_raw.json",
        )
        run_report = json.loads(Path(str(payload["report_path"])).read_text(encoding="utf-8"))
        metrics = _build_metrics(
            run_report=run_report,
            reference_report_path=reference_report_path,
            target_clawback_share=target_clawback_share,
        )
        points[0.0] = float(metrics["no_offset_mean_delta_ur"])
        points[candidate_uimatch] = float(metrics["offset_mean_delta_ur"])
        acceptance = _acceptance_summary(metrics)
        passes_completed = pass_index
        if acceptance["passes_core"]:
            break
        next_candidate = _next_uimatch_candidate(
            points=points,
            target_metric=float(metrics["target_mean_delta_ur"]),
            max_uimatch=_UI_OFFSET_MAX_UIMATCH,
        )
        if next_candidate is None:
            break
        candidate_uimatch = next_candidate

    assert metrics is not None
    assert acceptance is not None
    assert run_report is not None

    final_payload = {
        "reference_report_path": str(reference_report_path),
        "offset_variant_id": _offset_variant_id(target_clawback_share),
        "target_periods": list(FIRST_YEAR_TRLOWZ_PERIODS),
        "target_clawback_share": float(target_clawback_share),
        "clawback_tolerance": _UI_OFFSET_CLAWBACK_TOLERANCE,
        "trlowz_relative_tolerance": _UI_OFFSET_TRLOWZ_RELATIVE_TOLERANCE,
        "reference_match_tolerance": _UI_OFFSET_REFERENCE_MATCH_TOLERANCE,
        "experimental_patch_ids": list(_UI_OFFSET_EXPERIMENTAL_PATCH_IDS),
        "passes_completed": passes_completed,
        "calibration_points": [
            {"uimatch": float(uimatch), "mean_delta_ur": float(metric)}
            for uimatch, metric in sorted(points.items())
        ],
        "calibrated_uimatch": float(candidate_uimatch),
        "metrics": metrics,
        "acceptance": {
            **acceptance,
            "scenario_health": run_report["scenario_health"],
            "passes": bool(acceptance["passes_core"])
            and all(
                (not detail["solve_error_sol1"]) and (not detail["flat_tail_vars"])
                for detail in dict(run_report["scenario_health"]).values()
            ),
        },
        "scenarios": run_report["scenarios"],
        "track_variables": run_report["track_variables"],
        "scenario_paths": run_report["scenario_paths"],
    }
    report_path = paths.runtime_ui_offset_reports_root / f"run_phase2_ui_offset_{share_label}.json"
    report_path.write_text(json.dumps(final_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if write_latest_report:
        latest_report_path = paths.runtime_ui_offset_reports_root / "run_phase2_ui_offset.json"
        latest_report_path.write_text(json.dumps(final_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "overlay_root": str(paths.runtime_ui_offset_overlay_root),
        "scenarios_dir": str(paths.runtime_ui_offset_scenarios_root),
        "artifacts_dir": str(paths.runtime_ui_offset_artifacts_root),
        "passes": bool(final_payload["acceptance"]["passes"]),
    }


def run_phase2_ui_offset_envelope(
    *,
    fp_home: Path,
    reference_report_path: Path | None = None,
    target_clawback_shares: tuple[float, ...] = _UI_OFFSET_DEFAULT_ENVELOPE_SHARES,
) -> dict[str, object]:
    paths = repo_paths()
    results: dict[str, dict[str, object]] = {}
    for index, share in enumerate(target_clawback_shares):
        payload = run_phase2_ui_offset(
            fp_home=fp_home,
            reference_report_path=reference_report_path,
            target_clawback_share=float(share),
            write_latest_report=index == 0,
        )
        report = json.loads(Path(str(payload["report_path"])).read_text(encoding="utf-8"))
        results[f"{_clawback_percent_label(share)}"] = {
            "report_path": str(payload["report_path"]),
            "passes": bool(payload["passes"]),
            "calibrated_uimatch": float(report["calibrated_uimatch"]),
            "clawback_share": float(dict(report["metrics"])["clawback_share"]),
            "trlowz_relative_gap": float(dict(report["metrics"])["trlowz_relative_gap"]),
            "offset_has_lower_gdpr": bool(dict(dict(report["acceptance"])["diagnostics"])["offset_has_lower_gdpr"]),
        }
    summary = {
        "reference_report_path": str(reference_report_path) if reference_report_path is not None else str(paths.runtime_distribution_reports_root / "run_phase1_distribution_block.json"),
        "shares": results,
    }
    report_path = paths.runtime_ui_offset_reports_root / "run_phase2_ui_offset_envelope.json"
    report_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "passes": all(bool(detail["passes"]) for detail in results.values()),
    }
