from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .bridge import ensure_fp_wraptr_importable, locate_fp_home
from .paths import repo_paths
from .phase1_compose import compose_phase1_overlay
from .phase1_ui import (
    _extract_first_levels,
    _extract_last_levels,
    _extract_levels_from_loadformat,
    _flat_tail_flags,
    _read_loadformat_series,
)

__all__ = [
    "PHASE1_DISTRIBUTION_SCENARIOS",
    "build_phase1_distribution_overlay",
    "estimate_phase1_distribution_coefficients",
    "run_phase1_distribution_block",
    "write_phase1_distribution_scenarios",
]


PHASE1_DISTRIBUTION_SCENARIOS = (
    (
        "baseline-observed",
        1.00,
        0.0,
        1.00,
        "Baseline transfer-core scenario with integrated distribution identities.",
    ),
    (
        "ui-relief",
        1.02,
        0.0,
        1.00,
        "Higher UI generosity with integrated distribution identities.",
    ),
    (
        "ui-shock",
        0.98,
        0.0,
        1.00,
        "Lower UI generosity with integrated distribution identities.",
    ),
    (
        "snap-relief",
        1.00,
        2.0,
        1.00,
        "Higher SNAP-style transfers with integrated distribution identities.",
    ),
    (
        "snap-shock",
        1.00,
        -2.0,
        1.00,
        "Lower SNAP-style transfers with integrated distribution identities.",
    ),
    (
        "social-security-relief",
        1.00,
        0.0,
        1.02,
        "Higher Social Security benefits with integrated distribution identities.",
    ),
    (
        "social-security-shock",
        1.00,
        0.0,
        0.99,
        "Lower Social Security benefits with integrated distribution identities.",
    ),
    (
        "transfer-package-relief",
        1.02,
        2.0,
        1.02,
        "Combined transfer relief with offline-estimated in-model distribution identities.",
    ),
    (
        "transfer-package-shock",
        0.98,
        -2.0,
        0.99,
        "Combined transfer shock with offline-estimated in-model distribution identities.",
    ),
)

_DISTRIBUTION_TRACK_VARIABLES = [
    "IPOVALL",
    "IPOVCH",
    "IGINIHH",
    "IMEDRINC",
    "TRLOWZ",
    "RYDPC",
    "UB",
    "TRGH",
    "TRSH",
    "YD",
    "GDPR",
    "UR",
    "PCY",
    "RS",
]
_DISTRIBUTION_REQUIRED_MOVES = ("IPOVALL", "IPOVCH", "YD", "GDPR")
_DISTRIBUTION_REQUIRED_SIGN_TRACKS = ("IPOVALL", "IPOVCH", "TRLOWZ", "RYDPC", "YD", "GDPR")
_DISTRIBUTION_ONE_OF_MOVES = ("UR", "PCY")
_DISTRIBUTION_FLAT_TAIL_CHECKS = ("IPOVALL", "IPOVCH", "YD", "GDPR", "UR")
_RUNTIME_INCLUDE_NAMES = {
    "ipbase.txt": "ipbase.txt",
    "idist_phase1_block.txt": "idp1blk.txt",
}

_TARGET_CSV_NAMES = {
    "IPOVALL": "poverty_all_qtr.csv",
    "IPOVCH": "poverty_child_qtr.csv",
    "IGINIHH": "gini_households_qtr.csv",
    "IMEDRINC": "median_real_income_qtr.csv",
}

_COEFFICIENT_SPECS = {
    "IPOVALL": ("PV0", "PVU", "PVT"),
    "IPOVCH": ("CG0", "CGU", "CGT"),
    "IGINIHH": ("GN0", "GNU", "GNT"),
    "IMEDRINC": ("MD0", "MDR", "MDU"),
}


@dataclass(frozen=True)
class DistributionScenarioSpec:
    variant_id: str
    ui_factor: float
    snap_delta_q: float
    ss_factor: float
    description: str


def _replace_once(text: str, search: str, replace: str) -> str:
    count = text.count(search)
    if count != 1:
        raise ValueError(f"Expected exactly one match for patch anchor: {search!r}; found {count}")
    return text.replace(search, replace, 1)


def _phase1_specs() -> list[DistributionScenarioSpec]:
    return [DistributionScenarioSpec(*item) for item in PHASE1_DISTRIBUTION_SCENARIOS]


def _scenario_name(variant_id: str) -> str:
    return f"ineq_phase1_distribution_{variant_id.replace('-', '_')}"


def _required_moves_for_variant(variant_id: str) -> tuple[str, ...]:
    if variant_id.startswith("ui-"):
        return (*_DISTRIBUTION_REQUIRED_MOVES, "UB")
    if variant_id.startswith("snap-"):
        return (*_DISTRIBUTION_REQUIRED_MOVES, "TRGH")
    if variant_id.startswith("social-security-"):
        return (*_DISTRIBUTION_REQUIRED_MOVES, "TRSH")
    if variant_id.startswith("transfer-package-"):
        return (*_DISTRIBUTION_REQUIRED_MOVES, "UB", "TRGH", "TRSH")
    raise ValueError(f"Unsupported distribution variant: {variant_id}")


def _required_signs_for_variant(variant_id: str) -> tuple[str, ...]:
    if variant_id.startswith("ui-"):
        return (*_DISTRIBUTION_REQUIRED_SIGN_TRACKS, "UB")
    if variant_id.startswith("snap-"):
        return (*_DISTRIBUTION_REQUIRED_SIGN_TRACKS, "TRGH")
    if variant_id.startswith("social-security-"):
        return (*_DISTRIBUTION_REQUIRED_SIGN_TRACKS, "TRSH")
    if variant_id.startswith("transfer-package-"):
        return (*_DISTRIBUTION_REQUIRED_SIGN_TRACKS, "UB", "TRGH", "TRSH")
    raise ValueError(f"Unsupported distribution variant: {variant_id}")


def _expected_sign_for_variant(variant_id: str) -> float:
    if variant_id.endswith("-relief"):
        return 1.0
    if variant_id.endswith("-shock"):
        return -1.0
    raise ValueError(f"Unsupported distribution variant: {variant_id}")


def _load_target_series(var: str) -> pd.Series:
    paths = repo_paths()
    csv_name = _TARGET_CSV_NAMES[var]
    frame = pd.read_csv(paths.data_series_root / csv_name, dtype={"period": str, "value": float})
    return pd.Series(frame["value"].to_list(), index=frame["period"].to_list(), name=var, dtype=float)


def _period_year(token: str) -> int:
    return int(str(token).split(".", 1)[0])


def _latest_transfer_core_baseline_loadformat() -> Path:
    paths = repo_paths()
    report_path = paths.runtime_transfer_reports_root / "run_phase1_transfer_core.json"
    if report_path.exists():
        payload = json.loads(report_path.read_text(encoding="utf-8"))
        scenario = payload.get("scenarios", {}).get("baseline-observed", {})
        loadformat_path = scenario.get("loadformat_path")
        if loadformat_path:
            path = Path(loadformat_path)
            if path.exists():
                return path
    candidates = sorted(paths.runtime_transfer_artifacts_root.glob("*/LOADFORMAT.DAT"))
    if not candidates:
        raise FileNotFoundError(
            "No transfer-core baseline LOADFORMAT.DAT found. Run `fp-ineq run-phase1-transfer-core` first."
        )
    return candidates[-1]


def _load_baseline_regressors() -> pd.DataFrame:
    ensure_fp_wraptr_importable()
    from fp_wraptr.io.loadformat import add_derived_series, read_loadformat

    loadformat_path = _latest_transfer_core_baseline_loadformat()
    periods, series = read_loadformat(loadformat_path)
    series = add_derived_series(series)
    frame = pd.DataFrame(index=pd.Index(periods, name="period"))
    for name in ("UR", "UB", "TRGH", "TRSH", "POP", "PH", "GDPR"):
        values = series.get(name)
        if not values:
            raise KeyError(f"Required regressor series missing from transfer-core baseline: {name}")
        frame[name] = values
    frame["TRLOWZ"] = (frame["UB"] + frame["TRGH"] + frame["TRSH"]) / (frame["POP"] * frame["PH"])
    frame["RYDPC"] = frame["GDPR"]
    yd = series.get("YD")
    if yd:
        frame["YD"] = yd
        frame["RYDPC"] = frame["YD"] / (frame["POP"] * frame["PH"])
    frame["LRYDPC"] = np.log(frame["RYDPC"])
    frame["LGDPR"] = np.log(frame["GDPR"])
    return frame.loc["1990.1":"2025.4"]


def _logit(series: pd.Series) -> pd.Series:
    clipped = series.clip(1e-6, 1 - 1e-6)
    return np.log(clipped / (1.0 - clipped))


def _fit_linear(target: pd.Series, regressors: pd.DataFrame) -> tuple[np.ndarray, dict[str, float]]:
    sample = regressors.join(target.rename("target"), how="inner").dropna()
    X = sample.drop(columns=["target"]).to_numpy(dtype=float)
    y = sample["target"].to_numpy(dtype=float)
    X_design = np.column_stack([np.ones(X.shape[0]), X])
    beta, *_rest = np.linalg.lstsq(X_design, y, rcond=None)
    fitted = X_design @ beta
    rmse = float(np.sqrt(np.mean((y - fitted) ** 2)))
    stats = {
        "nobs": float(X.shape[0]),
        "rmse": rmse,
        "sample_start": str(sample.index[0]),
        "sample_end": str(sample.index[-1]),
    }
    return beta, stats


def _distinct_sensitivity_stats(
    target: pd.Series,
    regressors: pd.DataFrame,
    *,
    main_stats: dict[str, float],
    start: str,
    end: str,
) -> dict[str, float] | None:
    target_window = target.loc[start:end]
    regressors_window = regressors.loc[start:end]
    sample = regressors_window.join(target_window.rename("target"), how="inner").dropna()
    if sample.empty:
        return None
    sample_start = str(sample.index[0])
    sample_end = str(sample.index[-1])
    nobs = float(sample.shape[0])
    if (
        sample_start == main_stats["sample_start"]
        and sample_end == main_stats["sample_end"]
        and nobs == main_stats["nobs"]
    ):
        return None
    _beta, stats = _fit_linear(target_window, regressors_window)
    return stats


def _collapse_series_to_annual(series: pd.Series) -> pd.Series:
    frame = pd.DataFrame({"value": series.astype(float)})
    frame["year"] = [_period_year(idx) for idx in frame.index]
    annual = frame.groupby("year", sort=True)["value"].mean()
    annual.index = annual.index.astype(str)
    annual.name = str(series.name)
    return annual


def _collapse_frame_to_annual(frame: pd.DataFrame) -> pd.DataFrame:
    annual = frame.copy()
    annual["year"] = [_period_year(idx) for idx in annual.index]
    annual = annual.groupby("year", sort=True).mean(numeric_only=True)
    annual.index = annual.index.astype(str)
    return annual


def estimate_phase1_distribution_coefficients() -> dict[str, object]:
    paths = repo_paths()
    regressors_q = _load_baseline_regressors()
    regressors = _collapse_frame_to_annual(regressors_q)

    target_specs = {
        "IPOVALL": (_logit(_collapse_series_to_annual(_load_target_series("IPOVALL"))), regressors[["UR", "TRLOWZ"]]),
        "IPOVCH": (
            _logit(_collapse_series_to_annual(_load_target_series("IPOVCH")))
            - _logit(_collapse_series_to_annual(_load_target_series("IPOVALL"))),
            regressors[["UR", "TRLOWZ"]],
        ),
        "IGINIHH": (_logit(_collapse_series_to_annual(_load_target_series("IGINIHH"))), regressors[["UR", "TRLOWZ"]]),
        "IMEDRINC": (np.log(_collapse_series_to_annual(_load_target_series("IMEDRINC"))), regressors[["LRYDPC", "UR"]]),
    }

    coefficient_values: dict[str, float] = {}
    report_payload: dict[str, object] = {"equations": {}}
    lines = [
        "@ Offline-estimated phase-1 distribution coefficients.",
        "@ Private runtime artifact. Do not publish if it embeds stock-run calibration choices.",
        "",
    ]

    for eq_name, (target, X) in target_specs.items():
        beta, stats = _fit_linear(target, X)
        observed_target = target.dropna()
        sensitivity_stats = _distinct_sensitivity_stats(
            target,
            X,
            main_stats=stats,
            start="2015",
            end="2025",
        )
        symbol_names = _COEFFICIENT_SPECS[eq_name]
        symbol_values = {
            symbol_names[0]: float(beta[0]),
            symbol_names[1]: float(beta[1]),
            symbol_names[2]: float(beta[2]),
        }
        coefficient_values.update(symbol_values)
        report_payload["equations"][eq_name] = {
            "coefficients": symbol_values,
            "calibration_frequency": "annual",
            "effective_nobs": stats["nobs"],
            "target_observed_start": str(observed_target.index[0]),
            "target_observed_end": str(observed_target.index[-1]),
            "main_sample_start": stats["sample_start"],
            "main_sample_end": stats["sample_end"],
            "main_rmse": stats["rmse"],
            "sensitivity": (
                {
                    "sample_start": sensitivity_stats["sample_start"],
                    "sample_end": sensitivity_stats["sample_end"],
                    "effective_nobs": sensitivity_stats["nobs"],
                    "rmse": sensitivity_stats["rmse"],
                }
                if sensitivity_stats is not None
                else None
            ),
        }
        lines.append(f"@ {eq_name}")
        for symbol in symbol_names:
            lines.append(f"CREATE {symbol}={coefficient_values[symbol]:.12g};")
        lines.append("")

    lines.append("RETURN;")

    coeff_text = "\n".join(lines) + "\n"

    report_path = paths.runtime_distribution_reports_root / "estimate_phase1_distribution_coefficients.json"
    paths.runtime_distribution_reports_root.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return {
        "coeff_text": coeff_text,
        "report_path": str(report_path),
        "equations": report_payload["equations"],
    }


def build_phase1_distribution_overlay(*, fp_home: Path) -> dict[str, object]:
    paths = repo_paths()
    fp_home = locate_fp_home(fp_home)
    overlay_root = paths.runtime_distribution_overlay_root
    estimate_payload = estimate_phase1_distribution_coefficients()
    compose_phase1_overlay(
        fp_home=fp_home,
        overlay_root=overlay_root,
        extra_overlay_files=["ipbase.txt", "idist_phase1_block.txt"],
        runtime_name_overrides=_RUNTIME_INCLUDE_NAMES,
        runtime_text_files={"idcoef.txt": str(estimate_payload["coeff_text"])},
        post_patches=[
            {
                "search": "INPUT FILE=idistid.txt;",
                "replace": "INPUT FILE=idistid.txt;\nINPUT FILE=ipbase.txt;\nINPUT FILE=idcoef.txt;",
            },
            {
                "search": "GENR LCUSTZ=LOG(CUST/(PIM*IM));",
                "replace": "GENR LCUSTZ=LOG(CUST/(PIM*IM));\nINPUT FILE=idp1blk.txt;",
            },
        ],
    )

    build_report = {
        "fp_home": str(fp_home),
        "overlay_root": str(overlay_root),
        "coeff_path": str(overlay_root / "idcoef.txt"),
        "coeff_report_path": estimate_payload["report_path"],
        "scenario_ids": [spec.variant_id for spec in _phase1_specs()],
    }
    report_path = paths.runtime_distribution_reports_root / "compose_phase1_distribution_block.json"
    report_path.write_text(json.dumps(build_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return build_report


def write_phase1_distribution_scenarios(*, fp_home: Path) -> list[Path]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.scenarios.config import ScenarioConfig

    paths = repo_paths()
    fp_home = locate_fp_home(fp_home)
    build_phase1_distribution_overlay(fp_home=fp_home)
    paths.runtime_distribution_scenarios_root.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    overlay_dir = paths.runtime_distribution_overlay_root.resolve()
    for spec in _phase1_specs():
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
            track_variables=list(_DISTRIBUTION_TRACK_VARIABLES),
            input_patches=input_patches,
            artifacts_root=str(paths.runtime_distribution_artifacts_root),
        )
        path = paths.runtime_distribution_scenarios_root / f"{spec.variant_id}.yaml"
        config.to_yaml(path)
        written.append(path)
    return written


def _movement_summary(results: dict[str, dict[str, float | None]]) -> dict[str, Any]:
    baseline = results.get("baseline-observed", {})
    tolerance = 1e-9
    comparisons: dict[str, dict[str, float | None]] = {}
    scenario_checks: dict[str, dict[str, Any]] = {}
    for variant_id, values in results.items():
        if variant_id == "baseline-observed":
            continue
        required = _required_moves_for_variant(variant_id)
        required_signs_expected = _required_signs_for_variant(variant_id)
        expected_sign = _expected_sign_for_variant(variant_id)
        required_moves = {name: False for name in required}
        required_signs = {name: False for name in required_signs_expected}
        one_of_moves = {name: False for name in _DISTRIBUTION_ONE_OF_MOVES}
        one_of_signs = {name: False for name in _DISTRIBUTION_ONE_OF_MOVES}
        delta_map: dict[str, float | None] = {}
        for name in _DISTRIBUTION_TRACK_VARIABLES:
            base_value = baseline.get(name)
            scenario_value = values.get(name)
            if base_value is None or scenario_value is None:
                delta_map[name] = None
                continue
            delta = float(scenario_value - base_value)
            delta_map[name] = delta
            if name in required_moves and abs(delta) > tolerance:
                required_moves[name] = True
            if name in required_signs:
                signed_delta = -delta if name in ("IPOVALL", "IPOVCH") else delta
                if (signed_delta * expected_sign) > tolerance:
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


def run_phase1_distribution_block(*, fp_home: Path) -> dict[str, object]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.scenarios import runner as scenario_runner
    from fp_wraptr.scenarios.runner import load_scenario_config

    paths = repo_paths()
    scenario_paths = write_phase1_distribution_scenarios(fp_home=fp_home)
    paths.runtime_distribution_artifacts_root.mkdir(parents=True, exist_ok=True)
    paths.runtime_distribution_reports_root.mkdir(parents=True, exist_ok=True)

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
                output_dir=paths.runtime_distribution_artifacts_root,
            )
            fmout_path = result.output_dir / "fmout.txt"
            fmout_text = fmout_path.read_text(encoding="utf-8", errors="replace") if fmout_path.exists() else ""
            solve_error_sol1 = "Solution error in SOL1." in fmout_text
            loadformat_path = result.output_dir / "LOADFORMAT.DAT"
            if loadformat_path.exists():
                loadformat_series = _read_loadformat_series(loadformat_path)
                first_levels, variant_last_levels = _extract_levels_from_loadformat(
                    loadformat_path,
                    _DISTRIBUTION_TRACK_VARIABLES,
                )
                flat_tail_flags = _flat_tail_flags(loadformat_series, _DISTRIBUTION_FLAT_TAIL_CHECKS)
            else:
                first_levels = _extract_first_levels(result.parsed_output, _DISTRIBUTION_TRACK_VARIABLES)
                variant_last_levels = _extract_last_levels(result.parsed_output, _DISTRIBUTION_TRACK_VARIABLES)
                flat_tail_flags = {name: False for name in _DISTRIBUTION_FLAT_TAIL_CHECKS}
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

    acceptance = _movement_summary(last_levels)
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
        "track_variables": list(_DISTRIBUTION_TRACK_VARIABLES),
        "scenario_paths": [str(path) for path in scenario_paths],
    }
    report_path = paths.runtime_distribution_reports_root / "run_phase1_distribution_block.json"
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "overlay_root": str(paths.runtime_distribution_overlay_root),
        "scenarios_dir": str(paths.runtime_distribution_scenarios_root),
        "artifacts_dir": str(paths.runtime_distribution_artifacts_root),
        "passes": bool(acceptance["passes"]),
    }
