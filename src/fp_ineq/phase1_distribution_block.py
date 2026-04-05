from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .bridge import ensure_fp_wraptr_importable, locate_fp_home
from .paths import repo_paths
from .phase1_catalog import phase1_distribution_specs
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


PHASE1_DISTRIBUTION_SCENARIOS = tuple(
    (
        spec.variant_id,
        spec.ui_factor,
        spec.trgh_delta_q,
        spec.trsh_factor,
        spec.distribution_description,
    )
    for spec in phase1_distribution_specs()
)

_DISTRIBUTION_TRACK_VARIABLES = [
    "IPOVALL",
    "IPOVCH",
    "IGINIHH",
    "IMEDRINC",
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
    "RS",
]
_DISTRIBUTION_REQUIRED_MOVES = ("IPOVALL", "IPOVCH", "YD", "GDPR")
_DISTRIBUTION_REQUIRED_SIGN_TRACKS = ("IPOVALL", "IPOVCH", "TRLOWZ", "RYDPC", "YD", "GDPR")
_DISTRIBUTION_ONE_OF_MOVES = ("UR", "PCY")
_DISTRIBUTION_FLAT_TAIL_CHECKS = ("IPOVALL", "IPOVCH", "YD", "GDPR", "UR")
_RUNTIME_INCLUDE_NAMES = {
    "ipbase.txt": "ipbase.txt",
    "idist_phase1_block.txt": "idp1blk.txt",
    "idist_phase2_wealth_block.txt": "idp2wblk.txt",
}

_TARGET_CSV_NAMES = {
    "IPOVALL": "poverty_all_qtr.csv",
    "IPOVCH": "poverty_child_qtr.csv",
    "IGINIHH": "gini_households_qtr.csv",
    "IMEDRINC": "median_real_income_qtr.csv",
    "IWGAP150": "wealth_share_gap_top1_bottom50_qtr.csv",
}

_COEFFICIENT_SPECS = {
    "IPOVALL": ("PV0", "PVU", "PVT", "PVUI", "PVGH"),
    "IPOVCH": ("CG0", "CGU", "CGT", "CGUI", "CGGH"),
    "IGINIHH": ("GN0", "GNU", "GNT"),
    "IMEDRINC": ("MD0", "MDR", "MDU"),
    "IWGAP150": ("WG0", "WGU", "WGT", "WGA"),
}
_TRANSFER_ZSCORE_SPECS = (
    ("UB", "UBBAR", "UBSTD", "UBZ"),
    ("TRGH", "TRGHBAR", "TRGHSTD", "TRGHZ"),
    ("TRSH", "TRSHBAR", "TRSHSTD", "TRSHZ"),
)
_POVERTY_BASE_REGRESSORS = ("UR", "TRLOWZ")
_POVERTY_DEVIATION_REGRESSORS = ("UIDEV", "GHSHDV")
_POVERTY_RIDGE_ALPHAS = (0.1, 0.3, 1.0, 3.0, 10.0, 30.0)
_DECOMPOSITION_OUTPUT_SPECS = {
    "IPOVALL": {
        "transform": "logit",
        "components": {
            "overall_poverty_state": {
                "equation": "IPOVALL",
                "intercept_symbol": "PV0",
                "regressor_symbols": {
                    "UR": "PVU",
                    "TRLOWZ": "PVT",
                    "UIDEV": "PVUI",
                    "GHSHDV": "PVGH",
                },
            },
        },
    },
    "IPOVCH": {
        "transform": "logit",
        "components": {
            "overall_poverty_state": {
                "equation": "IPOVALL",
                "intercept_symbol": "PV0",
                "regressor_symbols": {
                    "UR": "PVU",
                    "TRLOWZ": "PVT",
                    "UIDEV": "PVUI",
                    "GHSHDV": "PVGH",
                },
            },
            "child_gap_state": {
                "equation": "IPOVCH",
                "intercept_symbol": "CG0",
                "regressor_symbols": {
                    "UR": "CGU",
                    "TRLOWZ": "CGT",
                    "UIDEV": "CGUI",
                    "GHSHDV": "CGGH",
                },
            },
        },
    },
    "IGINIHH": {
        "transform": "logit",
        "components": {
            "household_gini_state": {
                "equation": "IGINIHH",
                "intercept_symbol": "GN0",
                "regressor_symbols": {
                    "UR": "GNU",
                    "TRLOWZ": "GNT",
                },
            },
        },
    },
    "IMEDRINC": {
        "transform": "exp",
        "components": {
            "median_income_state": {
                "equation": "IMEDRINC",
                "intercept_symbol": "MD0",
                "regressor_symbols": {
                    "LRYDPC": "MDR",
                    "UR": "MDU",
                },
            },
        },
    },
}

def _replace_once(text: str, search: str, replace: str) -> str:
    count = text.count(search)
    if count != 1:
        raise ValueError(f"Expected exactly one match for patch anchor: {search!r}; found {count}")
    return text.replace(search, replace, 1)


def _phase1_specs():
    return phase1_distribution_specs()


def _scenario_name(variant_id: str) -> str:
    return f"ineq_phase1_distribution_{variant_id.replace('-', '_')}"


def _required_moves_for_variant(variant_id: str) -> tuple[str, ...]:
    if variant_id.startswith("ui-"):
        return (*_DISTRIBUTION_REQUIRED_MOVES, "UB")
    if variant_id.startswith("federal-transfer-"):
        return (*_DISTRIBUTION_REQUIRED_MOVES, "TRGH")
    if variant_id.startswith("state-local-transfer-"):
        return (*_DISTRIBUTION_REQUIRED_MOVES, "TRSH")
    if variant_id.startswith("transfer-package-") or variant_id.startswith("transfer-composite-"):
        return (*_DISTRIBUTION_REQUIRED_MOVES, "UB", "TRGH", "TRSH")
    raise ValueError(f"Unsupported distribution variant: {variant_id}")


def _required_signs_for_variant(variant_id: str) -> tuple[str, ...]:
    if variant_id.startswith("ui-"):
        return (*_DISTRIBUTION_REQUIRED_SIGN_TRACKS, "UB")
    if variant_id.startswith("federal-transfer-"):
        return (*_DISTRIBUTION_REQUIRED_SIGN_TRACKS, "TRGH")
    if variant_id.startswith("state-local-transfer-"):
        return (*_DISTRIBUTION_REQUIRED_SIGN_TRACKS, "TRSH")
    if variant_id.startswith("transfer-package-") or variant_id.startswith("transfer-composite-"):
        return (*_DISTRIBUTION_REQUIRED_SIGN_TRACKS, "UB", "TRGH", "TRSH")
    raise ValueError(f"Unsupported distribution variant: {variant_id}")


def _expected_sign_for_variant(variant_id: str) -> float:
    if variant_id.endswith("-relief"):
        return 1.0
    if variant_id.endswith("-shock"):
        return -1.0
    if variant_id.startswith("ui-") or variant_id.startswith("transfer-composite-"):
        return 1.0
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
    if not report_path.exists():
        raise FileNotFoundError(
            f"Transfer-core report missing: {report_path}. "
            "Run `fp-ineq run-phase1-transfer-core` first."
        )

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    scenarios = dict(payload.get("scenarios", {}))
    scenario = dict(scenarios.get("baseline-observed", {}))
    loadformat_path = scenario.get("loadformat_path")
    if not loadformat_path:
        raise KeyError(
            "Transfer-core report is missing "
            "`scenarios.baseline-observed.loadformat_path`."
        )

    path = Path(str(loadformat_path))
    if not path.exists():
        raise FileNotFoundError(f"Transfer-core baseline LOADFORMAT.DAT not found: {path}")
    return path


def _load_baseline_regressors() -> pd.DataFrame:
    ensure_fp_wraptr_importable()
    from fp_wraptr.io.loadformat import add_derived_series, read_loadformat

    loadformat_path = _latest_transfer_core_baseline_loadformat()
    periods, series = read_loadformat(loadformat_path)
    series = add_derived_series(series)
    frame = pd.DataFrame(index=pd.Index(periods, name="period"))
    for name in ("UR", "UB", "TRGH", "TRSH", "POP", "PH", "GDPR", "LAAZ"):
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


def _fit_constant_only(target: pd.Series) -> dict[str, float | str]:
    sample = target.dropna().astype(float)
    if sample.empty:
        raise ValueError("Cannot fit constant-only model on an empty sample")
    y = sample.to_numpy(dtype=float)
    constant = float(np.mean(y))
    rmse = float(np.sqrt(np.mean((y - constant) ** 2)))
    return {
        "nobs": float(sample.shape[0]),
        "rmse": rmse,
        "sample_start": str(sample.index[0]),
        "sample_end": str(sample.index[-1]),
        "constant": constant,
    }


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


def _ridge_slopes(y: np.ndarray, X: np.ndarray, *, alpha: float) -> np.ndarray:
    if X.ndim != 2:
        raise ValueError("Ridge fit expects a 2D regressor array")
    if X.shape[1] == 0:
        return np.zeros(0, dtype=float)
    gram = X.T @ X
    penalty = alpha * np.eye(X.shape[1], dtype=float)
    return np.linalg.solve(gram + penalty, X.T @ y)


def _fit_poverty_deviation_ridge(
    target: pd.Series,
    regressors: pd.DataFrame,
    *,
    ridge_alphas: tuple[float, ...] = _POVERTY_RIDGE_ALPHAS,
) -> tuple[np.ndarray, dict[str, float], dict[str, object]]:
    sample = regressors.join(target.rename("target"), how="inner").dropna()
    base_names = list(_POVERTY_BASE_REGRESSORS)
    deviation_names = list(_POVERTY_DEVIATION_REGRESSORS)
    X_base = np.column_stack(
        [
            np.ones(sample.shape[0], dtype=float),
            sample[base_names].to_numpy(dtype=float),
        ]
    )
    X_dev = sample[deviation_names].to_numpy(dtype=float)
    y = sample["target"].to_numpy(dtype=float)

    if X_base.shape[0] == 0:
        raise ValueError("Cannot fit poverty deviation ridge model on an empty sample")

    def _fold_rmse(alpha: float) -> float:
        residual_errors: list[float] = []
        for holdout_index in sample.index:
            train = sample.drop(index=holdout_index)
            holdout = sample.loc[[holdout_index]]
            Xb_train = np.column_stack(
                [
                    np.ones(train.shape[0], dtype=float),
                    train[base_names].to_numpy(dtype=float),
                ]
            )
            Xd_train = train[deviation_names].to_numpy(dtype=float)
            y_train = train["target"].to_numpy(dtype=float)
            base_beta, *_rest = np.linalg.lstsq(Xb_train, y_train, rcond=None)
            residual = y_train - (Xb_train @ base_beta)
            dev_beta = _ridge_slopes(residual, Xd_train, alpha=alpha)
            Xb_holdout = np.column_stack(
                [
                    np.ones(holdout.shape[0], dtype=float),
                    holdout[base_names].to_numpy(dtype=float),
                ]
            )
            Xd_holdout = holdout[deviation_names].to_numpy(dtype=float)
            prediction = float((Xb_holdout @ base_beta + Xd_holdout @ dev_beta)[0])
            actual = float(holdout["target"].iloc[0])
            residual_errors.append(actual - prediction)
        errors = np.asarray(residual_errors, dtype=float)
        return float(np.sqrt(np.mean(errors**2)))

    selected_alpha = min(ridge_alphas, key=lambda alpha: (_fold_rmse(alpha), -alpha))
    base_beta, *_rest = np.linalg.lstsq(X_base, y, rcond=None)
    residual = y - (X_base @ base_beta)
    dev_beta = _ridge_slopes(residual, X_dev, alpha=selected_alpha)
    fitted = X_base @ base_beta + X_dev @ dev_beta
    rmse = float(np.sqrt(np.mean((y - fitted) ** 2)))
    stats = {
        "nobs": float(sample.shape[0]),
        "rmse": rmse,
        "sample_start": str(sample.index[0]),
        "sample_end": str(sample.index[-1]),
    }
    coefficients = np.concatenate([base_beta, dev_beta])
    meta = {
        "ridge_alpha": float(selected_alpha),
        "ridge_alpha_grid": [float(value) for value in ridge_alphas],
        "base_regressors": base_names,
        "deviation_regressors": deviation_names,
    }
    return coefficients, stats, meta


def _fit_restricted(target: pd.Series, regressors: pd.DataFrame) -> dict[str, float | str]:
    if regressors.empty:
        raise ValueError("Restricted benchmark requires at least one regressor")
    best_stats: dict[str, float | str] | None = None
    for name in regressors.columns:
        _beta, stats = _fit_linear(target, regressors[[name]])
        candidate = {
            "regressor": str(name),
            "rmse": stats["rmse"],
            "nobs": stats["nobs"],
            "sample_start": stats["sample_start"],
            "sample_end": stats["sample_end"],
        }
        if best_stats is None or float(candidate["rmse"]) < float(best_stats["rmse"]):
            best_stats = candidate
    assert best_stats is not None
    return best_stats


def _coefficient_sign(value: float, *, tolerance: float = 1e-12) -> int:
    if value > tolerance:
        return 1
    if value < -tolerance:
        return -1
    return 0


def _loo_diagnostics(
    target: pd.Series,
    regressors: pd.DataFrame,
    coeff_names: tuple[str, ...],
    *,
    fit_mode: str = "linear",
) -> dict[str, float | dict[str, int]]:
    sample = regressors.join(target.rename("target"), how="inner").dropna()
    if sample.shape[0] < 2:
        raise ValueError("LOO diagnostics require at least two joined observations")
    if fit_mode == "poverty_deviation_ridge":
        full_beta, _stats, _meta = _fit_poverty_deviation_ridge(sample["target"], sample.drop(columns=["target"]))
    else:
        full_beta, _stats = _fit_linear(sample["target"], sample.drop(columns=["target"]))
    full_signs = {
        name: _coefficient_sign(float(value))
        for name, value in zip(coeff_names, full_beta, strict=True)
    }
    fold_rmses: list[float] = []
    predictions: list[float] = []
    sign_stability = {name: 0 for name in coeff_names}

    for holdout_index in sample.index:
        train = sample.drop(index=holdout_index)
        holdout = sample.loc[[holdout_index]]
        if fit_mode == "poverty_deviation_ridge":
            beta, _fold_stats, _meta = _fit_poverty_deviation_ridge(train["target"], train.drop(columns=["target"]))
        else:
            beta, _fold_stats = _fit_linear(train["target"], train.drop(columns=["target"]))
        X_holdout = holdout.drop(columns=["target"]).to_numpy(dtype=float)
        X_design = np.column_stack([np.ones(X_holdout.shape[0]), X_holdout])
        prediction = float((X_design @ beta)[0])
        actual = float(holdout["target"].iloc[0])
        fold_rmses.append(abs(actual - prediction))
        predictions.append(prediction)
        for name, coefficient in zip(coeff_names, beta, strict=True):
            if _coefficient_sign(float(coefficient)) == full_signs[name]:
                sign_stability[name] += 1

    return {
        "rmse_mean": float(np.mean(fold_rmses)),
        "rmse_std": float(np.std(fold_rmses, ddof=0)),
        "prediction_min": float(min(predictions)),
        "prediction_max": float(max(predictions)),
        "sign_stability": sign_stability,
    }


def _distinct_sensitivity_stats(
    target: pd.Series,
    regressors: pd.DataFrame,
    *,
    main_stats: dict[str, float],
    start: str,
    end: str,
    fit_mode: str = "linear",
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
    if fit_mode == "poverty_deviation_ridge":
        _beta, stats, _meta = _fit_poverty_deviation_ridge(target_window, regressors_window)
    else:
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


def _add_transfer_deviation_basis(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float]]:
    augmented = frame.copy()
    standardization: dict[str, float] = {}
    for source_name, mean_name, std_name, z_name in _TRANSFER_ZSCORE_SPECS:
        series = augmented[source_name].astype(float)
        mean_value = float(series.mean())
        std_value = float(series.std(ddof=0))
        if std_value <= 1e-12:
            raise ValueError(f"Cannot standardize {source_name} with near-zero sample variance")
        augmented[z_name] = (series - mean_value) / std_value
        standardization[mean_name] = mean_value
        standardization[std_name] = std_value
    augmented["UIDEV"] = augmented["UBZ"] - 0.5 * (augmented["TRGHZ"] + augmented["TRSHZ"])
    augmented["GHSHDV"] = augmented["TRGHZ"] - augmented["TRSHZ"]
    return augmented, standardization


def _load_distribution_coefficient_report() -> dict[str, object]:
    paths = repo_paths()
    report_path = paths.runtime_distribution_reports_root / "estimate_phase1_distribution_coefficients.json"
    if not report_path.exists():
        raise FileNotFoundError(
            "Distribution coefficient report missing. Run `estimate_phase1_distribution_coefficients()` first."
        )
    return json.loads(report_path.read_text(encoding="utf-8"))


def _distribution_regressors_from_levels(
    levels: dict[str, float | None],
    *,
    basis_standardization: dict[str, float],
) -> dict[str, float]:
    required = ("UR", "TRLOWZ", "UB", "TRGH", "TRSH", "RYDPC")
    missing = [name for name in required if levels.get(name) is None]
    if missing:
        raise KeyError(f"Distribution decomposition missing required level(s): {', '.join(missing)}")
    state = {
        "UR": float(levels["UR"]),
        "TRLOWZ": float(levels["TRLOWZ"]),
        "UB": float(levels["UB"]),
        "TRGH": float(levels["TRGH"]),
        "TRSH": float(levels["TRSH"]),
        "RYDPC": float(levels["RYDPC"]),
    }
    if state["RYDPC"] <= 0.0:
        raise ValueError("Cannot decompose IMEDRINC with nonpositive RYDPC")
    state["LRYDPC"] = float(np.log(state["RYDPC"]))
    for source_name, mean_name, std_name, z_name in _TRANSFER_ZSCORE_SPECS:
        std_value = float(basis_standardization[std_name])
        if abs(std_value) <= 1e-12:
            raise ValueError(f"Cannot standardize {source_name} with near-zero std")
        state[z_name] = (state[source_name] - float(basis_standardization[mean_name])) / std_value
    state["UIDEV"] = state["UBZ"] - 0.5 * (state["TRGHZ"] + state["TRSHZ"])
    state["GHSHDV"] = state["TRGHZ"] - state["TRSHZ"]
    return state


def _apply_distribution_transform(transform: str, state_value: float) -> float:
    if transform == "logit":
        return float(1.0 / (1.0 + np.exp(-state_value)))
    if transform == "exp":
        return float(np.exp(state_value))
    raise ValueError(f"Unsupported distribution transform: {transform}")


def _distribution_decomposition(
    results: dict[str, dict[str, float | None]],
    coefficient_report: dict[str, object],
) -> dict[str, object]:
    baseline_levels = results.get("baseline-observed")
    if baseline_levels is None:
        raise KeyError("Distribution decomposition requires a baseline-observed scenario")

    equations = dict(coefficient_report.get("equations", {}))
    basis_standardization = dict(
        dict(coefficient_report.get("deviation_basis", {})).get("standardization", {})
    )
    baseline_regressors = _distribution_regressors_from_levels(
        baseline_levels,
        basis_standardization=basis_standardization,
    )

    metadata = {
        "basis_standardization": {name: float(value) for name, value in basis_standardization.items()},
        "outputs": {
            output_name: {
                "transform": str(spec["transform"]),
                "components": list(dict(spec["components"]).keys()),
            }
            for output_name, spec in _DECOMPOSITION_OUTPUT_SPECS.items()
        },
    }
    scenarios: dict[str, dict[str, object]] = {}

    for variant_id, scenario_levels in results.items():
        if variant_id == "baseline-observed":
            continue
        scenario_regressors = _distribution_regressors_from_levels(
            scenario_levels,
            basis_standardization=basis_standardization,
        )
        output_payloads: dict[str, object] = {}
        for output_name, spec in _DECOMPOSITION_OUTPUT_SPECS.items():
            transform = str(spec["transform"])
            component_payloads: dict[str, object] = {}
            bridge_payloads: dict[str, object] = {}
            baseline_state_total = 0.0
            scenario_state_total = 0.0

            for component_name, component_spec in dict(spec["components"]).items():
                equation_name = str(component_spec["equation"])
                coefficients = dict(dict(equations[equation_name]).get("coefficients", {}))
                intercept_symbol = str(component_spec["intercept_symbol"])
                intercept_value = float(coefficients[intercept_symbol])
                baseline_component_state = intercept_value
                scenario_component_state = intercept_value
                term_payloads: dict[str, object] = {}
                for regressor_name, coefficient_symbol in dict(component_spec["regressor_symbols"]).items():
                    coefficient_value = float(coefficients[str(coefficient_symbol)])
                    baseline_input = float(baseline_regressors[str(regressor_name)])
                    scenario_input = float(scenario_regressors[str(regressor_name)])
                    input_delta = scenario_input - baseline_input
                    state_contribution = coefficient_value * input_delta
                    baseline_component_state += coefficient_value * baseline_input
                    scenario_component_state += coefficient_value * scenario_input
                    term_payloads[str(regressor_name)] = {
                        "coefficient_symbol": str(coefficient_symbol),
                        "coefficient": coefficient_value,
                        "baseline_input": baseline_input,
                        "scenario_input": scenario_input,
                        "input_delta": input_delta,
                        "state_contribution": state_contribution,
                    }
                    bridge_entry = bridge_payloads.setdefault(
                        str(regressor_name),
                        {
                            "input_delta": input_delta,
                            "state_contribution": 0.0,
                            "component_terms": [],
                        },
                    )
                    bridge_entry["state_contribution"] = float(bridge_entry["state_contribution"]) + state_contribution
                    bridge_entry["component_terms"].append(
                        {
                            "component": str(component_name),
                            "coefficient_symbol": str(coefficient_symbol),
                            "coefficient": coefficient_value,
                            "state_contribution": state_contribution,
                        }
                    )
                component_payloads[str(component_name)] = {
                    "equation": equation_name,
                    "intercept_symbol": intercept_symbol,
                    "intercept": intercept_value,
                    "baseline_state": baseline_component_state,
                    "scenario_state": scenario_component_state,
                    "state_delta": scenario_component_state - baseline_component_state,
                    "term_contributions": term_payloads,
                }
                baseline_state_total += baseline_component_state
                scenario_state_total += scenario_component_state

            observed_baseline_output = float(baseline_levels[output_name])
            observed_scenario_output = float(scenario_levels[output_name])
            reconstructed_baseline_output = _apply_distribution_transform(transform, baseline_state_total)
            reconstructed_scenario_output = _apply_distribution_transform(transform, scenario_state_total)
            state_delta = scenario_state_total - baseline_state_total
            for bridge_entry in bridge_payloads.values():
                if abs(state_delta) > 1e-12:
                    bridge_entry["share_of_state_delta"] = float(bridge_entry["state_contribution"]) / state_delta
                else:
                    bridge_entry["share_of_state_delta"] = 0.0

            output_payloads[output_name] = {
                "transform": transform,
                "baseline_output": observed_baseline_output,
                "scenario_output": observed_scenario_output,
                "observed_output_delta": observed_scenario_output - observed_baseline_output,
                "reconstructed_baseline_output": reconstructed_baseline_output,
                "reconstructed_scenario_output": reconstructed_scenario_output,
                "reconstructed_output_delta": reconstructed_scenario_output - reconstructed_baseline_output,
                "reconstruction_error": (observed_scenario_output - observed_baseline_output)
                - (reconstructed_scenario_output - reconstructed_baseline_output),
                "baseline_state": baseline_state_total,
                "scenario_state": scenario_state_total,
                "state_delta": state_delta,
                "bridge_contributions": bridge_payloads,
                "components": component_payloads,
            }
        scenarios[variant_id] = output_payloads

    return {
        "metadata": metadata,
        "scenarios": scenarios,
    }


def estimate_phase1_distribution_coefficients() -> dict[str, object]:
    paths = repo_paths()
    regressors_q = _load_baseline_regressors()
    regressors, basis_standardization = _add_transfer_deviation_basis(_collapse_frame_to_annual(regressors_q))

    target_specs = {
        "IPOVALL": (
            _logit(_collapse_series_to_annual(_load_target_series("IPOVALL"))),
            regressors[["UR", "TRLOWZ", "UIDEV", "GHSHDV"]],
            "poverty_deviation_ridge",
        ),
        "IPOVCH": (
            _logit(_collapse_series_to_annual(_load_target_series("IPOVCH")))
            - _logit(_collapse_series_to_annual(_load_target_series("IPOVALL"))),
            regressors[["UR", "TRLOWZ", "UIDEV", "GHSHDV"]],
            "poverty_deviation_ridge",
        ),
        "IGINIHH": (
            _logit(_collapse_series_to_annual(_load_target_series("IGINIHH"))),
            regressors[["UR", "TRLOWZ"]],
            "linear",
        ),
        "IMEDRINC": (
            np.log(_collapse_series_to_annual(_load_target_series("IMEDRINC"))),
            regressors[["LRYDPC", "UR"]],
            "linear",
        ),
        "IWGAP150": (
            np.log(_collapse_series_to_annual(_load_target_series("IWGAP150"))),
            regressors[["UR", "TRLOWZ", "LAAZ"]],
            "linear",
        ),
    }
    restricted_specs = {
        "IPOVALL": regressors[["UR"]],
        "IPOVCH": regressors[["UR"]],
        "IGINIHH": regressors[["UR"]],
        "IMEDRINC": regressors[["LRYDPC"]],
        "IWGAP150": regressors[["UR", "TRLOWZ", "LAAZ"]],
    }

    coefficient_values: dict[str, float] = {}
    report_payload: dict[str, object] = {
        "equations": {},
        "deviation_basis": {
            "standardization": basis_standardization,
            "ridge_alpha_grid": [float(value) for value in _POVERTY_RIDGE_ALPHAS],
        },
    }
    lines = [
        "@ Offline-estimated phase-1 distribution coefficients.",
        "@ Private runtime artifact. Do not publish if it embeds stock-run calibration choices.",
        "",
        "@ Transfer-channel deviation basis standardization.",
    ]
    for symbol_name, value in basis_standardization.items():
        lines.append(f"CREATE {symbol_name}={value:.12g};")
    lines.append("")

    for eq_name, (target, X, fit_mode) in target_specs.items():
        if fit_mode == "poverty_deviation_ridge":
            beta, stats, fit_meta = _fit_poverty_deviation_ridge(target, X)
        else:
            beta, stats = _fit_linear(target, X)
            fit_meta = {}
        constant_only = _fit_constant_only(target)
        restricted = _fit_restricted(target, restricted_specs[eq_name])
        loo = _loo_diagnostics(target, X, _COEFFICIENT_SPECS[eq_name], fit_mode=fit_mode)
        observed_target = target.dropna()
        sensitivity_stats = _distinct_sensitivity_stats(
            target,
            X,
            main_stats=stats,
            start="2015",
            end="2025",
            fit_mode=fit_mode,
        )
        symbol_names = _COEFFICIENT_SPECS[eq_name]
        symbol_values = {
            symbol_name: float(coefficient)
            for symbol_name, coefficient in zip(symbol_names, beta, strict=True)
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
            "benchmarks": {
                "constant_only": float(constant_only["rmse"]),
                "restricted": float(restricted["rmse"]),
                "restricted_regressor": str(restricted["regressor"]),
            },
            "loo": loo,
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
            "fit_mode": fit_mode,
        }
        if fit_meta:
            report_payload["equations"][eq_name]["ridge"] = fit_meta
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
        "deviation_basis": report_payload["deviation_basis"],
    }


def build_phase1_distribution_overlay(
    *,
    fp_home: Path,
    overlay_root: Path | None = None,
    reports_root: Path | None = None,
    report_name: str = "compose_phase1_distribution_block.json",
    experimental_patch_ids: list[str] | tuple[str, ...] = (),
) -> dict[str, object]:
    paths = repo_paths()
    fp_home = locate_fp_home(fp_home)
    overlay_root = overlay_root or paths.runtime_distribution_overlay_root
    reports_root = reports_root or paths.runtime_distribution_reports_root
    estimate_payload = estimate_phase1_distribution_coefficients()
    compose_payload = compose_phase1_overlay(
        fp_home=fp_home,
        overlay_root=overlay_root,
        extra_overlay_files=["ipbase.txt", "idist_phase1_block.txt", "idist_phase2_wealth_block.txt"],
        runtime_name_overrides=_RUNTIME_INCLUDE_NAMES,
        runtime_text_files={"idcoef.txt": str(estimate_payload["coeff_text"])},
        post_patches=[
            {
                "search": "INPUT FILE=idistid.txt;",
                "replace": "INPUT FILE=idistid.txt;\nINPUT FILE=ipbase.txt;\nINPUT FILE=idcoef.txt;",
            },
            {
                "search": "GENR LCUSTZ=LOG(CUST/(PIM*IM));",
                "replace": "GENR LCUSTZ=LOG(CUST/(PIM*IM));\nINPUT FILE=idp1blk.txt;\nINPUT FILE=idp2wblk.txt;",
            },
        ],
        experimental_patch_ids=experimental_patch_ids,
    )

    build_report = {
        "fp_home": str(fp_home),
        "overlay_root": str(overlay_root),
        "coeff_path": str(overlay_root / "idcoef.txt"),
        "coeff_report_path": estimate_payload["report_path"],
        "scenario_ids": [spec.variant_id for spec in _phase1_specs()],
        "patch_ids": compose_payload["patch_ids"],
        "experimental_patch_ids": [str(item) for item in experimental_patch_ids],
    }
    reports_root.mkdir(parents=True, exist_ok=True)
    report_path = reports_root / report_name
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
        if abs(spec.trgh_delta_q) > 1e-12:
            input_patches["CREATE SNAPDELTAQ=0;"] = f"CREATE SNAPDELTAQ={spec.trgh_delta_q:.12g};"
        if abs(spec.trsh_factor - 1.0) > 1e-12:
            input_patches["CREATE SSFAC=1;"] = f"CREATE SSFAC={spec.trsh_factor:.12g};"
        config = ScenarioConfig(
            name=_scenario_name(spec.variant_id),
            description=spec.distribution_description,
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
    first_levels_map: dict[str, dict[str, float | None]] = {}
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
            first_levels_map[variant_id] = first_levels
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
    coefficient_report = _load_distribution_coefficient_report()
    decomposition_first = _distribution_decomposition(first_levels_map, coefficient_report)
    decomposition_last = _distribution_decomposition(last_levels, coefficient_report)
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
        "decomposition": {
            "metadata": decomposition_last["metadata"],
            "first_levels": decomposition_first["scenarios"],
            "last_levels": decomposition_last["scenarios"],
        },
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
