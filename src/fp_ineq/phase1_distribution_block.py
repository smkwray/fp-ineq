from __future__ import annotations

import csv
import json
from pathlib import Path
import re
from typing import Any

import numpy as np
import pandas as pd
import yaml

from .bridge import ensure_fp_wraptr_importable, locate_fp_home
from .paths import repo_paths
from .phase1_catalog import phase1_distribution_specs, phase1_scenario_by_variant
from .phase1_compose import compose_phase1_overlay
from .phase1_ui import (
    _extract_first_levels,
    _extract_last_levels,
    _extract_levels_from_loadformat,
    _flat_tail_flags,
    _read_loadformat_payload,
    _read_loadformat_series,
)

__all__ = [
    "analyze_phase1_distribution_canonical_blocker_traces",
    "analyze_phase1_distribution_canonical_solved_path",
    "assess_phase1_distribution_canonical_parity",
    "assess_phase1_distribution_backend_boundary",
    "PHASE1_DISTRIBUTION_SCENARIOS",
    "analyze_phase1_distribution_driver_gap",
    "analyze_phase1_distribution_first_levels",
    "analyze_phase1_distribution_transfer_macro_block",
    "analyze_phase1_distribution_ui_attenuation",
    "build_phase1_distribution_overlay",
    "compare_phase1_distribution_reports",
    "compare_phase1_distribution_backends",
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
    "GDPD",
    "YD",
    "GDPR",
    "UR",
    "PCY",
    "RS",
    "THG",
    "THS",
    "RECG",
    "RECS",
    "SGP",
    "SSP",
]
_DISTRIBUTION_REQUIRED_MOVES = ("IPOVALL", "IPOVCH", "YD", "GDPR")
_DISTRIBUTION_REQUIRED_SIGN_TRACKS = ("IPOVALL", "IPOVCH", "TRLOWZ", "RYDPC", "YD", "GDPR")
_DISTRIBUTION_ONE_OF_MOVES = ("UR", "PCY")
_DISTRIBUTION_FLAT_TAIL_CHECKS = ("IPOVALL", "IPOVCH", "YD", "GDPR", "UR")
_PRIVATE_PACKAGE_EVIDENCE_TRACKS = ("THG", "THS", "RECG", "RECS", "SGP", "SSP", "PKGGROSS", "PKGFIN", "PKGNET")
_PRIVATE_PACKAGE_BALANCE_TOLERANCE = 1e-9
_DISTRIBUTION_FORECAST_START = "2026.1"
_DISTRIBUTION_FORECAST_END = "2029.4"
_DISTRIBUTION_HISTORY_SEEDED_THROUGH = "2025.4"
_DISTRIBUTION_FIRST_LEVEL_GAP_VARIANT_IDS = ("ui-relief", "transfer-composite-medium")
_DISTRIBUTION_FIRST_LEVEL_GAP_VARIABLES = (
    "UB",
    "TRGH",
    "TRSH",
    "THG",
    "THS",
    "YD",
    "GDPR",
    "RYDPC",
    "RS",
    "PCY",
    "UR",
    "GDPD",
    "TRLOWZ",
    "IPOVALL",
    "IPOVCH",
)
_DISTRIBUTION_BOUNDARY_UI_VARIABLES = ("UB", "YD", "GDPR", "RYDPC", "TRLOWZ", "IPOVALL", "IPOVCH")
_DISTRIBUTION_BOUNDARY_TRANSFER_DIRECT_VARIABLES = ("TRGH", "TRSH", "THG", "THS", "TRLOWZ", "IPOVALL", "IPOVCH")
_DISTRIBUTION_BOUNDARY_TRANSFER_MACRO_VARIABLES = ("YD", "GDPR", "RYDPC", "RS", "PCY", "UR", "GDPD")
_DISTRIBUTION_UI_ATTENUATION_CORE_VARIABLES = ("UB", "TRLOWZ", "IPOVALL", "IPOVCH", "RYDPC")
_DISTRIBUTION_FORECAST_ONLY_SERIES = ("IPOVALL", "IPOVCH", "IGINIHH", "IMEDRINC")
_DISTRIBUTION_FORECAST_NOTE = (
    "The integrated distribution block seeds history through 2025.4 and solves these series endogenously from 2026.1 onward."
)
_DISTRIBUTION_DEFAULT_BACKEND = "fp-r"
_DISTRIBUTION_COMPARE_VARIANT_IDS = ("baseline-observed", "ui-relief", "transfer-composite-medium")
_DISTRIBUTION_COMPARE_VARIABLES = ("TRLOWZ", "IPOVALL", "IPOVCH", "RYDPC")
_DISTRIBUTION_IDENTITY_VALIDATION_FAMILY_IDS = ("ui", "transfer-composite")
_DISTRIBUTION_REQUIRED_IDENTITY_IDS = ("ub_scaled", "thg_financing", "ths_financing")
_DISTRIBUTION_REQUIRED_IDENTITY_IDS_TRANSFORMED_LHS = ("ub_unscaled", "thg_financing", "ths_financing")
_DISTRIBUTION_POLICY_GAP_VARIANT_IDS = ("ui-relief", "transfer-composite-medium")
_DISTRIBUTION_POLICY_GAP_VARIABLES = ("LUB", "UB", "TRLOWZ", "IPOVALL", "IPOVCH", "RYDPC")
_DISTRIBUTION_DRIVER_GAP_UI_VARIANT_ID = "ui-relief"
_DISTRIBUTION_DRIVER_GAP_TRANSFER_VARIANT_ID = "transfer-composite-medium"
_DISTRIBUTION_DRIVER_GAP_PERIODS = ("2026.1", "2029.4")
_DISTRIBUTION_DRIVER_GAP_UI_VARIABLES = ("LUB", "UB", "UIFAC", "TRLOWZ", "IPOVALL", "IPOVCH", "RYDPC", "YD", "GDPR")
_DISTRIBUTION_CANONICAL_TRACE_UI_VARIABLES = ("UIFAC", "LUB", "UB", "TRLOWZ", "YD", "GDPR", "RYDPC", "UR", "PCY")
_DISTRIBUTION_DRIVER_GAP_TRANSFER_VARIABLES = (
    "YD",
    "PH",
    "POP",
    "RYDPC",
    "GDPR",
    "UR",
    "PCY",
    "RS",
    "GDPD",
    "TRLOWZ",
    "IPOVALL",
    "IPOVCH",
    "LUB",
    "UB",
    "UIFAC",
    "TRGH",
    "TRSH",
    "RECG",
    "RECS",
    "SGP",
    "SSP",
)
_DISTRIBUTION_CANONICAL_TRACE_TRANSFER_VARIABLES = (
    "INTGZ",
    "AAG",
    "INTG",
    "LJF1",
    "JF",
    "TRLOWZ",
    "YD",
    "GDPR",
    "RYDPC",
    "UR",
    "PCY",
    "RS",
)
_DISTRIBUTION_DRIVER_GAP_TRANSFER_PROPAGATION_VARIABLES = (
    "RS",
    "YD",
    "GDPR",
    "RYDPC",
    "PCY",
    "UR",
    "GDPD",
    "UB",
    "TRGH",
    "TRSH",
    "RECG",
    "RECS",
    "SGP",
    "SSP",
)
_DISTRIBUTION_IDENTITY_EXPRESSIONS = {
    "ub_scaled": "UB - EXP(LUB) * UIFAC",
    "ub_unscaled": "UB - EXP(LUB)",
    "thg_financing": "THG - (D1G * YT + TFEDSHR * (UB - UB / UIFAC + SNAPDELTAQ * GDPD))",
    "ths_financing": "THS - (D1S * YT + TSLSHR * (TRSH - TRSH / SSFAC))",
}
_RUNTIME_INCLUDE_NAMES = {
    "ipbase.txt": "ipbase.txt",
    "idist_phase1_block.txt": "idp1blk.txt",
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


def _phase1_specs(variant_ids: tuple[str, ...] | list[str] | None = None):
    specs = phase1_distribution_specs()
    if not variant_ids:
        return specs
    order = [str(item) for item in variant_ids]
    spec_map = {spec.variant_id: spec for spec in specs}
    missing = [variant_id for variant_id in order if variant_id not in spec_map]
    if missing:
        raise KeyError(f"Unknown distribution variant(s): {', '.join(missing)}")
    return [spec_map[variant_id] for variant_id in order]


def _tagged_runtime_dir(base: Path, runtime_tag: str | None = None) -> Path:
    token = str(runtime_tag or "").strip()
    if not token:
        return base
    return base.parent / f"{base.name}-{token}"


def _tagged_report_path(base: Path, runtime_tag: str | None = None) -> Path:
    token = str(runtime_tag or "").strip()
    if not token:
        return base
    return base.with_name(f"{base.stem}.{token}{base.suffix}")


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
    if variant_id.startswith("transfer-package-"):
        return (*_DISTRIBUTION_REQUIRED_SIGN_TRACKS, "UB", "TRGH", "TRSH")
    if variant_id.startswith("transfer-composite-"):
        return ("IPOVALL", "IPOVCH", "TRLOWZ", "RYDPC", "UB", "TRGH", "TRSH")
    raise ValueError(f"Unsupported distribution variant: {variant_id}")


def _expected_sign_for_variant(variant_id: str) -> float:
    if variant_id.endswith("-relief"):
        return 1.0
    if variant_id.endswith("-shock"):
        return -1.0
    if variant_id.startswith("ui-") or variant_id.startswith("transfer-composite-"):
        return 1.0
    raise ValueError(f"Unsupported distribution variant: {variant_id}")


def _scenario_input_patches(spec) -> dict[str, str]:
    # These policy controls must only move inside the forecast window. Keep
    # them out of global CREATE patches and layer them through scenario
    # exogenous overrides instead.
    return {}


def _scenario_overrides(spec) -> dict[str, dict[str, float | str]]:
    overrides: dict[str, dict[str, float | str]] = {}
    if abs(spec.ui_factor - 1.0) > 1e-12:
        overrides["UIFAC"] = {"method": "SAMEVALUE", "value": float(spec.ui_factor)}
    if abs(spec.trgh_delta_q) > 1e-12:
        overrides["SNAPDELTAQ"] = {"method": "SAMEVALUE", "value": float(spec.trgh_delta_q)}
    if abs(spec.trsh_factor - 1.0) > 1e-12:
        overrides["SSFAC"] = {"method": "SAMEVALUE", "value": float(spec.trsh_factor)}
    trfin_fed_share = float(getattr(spec, "trfin_fed_share", 0.0))
    trfin_sl_share = float(getattr(spec, "trfin_sl_share", 0.0))
    if abs(trfin_fed_share) > 1e-12:
        overrides["TFEDSHR"] = {"method": "SAMEVALUE", "value": trfin_fed_share}
    if abs(trfin_sl_share) > 1e-12:
        overrides["TSLSHR"] = {"method": "SAMEVALUE", "value": trfin_sl_share}
    return overrides


def _scenario_fpr_settings(spec, backend: str) -> dict[str, str]:
    if str(backend).strip().lower() not in {"fp-r", "fp_r", "fpr"}:
        return {}
    if abs(float(spec.ui_factor) - 1.0) <= 1e-12:
        return {}
    return {"exogenous_equation_target_policy": "retain_reduced_eq_only"}


def _setupsolve_compose_post_patches(statement: str | None) -> list[dict[str, str]]:
    text = (statement or "").strip()
    if not text:
        return []
    if not text.endswith(";"):
        text = f"{text};"
    return [
        {
            "search": "SETUPEST ALT2SLS\n@SETUPEST DIVIDET;",
            "replace": f"SETUPEST ALT2SLS\n{text}\n@SETUPEST DIVIDET;",
        }
    ]


def _tail_text(value: str | None, limit: int = 4000) -> str | None:
    text = (value or "").strip()
    if not text:
        return None
    if len(text) <= limit:
        return text
    return text[-limit:]


def _merge_variable_override_maps(
    base: dict[str, dict[str, float | str]] | None,
    additions: dict[str, dict[str, float | str]] | None,
) -> dict[str, dict[str, float | str]]:
    merged: dict[str, dict[str, float | str]] = {
        str(name): dict(payload) for name, payload in dict(base or {}).items()
    }
    for name, payload in dict(additions or {}).items():
        merged[str(name)] = {**dict(merged.get(str(name), {})), **dict(payload)}
    return merged


def _merge_mapping_values(
    base: dict[str, Any] | None,
    additions: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = {str(name): value for name, value in dict(base or {}).items()}
    for name, value in dict(additions or {}).items():
        merged[str(name)] = value
    return merged


def _merge_runtime_text_files(
    base: dict[str, str],
    *,
    overrides: dict[str, str] | None = None,
    appends: dict[str, str] | None = None,
) -> dict[str, str]:
    merged = {str(name): str(value) for name, value in dict(base).items()}
    for name, value in dict(overrides or {}).items():
        merged[str(name)] = str(value)
    for name, value in dict(appends or {}).items():
        token = str(name)
        if token not in merged:
            merged[token] = str(value)
            continue
        existing = merged[token].rstrip()
        suffix = str(value)
        stripped = existing.rstrip()
        if stripped.endswith("RETURN;"):
            return_index = stripped.rfind("RETURN;")
            prefix = stripped[:return_index].rstrip()
            pieces = [piece for piece in (prefix, suffix.strip(), "RETURN;") if piece]
            merged[token] = "\n".join(pieces) + "\n"
        else:
            merged[token] = f"{existing}\n{suffix.lstrip()}"
    return merged


def _replace_text_once(text: str, search: str, replace: str) -> str:
    count = str(text).count(str(search))
    if count != 1:
        raise ValueError(f"Expected exactly one match for patch anchor: {search!r}; found {count}")
    return str(text).replace(str(search), str(replace), 1)


def _apply_runtime_text_post_patches(
    runtime_text_files: dict[str, str],
    *,
    fp_home: Path,
    post_patches: dict[str, list[dict[str, str]] | tuple[dict[str, str], ...]] | None = None,
) -> dict[str, str]:
    patched = {str(name): str(value) for name, value in dict(runtime_text_files).items()}
    for file_name, file_patches in dict(post_patches or {}).items():
        token = str(file_name)
        if token in patched:
            text = patched[token]
        else:
            source_path = Path(fp_home) / token
            if not source_path.exists():
                raise FileNotFoundError(f"Runtime text patch target not found in fp_home: {source_path}")
            text = source_path.read_text(encoding="utf-8", errors="replace")
        for patch in tuple(file_patches or ()):
            text = _replace_text_once(text, str(patch["search"]), str(patch["replace"]))
        patched[token] = text
    return patched


def _variant_level_config(
    mapping: dict[str, Any] | None,
    variant_id: str,
) -> dict[str, Any]:
    payload = dict(mapping or {})
    merged: dict[str, Any] = {}
    for key in ("__all__", "*"):
        if key in payload and payload[key] is not None:
            merged.update(dict(payload[key]))
    if variant_id in payload and payload[variant_id] is not None:
        merged.update(dict(payload[variant_id]))
    return merged


def _derive_private_package_levels(
    levels: dict[str, float | None],
    spec,
) -> dict[str, float | None]:
    ub = levels.get("UB")
    trsh = levels.get("TRSH")
    gdpd = levels.get("GDPD")

    if ub is None or trsh is None or gdpd is None:
        return {"PKGGROSS": None, "PKGFIN": None, "PKGNET": None}

    ui_component = 0.0 if abs(spec.ui_factor - 1.0) <= 1e-12 else float(ub) - (float(ub) / float(spec.ui_factor))
    federal_component = float(spec.trgh_delta_q) * float(gdpd)
    state_local_component = (
        0.0 if abs(spec.trsh_factor - 1.0) <= 1e-12 else float(trsh) - (float(trsh) / float(spec.trsh_factor))
    )
    gross = ui_component + federal_component + state_local_component
    fin = float(spec.trfin_fed_share) * (ui_component + federal_component) + float(spec.trfin_sl_share) * state_local_component
    return {
        "PKGGROSS": gross,
        "PKGFIN": fin,
        "PKGNET": gross - fin,
    }


def _load_target_series(var: str) -> pd.Series:
    paths = repo_paths()
    csv_name = _TARGET_CSV_NAMES[var]
    frame = pd.read_csv(paths.data_series_root / csv_name, dtype={"period": str, "value": float})
    return pd.Series(frame["value"].to_list(), index=frame["period"].to_list(), name=var, dtype=float)


def _period_year(token: str) -> int:
    return int(str(token).split(".", 1)[0])


def _period_key(token: str) -> tuple[int, int]:
    year, quarter = str(token).split(".", 1)
    return int(year), int(quarter)


def _periods_in_forecast_window(
    periods: list[str],
    *,
    forecast_start: str | None = None,
    forecast_end: str | None = None,
) -> list[int]:
    start_key = _period_key(str(forecast_start or _DISTRIBUTION_FORECAST_START))
    end_key = _period_key(str(forecast_end or _DISTRIBUTION_FORECAST_END))
    selected: list[int] = []
    for idx, period in enumerate(periods):
        key = _period_key(period)
        if start_key <= key <= end_key:
            selected.append(idx)
    return selected


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


def _identity_validation_variant_ids(
    variant_ids: tuple[str, ...] | list[str] | None = None,
) -> list[str]:
    if variant_ids:
        return [str(item) for item in variant_ids]
    return [
        spec.variant_id
        for spec in phase1_distribution_specs()
        if spec.family_id in _DISTRIBUTION_IDENTITY_VALIDATION_FAMILY_IDS
    ]


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


def _format_fp_number(value: float) -> str:
    return f"{float(value):.12g}"


def _format_fp_runtime_number(value: float) -> str:
    return f"{float(value):.6g}"


def _render_runtime_distribution_block(coefficient_report: dict[str, object]) -> str:
    equations = dict(coefficient_report.get("equations", {}))
    poverty = dict(dict(equations.get("IPOVALL", {})).get("coefficients", {}))
    child_gap = dict(dict(equations.get("IPOVCH", {})).get("coefficients", {}))
    gini = dict(dict(equations.get("IGINIHH", {})).get("coefficients", {}))
    medinc = dict(dict(equations.get("IMEDRINC", {})).get("coefficients", {}))
    basis = dict(dict(coefficient_report.get("deviation_basis", {})).get("standardization", {}))

    trlowz = "(UB+TRGH+TRSH)/(POP*PH)"
    ubz = f"(UB-({_format_fp_runtime_number(basis['UBBAR'])}))/({_format_fp_runtime_number(basis['UBSTD'])})"
    trghz = (
        f"(TRGH-({_format_fp_runtime_number(basis['TRGHBAR'])}))/({_format_fp_runtime_number(basis['TRGHSTD'])})"
    )
    trshz = (
        f"(TRSH-({_format_fp_runtime_number(basis['TRSHBAR'])}))/({_format_fp_runtime_number(basis['TRSHSTD'])})"
    )
    uidev = f"UBZ-0.5*(({trghz})+({trshz}))".replace("UBZ", ubz)
    ghshdv = f"({trghz})-({trshz})"

    pov_state_core = (
        f"({_format_fp_runtime_number(poverty['PV0'])})"
        f"+({_format_fp_runtime_number(poverty['PVU'])})*UR"
        f"+({_format_fp_runtime_number(poverty['PVT'])})*TRLOWZ"
    )
    pov_state_dev = (
        f"({_format_fp_runtime_number(poverty['PVUI'])})*UIDEV"
        f"+({_format_fp_runtime_number(poverty['PVGH'])})*GHSHDV"
    )
    child_gap_core = (
        f"({_format_fp_runtime_number(child_gap['CG0'])})"
        f"+({_format_fp_runtime_number(child_gap['CGU'])})*UR"
        f"+({_format_fp_runtime_number(child_gap['CGT'])})*TRLOWZ"
    )
    child_gap_dev = (
        f"({_format_fp_runtime_number(child_gap['CGUI'])})*UIDEV"
        f"+({_format_fp_runtime_number(child_gap['CGGH'])})*GHSHDV"
    )
    gini_state = (
        f"({_format_fp_runtime_number(gini['GN0'])})"
        f"+({_format_fp_runtime_number(gini['GNU'])})*UR"
        f"+({_format_fp_runtime_number(gini['GNT'])})*TRLOWZ"
    )
    medinc_state = (
        f"({_format_fp_runtime_number(medinc['MD0'])})"
        f"+({_format_fp_runtime_number(medinc['MDR'])})*LOG(RYDPC)"
        f"+({_format_fp_runtime_number(medinc['MDU'])})*UR"
    )

    lines = [
        "@ Runtime-trimmed phase-1 distribution block.",
        "@ Coefficients are inlined to stay under the stock deck MAXY limit.",
        "",
        f"GENR TRLOWZ={trlowz};",
        f"GENR UBZ={ubz};",
        f"GENR TRGHZ={trghz};",
        f"GENR TRSHZ={trshz};",
        "GENR UIDEV=UBZ-0.5*(TRGHZ+TRSHZ);",
        "GENR GHSHDV=TRGHZ-TRSHZ;",
        "",
        f"IDENT LPOVA={pov_state_core};",
        f"IDENT LPOVB={pov_state_dev};",
        "IDENT LPOVALL=LPOVA+LPOVB;",
        f"IDENT LPOCA={child_gap_core};",
        f"IDENT LPOCB={child_gap_dev};",
        "IDENT LPOVCHGAP=LPOCA+LPOCB;",
        "IDENT LPOVCHLVL=LPOVALL+LPOVCHGAP;",
        f"IDENT LGINIHH={gini_state};",
        f"IDENT LMEDINC={medinc_state};",
        "IDENT IPOVALL=EXP(LPOVALL)/(1+EXP(LPOVALL));",
        "IDENT IPOVCH=EXP(LPOVCHLVL)/(1+EXP(LPOVCHLVL));",
        "IDENT IGINIHH=EXP(LGINIHH)/(1+EXP(LGINIHH));",
        "IDENT IMEDRINC=EXP(LMEDINC);",
        "",
        "RETURN;",
        "",
    ]
    return "\n".join(lines)


def _distribution_bridge_parse_errors(fmout_text: str) -> list[str]:
    lines = fmout_text.splitlines()
    saw_distribution_block = False
    errors: list[str] = []
    for idx, line in enumerate(lines):
        if "INPUT FILE=idp1blk.txt" in line:
            saw_distribution_block = True
            continue
        if not saw_distribution_block or "UNRECOGNIZABLE VARIABLE" not in line:
            continue
        snippet = [line.strip()]
        if idx + 1 < len(lines):
            next_line = lines[idx + 1].strip()
            if next_line:
                snippet.append(next_line)
        errors.append(" | ".join(snippet))
    return errors


def _read_fp_r_series_levels(
    series_path: Path,
    variables: list[str],
    *,
    forecast_start: str | None = None,
    forecast_end: str | None = None,
) -> tuple[dict[str, float | None], dict[str, float | None]]:
    first_levels = {name: None for name in variables}
    last_levels = {name: None for name in variables}
    if not series_path.exists():
        return first_levels, last_levels

    with series_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    if not rows:
        return first_levels, last_levels

    def _coerce(row: dict[str, str], name: str) -> float | None:
        raw = str(row.get(name, "")).strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    def _derived(row: dict[str, str], name: str) -> float | None:
        if name == "TRLOWZ":
            ub = _coerce(row, "UB")
            trgh = _coerce(row, "TRGH")
            trsh = _coerce(row, "TRSH")
            pop = _coerce(row, "POP")
            ph = _coerce(row, "PH")
            if None in (ub, trgh, trsh, pop, ph):
                return None
            denom = float(pop) * float(ph)
            if abs(denom) <= 1e-12:
                return None
            return (float(ub) + float(trgh) + float(trsh)) / denom
        if name == "RYDPC":
            yd = _coerce(row, "YD")
            pop = _coerce(row, "POP")
            ph = _coerce(row, "PH")
            if None in (yd, pop, ph):
                return None
            denom = float(pop) * float(ph)
            if abs(denom) <= 1e-12:
                return None
            return float(yd) / denom
        return None

    start_period = str(forecast_start or "").strip()
    end_period = str(forecast_end or "").strip()
    first_row = rows[0]
    last_row = rows[-1]
    if start_period:
        for row in rows:
            if str(row.get("period", "")).strip() == start_period:
                first_row = row
                break
    if end_period:
        for row in rows:
            if str(row.get("period", "")).strip() == end_period:
                last_row = row
                break
    for name in variables:
        first_levels[name] = _coerce(first_row, name)
        if first_levels[name] is None:
            first_levels[name] = _derived(first_row, name)
        last_levels[name] = _coerce(last_row, name)
        if last_levels[name] is None:
            last_levels[name] = _derived(last_row, name)
    return first_levels, last_levels


def _fp_r_row_level(row: dict[str, str], variable: str) -> float | None:
    name = str(variable)
    raw = str(row.get(name, "")).strip()
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    if name == "TRLOWZ":
        try:
            ub = float(str(row.get("UB", "")).strip())
            trgh = float(str(row.get("TRGH", "")).strip())
            trsh = float(str(row.get("TRSH", "")).strip())
            pop = float(str(row.get("POP", "")).strip())
            ph = float(str(row.get("PH", "")).strip())
        except ValueError:
            return None
        denom = pop * ph
        if abs(denom) <= 1e-12:
            return None
        return (ub + trgh + trsh) / denom
    if name == "RYDPC":
        try:
            yd = float(str(row.get("YD", "")).strip())
            pop = float(str(row.get("POP", "")).strip())
            ph = float(str(row.get("PH", "")).strip())
        except ValueError:
            return None
        denom = pop * ph
        if abs(denom) <= 1e-12:
            return None
        return yd / denom
    return None


def _read_fp_r_series_level_for_period(
    series_path: Path,
    *,
    variable: str,
    period: str | None,
    level_kind: str,
) -> float | None:
    if not series_path.exists():
        return None
    with series_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    if not rows:
        return None
    target_period = str(period or "").strip()
    if target_period:
        for row in rows:
            if str(row.get("period", "")).strip() == target_period:
                return _fp_r_row_level(row, variable)
    fallback_row = rows[0] if level_kind == "first" else rows[-1]
    return _fp_r_row_level(fallback_row, variable)


def _read_loadformat_level_for_period(
    loadformat_path: Path,
    *,
    variable: str,
    period: str | None,
    level_kind: str,
) -> float | None:
    if not loadformat_path.exists():
        return None
    periods, series = _read_loadformat_payload(loadformat_path)
    values = list(series.get(str(variable), []) or [])
    if not values:
        return None
    target_period = str(period or "").strip()
    if target_period:
        for idx, token in enumerate(periods):
            if str(token).strip() == target_period and idx < len(values):
                return float(values[idx])
    return float(values[0] if level_kind == "first" else values[-1])


def _scenario_level_for_backend(
    scenario: dict[str, object],
    *,
    backend: str,
    variable: str,
    level_kind: str,
) -> float | None:
    scenario_levels = dict(scenario.get(f"{level_kind}_levels", {}) or {})
    backend_name = str(backend).strip().lower()
    loadformat_path = scenario.get("loadformat_path")
    if backend_name.startswith("fp-r") or backend_name in {"fp_r", "fpr"}:
        if not loadformat_path:
            return scenario_levels.get(str(variable))
        fp_r_series_path = Path(str(loadformat_path)).with_name("fp_r_series.csv")
        period_key = "forecast_window_start" if level_kind == "first" else "forecast_window_end"
        resolved = _read_fp_r_series_level_for_period(
            fp_r_series_path,
            variable=str(variable),
            period=str(scenario.get(period_key, "")).strip() or None,
            level_kind=level_kind,
        )
        if resolved is not None:
            return resolved
    if loadformat_path:
        period_key = "forecast_window_start" if level_kind == "first" else "forecast_window_end"
        resolved = _read_loadformat_level_for_period(
            Path(str(loadformat_path)),
            variable=str(variable),
            period=str(scenario.get(period_key, "")).strip() or None,
            level_kind=level_kind,
        )
        if resolved is not None:
            return resolved
    return scenario_levels.get(str(variable))


def _read_fp_r_series_payload(series_path: Path) -> tuple[list[str], dict[str, list[float]]]:
    if not series_path.exists():
        return [], {}

    with series_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    if not rows:
        return [], {}

    periods = [str(row.get("period", "")).strip() for row in rows]
    columns = [name for name in (reader.fieldnames or []) if name and name != "period"]
    series: dict[str, list[float]] = {name: [] for name in columns}
    for row in rows:
        for name in columns:
            raw = str(row.get(name, "")).strip()
            if not raw:
                series[name].append(float("nan"))
                continue
            try:
                series[name].append(float(raw))
            except ValueError:
                series[name].append(float("nan"))
    return periods, series


def _is_missing_level_value(value: float | None) -> bool:
    if value is None:
        return True
    if not np.isfinite(float(value)):
        return True
    return abs(float(value) + 99.0) <= 1e-9


def _supplement_missing_levels(
    base_levels: dict[str, float | None],
    fallback_levels: dict[str, float | None],
) -> dict[str, float | None]:
    supplemented = dict(base_levels)
    for name, value in fallback_levels.items():
        if _is_missing_level_value(supplemented.get(name)) and not _is_missing_level_value(value):
            supplemented[name] = float(value)
    return supplemented


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


def _safe_distribution_decomposition(
    results: dict[str, dict[str, float | None]],
    coefficient_report: dict[str, object],
) -> dict[str, object]:
    try:
        payload = _distribution_decomposition(results, coefficient_report)
        return {
            "metadata": dict(payload["metadata"]),
            "scenarios": dict(payload["scenarios"]),
            "error": None,
        }
    except (KeyError, ValueError) as exc:
        return {
            "metadata": {
                "error": str(exc),
            },
            "scenarios": {},
            "error": str(exc),
        }


def _private_package_gate_summary(
    baseline_levels: dict[str, float | None],
    scenario_levels: dict[str, float | None],
    spec,
) -> dict[str, object]:
    selected_levels = {name: scenario_levels.get(name) for name in _PRIVATE_PACKAGE_EVIDENCE_TRACKS}
    selected_deltas: dict[str, float | None] = {}
    for name in _PRIVATE_PACKAGE_EVIDENCE_TRACKS:
        baseline_value = baseline_levels.get(name)
        scenario_value = scenario_levels.get(name)
        if baseline_value is None or scenario_value is None:
            selected_deltas[name] = None
        else:
            selected_deltas[name] = float(scenario_value - baseline_value)

    has_all_private_evidence = all(value is not None for value in selected_levels.values())
    has_private_flow_move = any(
        value is not None and abs(float(value)) > _PRIVATE_PACKAGE_BALANCE_TOLERANCE
        for name, value in selected_deltas.items()
        if name in ("THG", "THS", "RECG", "RECS", "SGP", "SSP")
    )
    has_private_package_move = any(
        value is not None and abs(float(value)) > _PRIVATE_PACKAGE_BALANCE_TOLERANCE
        for name, value in selected_deltas.items()
        if name in ("PKGGROSS", "PKGFIN", "PKGNET")
    )
    gross = selected_levels["PKGGROSS"]
    fin = selected_levels["PKGFIN"]
    net = selected_levels["PKGNET"]
    balance_error: float | None = None
    if gross is not None and fin is not None and net is not None:
        balance_error = float((float(gross) - float(fin)) - float(net))
    balance_ok = balance_error is not None and abs(balance_error) <= _PRIVATE_PACKAGE_BALANCE_TOLERANCE
    gross_positive = gross is not None and float(gross) > _PRIVATE_PACKAGE_BALANCE_TOLERANCE
    has_financing = abs(float(spec.trfin_fed_share)) > _PRIVATE_PACKAGE_BALANCE_TOLERANCE or abs(
        float(spec.trfin_sl_share)
    ) > _PRIVATE_PACKAGE_BALANCE_TOLERANCE
    financing_ok = (
        fin is not None
        and (
            abs(float(fin)) <= _PRIVATE_PACKAGE_BALANCE_TOLERANCE
            if not has_financing
            else float(fin) > _PRIVATE_PACKAGE_BALANCE_TOLERANCE
        )
    )
    full_balancing = (
        abs(float(spec.trfin_fed_share) - 1.0) <= _PRIVATE_PACKAGE_BALANCE_TOLERANCE
        and abs(float(spec.trfin_sl_share) - 1.0) <= _PRIVATE_PACKAGE_BALANCE_TOLERANCE
    )
    net_ok = net is not None and (
        abs(float(net)) <= _PRIVATE_PACKAGE_BALANCE_TOLERANCE
        if full_balancing
        else float(net) >= -_PRIVATE_PACKAGE_BALANCE_TOLERANCE
    )
    passes = bool(
        has_all_private_evidence
        and has_private_flow_move
        and has_private_package_move
        and gross_positive
        and balance_ok
        and financing_ok
        and net_ok
    )
    return {
        "selected_levels": {name: None if value is None else float(value) for name, value in selected_levels.items()},
        "selected_deltas": selected_deltas,
        "diagnostics": {
            "has_all_private_evidence": has_all_private_evidence,
            "has_private_flow_move": has_private_flow_move,
            "has_private_package_move": has_private_package_move,
            "gross_positive": gross_positive,
            "financing_ok": financing_ok,
            "package_balance_error": balance_error,
            "package_balance_ok": balance_ok,
            "package_net_ok": net_ok,
        },
        "passes": passes,
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
    runtime_text_overrides: dict[str, str] | None = None,
    runtime_text_appends: dict[str, str] | None = None,
    runtime_text_post_patches: dict[str, list[dict[str, str]] | tuple[dict[str, str], ...]] | None = None,
    compose_post_patches: list[dict[str, str]] | tuple[dict[str, str], ...] = (),
) -> dict[str, object]:
    paths = repo_paths()
    fp_home = locate_fp_home(fp_home)
    overlay_root = overlay_root or paths.runtime_distribution_overlay_root
    reports_root = reports_root or paths.runtime_distribution_reports_root
    estimate_payload = estimate_phase1_distribution_coefficients()
    coefficient_report = _load_distribution_coefficient_report()
    runtime_text_files = _merge_runtime_text_files(
        {
            "idcoef.txt": "RETURN;\n",
            "idp1blk.txt": _render_runtime_distribution_block(coefficient_report),
        },
        overrides=runtime_text_overrides,
        appends=runtime_text_appends,
    )
    runtime_text_files = _apply_runtime_text_post_patches(
        runtime_text_files,
        fp_home=fp_home,
        post_patches=runtime_text_post_patches,
    )
    base_post_patches = [
        {
            "search": "INPUT FILE=idistid.txt;",
            "replace": "INPUT FILE=idistid.txt;\nINPUT FILE=ipbase.txt;\nINPUT FILE=idcoef.txt;",
        },
        {
            "search": "INPUT FILE=FMEXOG.TXT;",
            "replace": "INPUT FILE=FMEXOG.TXT;\nINPUT FILE=idp1blk.txt;",
        },
    ]
    compose_payload = compose_phase1_overlay(
        fp_home=fp_home,
        overlay_root=overlay_root,
        extra_overlay_files=["ipbase.txt", "idist_phase1_block.txt"],
        runtime_name_overrides=_RUNTIME_INCLUDE_NAMES,
        runtime_text_files=runtime_text_files,
        post_patches=[*base_post_patches, *[dict(item) for item in compose_post_patches]],
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
        "runtime_text_files": sorted(runtime_text_files),
        "runtime_text_post_patch_targets": sorted(dict(runtime_text_post_patches or {})),
        "compose_post_patch_count": len(tuple(compose_post_patches)),
    }
    reports_root.mkdir(parents=True, exist_ok=True)
    report_path = reports_root / report_name
    report_path.write_text(json.dumps(build_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return build_report


def write_phase1_distribution_scenarios(
    *,
    fp_home: Path,
    backend: str = _DISTRIBUTION_DEFAULT_BACKEND,
    scenarios_root: Path | None = None,
    artifacts_root: Path | None = None,
    variant_ids: tuple[str, ...] | list[str] | None = None,
    overlay_root: Path | None = None,
    forecast_start: str = "2026.1",
    forecast_end: str = "2029.4",
    experimental_patch_ids: list[str] | tuple[str, ...] = (),
    runtime_text_overrides: dict[str, str] | None = None,
    runtime_text_appends: dict[str, str] | None = None,
    runtime_text_post_patches: dict[str, list[dict[str, str]] | tuple[dict[str, str], ...]] | None = None,
    compose_post_patches: list[dict[str, str]] | tuple[dict[str, str], ...] = (),
    scenario_override_additions: dict[str, Any] | None = None,
    scenario_fpr_additions: dict[str, Any] | None = None,
    scenario_extra_metadata: dict[str, Any] | None = None,
) -> list[Path]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.scenarios.config import ScenarioConfig

    paths = repo_paths()
    fp_home = locate_fp_home(fp_home)
    overlay_root = overlay_root or paths.runtime_distribution_overlay_root
    build_phase1_distribution_overlay(
        fp_home=fp_home,
        overlay_root=overlay_root,
        experimental_patch_ids=experimental_patch_ids,
        runtime_text_overrides=runtime_text_overrides,
        runtime_text_appends=runtime_text_appends,
        runtime_text_post_patches=runtime_text_post_patches,
        compose_post_patches=compose_post_patches,
    )
    scenarios_root = scenarios_root or paths.runtime_distribution_scenarios_root
    artifacts_root = artifacts_root or paths.runtime_distribution_artifacts_root
    scenarios_root.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    overlay_dir = overlay_root.resolve()
    for spec in _phase1_specs(variant_ids):
        override_additions = _variant_level_config(scenario_override_additions, spec.variant_id)
        fpr_additions = _variant_level_config(scenario_fpr_additions, spec.variant_id)
        extra_metadata = _variant_level_config(scenario_extra_metadata, spec.variant_id)
        config = ScenarioConfig(
            name=_scenario_name(spec.variant_id),
            description=spec.distribution_description,
            fp_home=fp_home,
            input_overlay_dir=overlay_dir,
            input_file="fminput.txt",
            forecast_start=forecast_start,
            forecast_end=forecast_end,
            backend=backend,
            track_variables=list(_DISTRIBUTION_TRACK_VARIABLES),
            fpr=_merge_mapping_values(_scenario_fpr_settings(spec, backend), fpr_additions),
            overrides=_merge_variable_override_maps(_scenario_overrides(spec), override_additions),
            input_patches=_scenario_input_patches(spec),
            artifacts_root=str(artifacts_root),
            extra=extra_metadata,
        )
        path = scenarios_root / f"{spec.variant_id}.yaml"
        config.to_yaml(path)
        written.append(path)
    return written


def _movement_summary(results: dict[str, dict[str, float | None]]) -> dict[str, Any]:
    baseline = results.get("baseline-observed", {})
    scenario_specs = {spec.variant_id: spec for spec in _phase1_specs()}
    tolerance = 1e-9
    comparisons: dict[str, dict[str, float | None]] = {}
    scenario_checks: dict[str, dict[str, Any]] = {}
    for variant_id, values in results.items():
        if variant_id == "baseline-observed":
            continue
        required = _required_moves_for_variant(variant_id)
        required_signs_expected = _required_signs_for_variant(variant_id)
        expected_sign = _expected_sign_for_variant(variant_id)
        is_transfer_composite = variant_id.startswith("transfer-composite-")
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
                if is_transfer_composite and name in ("YD", "GDPR"):
                    continue
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
        if is_transfer_composite:
            scenario_checks[variant_id]["private_package_gates"] = _private_package_gate_summary(
                baseline,
                values,
                scenario_specs[variant_id],
            )
    return {
        "comparisons": comparisons,
        "scenario_checks": scenario_checks,
        "passes_core": all(detail["passes_core"] for detail in scenario_checks.values()),
    }


def run_phase1_distribution_block(
    *,
    fp_home: Path,
    backend: str = _DISTRIBUTION_DEFAULT_BACKEND,
    scenarios_root: Path | None = None,
    artifacts_root: Path | None = None,
    report_path: Path | None = None,
    variant_ids: tuple[str, ...] | list[str] | None = None,
    overlay_root: Path | None = None,
    forecast_start: str = "2026.1",
    forecast_end: str = "2029.4",
    experimental_patch_ids: list[str] | tuple[str, ...] = (),
    runtime_text_overrides: dict[str, str] | None = None,
    runtime_text_appends: dict[str, str] | None = None,
    runtime_text_post_patches: dict[str, list[dict[str, str]] | tuple[dict[str, str], ...]] | None = None,
    compose_post_patches: list[dict[str, str]] | tuple[dict[str, str], ...] = (),
    scenario_override_additions: dict[str, Any] | None = None,
    scenario_fpr_additions: dict[str, Any] | None = None,
    scenario_extra_metadata: dict[str, Any] | None = None,
    fpr_timeout_seconds: int | None = None,
    fpr_setupsolve_statement: str | None = None,
) -> dict[str, object]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.scenarios import runner as scenario_runner
    from fp_wraptr.scenarios.runner import load_scenario_config

    paths = repo_paths()
    scenarios_root = scenarios_root or paths.runtime_distribution_scenarios_root
    artifacts_root = artifacts_root or paths.runtime_distribution_artifacts_root
    report_path = report_path or (paths.runtime_distribution_reports_root / "run_phase1_distribution_block.json")
    overlay_root = overlay_root or paths.runtime_distribution_overlay_root
    effective_scenario_fpr_additions = scenario_fpr_additions
    if fpr_timeout_seconds is not None:
        timeout_additions = {"__all__": {"timeout_seconds": int(fpr_timeout_seconds)}}
        effective_scenario_fpr_additions = _merge_mapping_values(timeout_additions, scenario_fpr_additions)
    effective_compose_post_patches = [
        *[dict(item) for item in compose_post_patches],
        *_setupsolve_compose_post_patches(fpr_setupsolve_statement),
    ]
    scenario_paths = write_phase1_distribution_scenarios(
        fp_home=fp_home,
        backend=backend,
        scenarios_root=scenarios_root,
        artifacts_root=artifacts_root,
        variant_ids=variant_ids,
        overlay_root=overlay_root,
        forecast_start=forecast_start,
        forecast_end=forecast_end,
        experimental_patch_ids=experimental_patch_ids,
        runtime_text_overrides=runtime_text_overrides,
        runtime_text_appends=runtime_text_appends,
        runtime_text_post_patches=runtime_text_post_patches,
        compose_post_patches=effective_compose_post_patches,
        scenario_override_additions=scenario_override_additions,
        scenario_fpr_additions=effective_scenario_fpr_additions,
        scenario_extra_metadata=scenario_extra_metadata,
    )
    scenario_specs = {spec.variant_id: spec for spec in _phase1_specs()}
    artifacts_root.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

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
                output_dir=artifacts_root,
            )
            fmout_path = result.output_dir / "fmout.txt"
            fmout_text = fmout_path.read_text(encoding="utf-8", errors="replace") if fmout_path.exists() else ""
            solve_error_sol1 = "Solution error in SOL1." in fmout_text
            bridge_parse_errors = _distribution_bridge_parse_errors(fmout_text)
            loadformat_path = result.output_dir / "LOADFORMAT.DAT"
            if loadformat_path.exists():
                loadformat_series = _read_loadformat_series(loadformat_path)
                first_levels, variant_last_levels = _extract_levels_from_loadformat(
                    loadformat_path,
                    _DISTRIBUTION_TRACK_VARIABLES,
                )
                fp_r_series_path = result.output_dir / "fp_r_series.csv"
                fp_r_first_levels, fp_r_last_levels = _read_fp_r_series_levels(
                    fp_r_series_path,
                    _DISTRIBUTION_TRACK_VARIABLES,
                    forecast_start=_DISTRIBUTION_FORECAST_START,
                    forecast_end=_DISTRIBUTION_FORECAST_END,
                )
                first_levels = _supplement_missing_levels(first_levels, fp_r_first_levels)
                variant_last_levels = _supplement_missing_levels(variant_last_levels, fp_r_last_levels)
                flat_tail_flags = _flat_tail_flags(loadformat_series, _DISTRIBUTION_FLAT_TAIL_CHECKS)
            else:
                first_levels = _extract_first_levels(result.parsed_output, _DISTRIBUTION_TRACK_VARIABLES)
                variant_last_levels = _extract_last_levels(result.parsed_output, _DISTRIBUTION_TRACK_VARIABLES)
                flat_tail_flags = {name: False for name in _DISTRIBUTION_FLAT_TAIL_CHECKS}
            spec = scenario_specs[variant_id]
            first_levels = {**first_levels, **_derive_private_package_levels(first_levels, spec)}
            variant_last_levels = {**variant_last_levels, **_derive_private_package_levels(variant_last_levels, spec)}
            first_levels_map[variant_id] = first_levels
            last_levels[variant_id] = variant_last_levels
            run_payloads[variant_id] = {
                "scenario_name": config.name,
                "description": config.description,
                "backend": str(config.backend),
                "forecast_only_series": list(_DISTRIBUTION_FORECAST_ONLY_SERIES),
                "forecast_window_end": _DISTRIBUTION_FORECAST_END,
                "forecast_window_note": _DISTRIBUTION_FORECAST_NOTE,
                "forecast_window_start": _DISTRIBUTION_FORECAST_START,
                "history_seeded_through": _DISTRIBUTION_HISTORY_SEEDED_THROUGH,
                "success": bool(result.success),
                "output_dir": str(result.output_dir),
                "loadformat_path": str(loadformat_path) if loadformat_path.exists() else None,
                "return_code": int(result.run_result.return_code) if result.run_result is not None else None,
                "run_result_stdout_tail": _tail_text(
                    None if result.run_result is None else result.run_result.stdout
                ),
                "run_result_stderr_tail": _tail_text(
                    None if result.run_result is None else result.run_result.stderr
                ),
                "solve_error_sol1": solve_error_sol1,
                "bridge_parse_errors": bridge_parse_errors,
                "flat_tail_flags": flat_tail_flags,
                "first_levels": first_levels,
                "last_levels": variant_last_levels,
                "backend_diagnostics": result.backend_diagnostics,
            }
    finally:
        scenario_runner.parse_fp_output = original_parse_fp_output

    acceptance = _movement_summary(last_levels)
    coefficient_report = _load_distribution_coefficient_report()
    decomposition_first = _safe_distribution_decomposition(first_levels_map, coefficient_report)
    decomposition_last = _safe_distribution_decomposition(last_levels, coefficient_report)
    unhealthy = {
        variant_id: {
            "solve_error_sol1": bool(payload["solve_error_sol1"]),
            "bridge_parse_errors": list(payload["bridge_parse_errors"]),
            "flat_tail_vars": [name for name, flagged in dict(payload["flat_tail_flags"]).items() if flagged],
        }
        for variant_id, payload in run_payloads.items()
        if variant_id != "baseline-observed"
    }
    passes_health = all(
        (not detail["solve_error_sol1"])
        and (not detail["bridge_parse_errors"])
        and (not detail["flat_tail_vars"])
        for detail in unhealthy.values()
    )
    private_package_gates = {
        variant_id: dict(dict(acceptance["scenario_checks"]).get(variant_id, {}).get("private_package_gates", {}))
        for variant_id in last_levels
        if variant_id.startswith("transfer-composite-")
    }
    acceptance["scenario_health"] = unhealthy
    acceptance["passes_health"] = passes_health
    acceptance["private_package_gates"] = private_package_gates
    acceptance["passes_private_package_gates"] = all(bool(dict(detail).get("passes")) for detail in private_package_gates.values())
    acceptance["passes"] = bool(acceptance["passes_core"]) and passes_health
    payload = {
        "scenarios": run_payloads,
        "acceptance": acceptance,
        "execution_backend": str(backend),
        "decomposition": {
            "metadata": decomposition_last["metadata"],
            "first_levels": decomposition_first["scenarios"],
            "last_levels": decomposition_last["scenarios"],
            "first_levels_error": decomposition_first["error"],
            "last_levels_error": decomposition_last["error"],
        },
        "forecast_only_series": list(_DISTRIBUTION_FORECAST_ONLY_SERIES),
        "forecast_window_end": _DISTRIBUTION_FORECAST_END,
        "forecast_window_note": _DISTRIBUTION_FORECAST_NOTE,
        "forecast_window_start": _DISTRIBUTION_FORECAST_START,
        "history_seeded_through": _DISTRIBUTION_HISTORY_SEEDED_THROUGH,
        "track_variables": list(_DISTRIBUTION_TRACK_VARIABLES),
        "scenario_paths": [str(path) for path in scenario_paths],
        "variant_ids": [path.stem for path in scenario_paths],
        "overlay_root": str(overlay_root),
        "scenario_forecast_start": str(forecast_start),
        "scenario_forecast_end": str(forecast_end),
        "experimental_patch_ids": [str(item) for item in experimental_patch_ids],
    }
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "overlay_root": str(overlay_root),
        "scenarios_dir": str(scenarios_root),
        "artifacts_dir": str(artifacts_root),
        "backend": str(backend),
        "passes": bool(acceptance["passes"]),
    }


def _load_distribution_run_report(report_path: Path) -> dict[str, object]:
    return json.loads(report_path.read_text(encoding="utf-8"))


def _identity_summary(
    periods: list[str],
    residual: np.ndarray,
    *,
    expression: str,
    parameters: dict[str, float],
    missing_series: tuple[str, ...] = (),
) -> dict[str, object]:
    if residual.size == 0:
        return {
            "evaluated": False,
            "expression": expression,
            "missing_series": list(missing_series),
            "parameters": {name: float(value) for name, value in parameters.items()},
            "period_count": 0,
        }
    finite_mask = np.isfinite(residual)
    if not np.any(finite_mask):
        return {
            "evaluated": False,
            "expression": expression,
            "missing_series": list(missing_series),
            "parameters": {name: float(value) for name, value in parameters.items()},
            "period_count": 0,
        }
    kept_periods = [period for idx, period in enumerate(periods) if finite_mask[idx]]
    kept_values = residual[finite_mask]
    abs_values = np.abs(kept_values)
    max_idx = int(np.argmax(abs_values))
    terminal_value = float(kept_values[-1])
    return {
        "evaluated": True,
        "expression": expression,
        "missing_series": list(missing_series),
        "parameters": {name: float(value) for name, value in parameters.items()},
        "period_count": int(len(kept_periods)),
        "period_start": kept_periods[0],
        "period_end": kept_periods[-1],
        "max_abs_residual": float(abs_values[max_idx]),
        "mean_abs_residual": float(np.mean(abs_values)),
        "period_of_max_abs_residual": kept_periods[max_idx],
        "residual_at_max_abs": float(kept_values[max_idx]),
        "terminal_residual": terminal_value,
        "terminal_abs_residual": abs(terminal_value),
    }


def _distribution_identity_checks(
    periods: list[str],
    series: dict[str, list[float]],
    spec,
    *,
    forecast_start: str | None = None,
    forecast_end: str | None = None,
) -> dict[str, dict[str, object]]:
    indices = _periods_in_forecast_window(periods, forecast_start=forecast_start, forecast_end=forecast_end)
    window_periods = [periods[idx] for idx in indices]
    policy_inputs = {
        "UIFAC": float(spec.ui_factor),
        "SNAPDELTAQ": float(spec.trgh_delta_q),
        "SSFAC": float(spec.trsh_factor),
        "TFEDSHR": float(spec.trfin_fed_share),
        "TSLSHR": float(spec.trfin_sl_share),
    }

    def _series_window(name: str) -> np.ndarray | None:
        values = series.get(name)
        if values is None:
            return None
        return np.asarray([float(values[idx]) for idx in indices], dtype=float)

    checks: dict[str, dict[str, object]] = {}

    ub = _series_window("UB")
    lub = _series_window("LUB")
    ub_missing = tuple(name for name, value in (("UB", ub), ("LUB", lub)) if value is None)
    if ub_missing:
        checks["ub_scaled"] = _identity_summary(
            window_periods,
            np.asarray([], dtype=float),
            expression=_DISTRIBUTION_IDENTITY_EXPRESSIONS["ub_scaled"],
            parameters={"UIFAC": policy_inputs["UIFAC"]},
            missing_series=ub_missing,
        )
        checks["ub_unscaled"] = _identity_summary(
            window_periods,
            np.asarray([], dtype=float),
            expression=_DISTRIBUTION_IDENTITY_EXPRESSIONS["ub_unscaled"],
            parameters={},
            missing_series=ub_missing,
        )
    else:
        checks["ub_scaled"] = _identity_summary(
            window_periods,
            ub - np.exp(lub) * policy_inputs["UIFAC"],
            expression=_DISTRIBUTION_IDENTITY_EXPRESSIONS["ub_scaled"],
            parameters={"UIFAC": policy_inputs["UIFAC"]},
        )
        checks["ub_unscaled"] = _identity_summary(
            window_periods,
            ub - np.exp(lub),
            expression=_DISTRIBUTION_IDENTITY_EXPRESSIONS["ub_unscaled"],
            parameters={},
        )

    thg = _series_window("THG")
    d1g = _series_window("D1G")
    yt = _series_window("YT")
    gdpd = _series_window("GDPD")
    thg_missing = tuple(
        name for name, value in (("THG", thg), ("D1G", d1g), ("YT", yt), ("UB", ub), ("GDPD", gdpd)) if value is None
    )
    if thg_missing:
        checks["thg_financing"] = _identity_summary(
            window_periods,
            np.asarray([], dtype=float),
            expression=_DISTRIBUTION_IDENTITY_EXPRESSIONS["thg_financing"],
            parameters={
                "UIFAC": policy_inputs["UIFAC"],
                "SNAPDELTAQ": policy_inputs["SNAPDELTAQ"],
                "TFEDSHR": policy_inputs["TFEDSHR"],
            },
            missing_series=thg_missing,
        )
    else:
        checks["thg_financing"] = _identity_summary(
            window_periods,
            thg
            - (
                d1g * yt
                + policy_inputs["TFEDSHR"]
                * ((ub - (ub / policy_inputs["UIFAC"])) + policy_inputs["SNAPDELTAQ"] * gdpd)
            ),
            expression=_DISTRIBUTION_IDENTITY_EXPRESSIONS["thg_financing"],
            parameters={
                "UIFAC": policy_inputs["UIFAC"],
                "SNAPDELTAQ": policy_inputs["SNAPDELTAQ"],
                "TFEDSHR": policy_inputs["TFEDSHR"],
            },
        )

    ths = _series_window("THS")
    d1s = _series_window("D1S")
    trsh = _series_window("TRSH")
    ths_missing = tuple(name for name, value in (("THS", ths), ("D1S", d1s), ("YT", yt), ("TRSH", trsh)) if value is None)
    if ths_missing:
        checks["ths_financing"] = _identity_summary(
            window_periods,
            np.asarray([], dtype=float),
            expression=_DISTRIBUTION_IDENTITY_EXPRESSIONS["ths_financing"],
            parameters={
                "SSFAC": policy_inputs["SSFAC"],
                "TSLSHR": policy_inputs["TSLSHR"],
            },
            missing_series=ths_missing,
        )
    else:
        checks["ths_financing"] = _identity_summary(
            window_periods,
            ths - (d1s * yt + policy_inputs["TSLSHR"] * (trsh - (trsh / policy_inputs["SSFAC"]))),
            expression=_DISTRIBUTION_IDENTITY_EXPRESSIONS["ths_financing"],
            parameters={
                "SSFAC": policy_inputs["SSFAC"],
                "TSLSHR": policy_inputs["TSLSHR"],
            },
        )

    return checks


def _distribution_identity_checks_for_loadformat(
    loadformat_path: Path,
    spec,
    *,
    forecast_start: str | None = None,
    forecast_end: str | None = None,
) -> dict[str, dict[str, object]]:
    periods, series = _read_loadformat_payload(loadformat_path)
    return _distribution_identity_checks(
        periods,
        series,
        spec,
        forecast_start=forecast_start,
        forecast_end=forecast_end,
    )


def validate_phase1_distribution_identities(
    *,
    fp_home: Path | None = None,
    backend: str = _DISTRIBUTION_DEFAULT_BACKEND,
    variant_ids: tuple[str, ...] | list[str] | None = None,
    run_report_path: Path | None = None,
    report_path: Path | None = None,
    max_abs_residual: float = 1e-6,
    fpr_timeout_seconds: int | None = None,
    ub_identity_mode: str = "scaled",
    forecast_start: str | None = None,
    forecast_end: str | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    selected_variant_ids = _identity_validation_variant_ids(variant_ids)
    normalized_ub_identity_mode = str(ub_identity_mode).strip().lower().replace("-", "_")
    if normalized_ub_identity_mode in {"scaled", "ub_scaled"}:
        required_identity_ids = _DISTRIBUTION_REQUIRED_IDENTITY_IDS
    elif normalized_ub_identity_mode in {"unscaled", "transformed_lhs", "transformed_lhs_percent_log", "ub_unscaled"}:
        required_identity_ids = _DISTRIBUTION_REQUIRED_IDENTITY_IDS_TRANSFORMED_LHS
    else:
        raise ValueError(
            "ub_identity_mode must be 'scaled' or 'unscaled' "
            f"(got {ub_identity_mode!r})."
        )
    spec_map = phase1_scenario_by_variant()
    tag = str(backend).replace("_", "-").replace(".", "-")
    run_report_path = run_report_path or _tagged_report_path(
        paths.runtime_distribution_reports_root / "run_phase1_distribution_block.json",
        tag if backend != _DISTRIBUTION_DEFAULT_BACKEND else None,
    )
    if not run_report_path.exists():
        if fp_home is None:
            raise FileNotFoundError(
                f"Distribution run report missing: {run_report_path}. "
                "Pass `fp_home` to generate it or provide `run_report_path`."
            )
        run_phase1_distribution_block(
            fp_home=fp_home,
            backend=backend,
            variant_ids=selected_variant_ids,
            report_path=run_report_path,
            fpr_timeout_seconds=fpr_timeout_seconds,
        )

    payload = _load_distribution_run_report(run_report_path)
    scenarios = dict(payload.get("scenarios", {}))
    results: list[dict[str, object]] = []
    all_required_pass = True
    all_evaluated = True
    max_required_residual = 0.0

    for variant_id in selected_variant_ids:
        if variant_id not in scenarios:
            raise KeyError(f"Run report missing variant: {variant_id}")
        spec = spec_map[variant_id]
        scenario = dict(scenarios[variant_id])
        loadformat_path = scenario.get("loadformat_path")
        if not loadformat_path:
            raise KeyError(f"Run report missing loadformat_path for variant: {variant_id}")
        identity_checks = _distribution_identity_checks_for_loadformat(
            Path(str(loadformat_path)),
            spec,
            forecast_start=(
                str(forecast_start or "").strip()
                or str(payload.get("scenario_forecast_start", payload.get("forecast_window_start", "")) or "").strip()
                or None
            ),
            forecast_end=(
                str(forecast_end or "").strip()
                or str(payload.get("scenario_forecast_end", payload.get("forecast_window_end", "")) or "").strip()
                or None
            ),
        )
        required_checks: dict[str, dict[str, object]] = {}
        required_pass = True
        required_evaluated = True
        for identity_id in required_identity_ids:
            detail = dict(identity_checks.get(identity_id, {}))
            evaluated = bool(detail.get("evaluated"))
            max_residual = detail.get("max_abs_residual")
            passed = evaluated and max_residual is not None and float(max_residual) <= float(max_abs_residual)
            required_checks[identity_id] = {
                **detail,
                "passes": bool(passed),
            }
            required_evaluated = required_evaluated and evaluated
            required_pass = required_pass and passed
            if max_residual is not None:
                max_required_residual = max(max_required_residual, float(max_residual))
        all_required_pass = all_required_pass and required_pass
        all_evaluated = all_evaluated and required_evaluated
        results.append(
            {
                "variant_id": variant_id,
                "backend": scenario.get("backend", backend),
                "loadformat_path": str(loadformat_path),
                "required_identity_checks": required_checks,
                "all_identity_checks": identity_checks,
                "passes_required_identities": bool(required_pass),
                "evaluated_required_identities": bool(required_evaluated),
            }
        )

    summary = {
        "backend": backend,
        "variant_count": len(results),
        "ub_identity_mode": normalized_ub_identity_mode,
        "required_identity_ids": list(required_identity_ids),
        "forecast_start": (
            str(forecast_start or "").strip()
            or str(payload.get("scenario_forecast_start", payload.get("forecast_window_start", "")) or "").strip()
            or None
        ),
        "forecast_end": (
            str(forecast_end or "").strip()
            or str(payload.get("scenario_forecast_end", payload.get("forecast_window_end", "")) or "").strip()
            or None
        ),
        "max_abs_residual_tolerance": float(max_abs_residual),
        "passes": bool(all_required_pass),
        "all_required_identities_evaluated": bool(all_evaluated),
        "max_required_identity_residual": float(max_required_residual),
    }
    report_payload = {
        "backend": backend,
        "run_report_path": str(run_report_path),
        "variant_ids": selected_variant_ids,
        "summary": summary,
        "variants": results,
    }
    report_path = report_path or _tagged_report_path(
        paths.runtime_distribution_reports_root / "validate_phase1_distribution_identities.json",
        tag,
    )
    report_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "run_report_path": str(run_report_path),
        "variant_count": len(results),
        "passes": bool(all_required_pass),
        "max_required_identity_residual": float(max_required_residual),
    }


def analyze_phase1_distribution_policy_gap(
    *,
    compare_report_path: Path | None = None,
    variant_ids: tuple[str, ...] | list[str] = _DISTRIBUTION_POLICY_GAP_VARIANT_IDS,
    variables: tuple[str, ...] | list[str] = _DISTRIBUTION_POLICY_GAP_VARIABLES,
    report_path: Path | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    compare_report_path = compare_report_path or (
        paths.runtime_distribution_reports_root / "compare_phase1_distribution_backends.json"
    )
    compare_payload = json.loads(compare_report_path.read_text(encoding="utf-8"))
    left_backend = str(compare_payload["left_backend"])
    right_backend = str(compare_payload["right_backend"])
    comparisons = {str(row["variant_id"]): dict(row) for row in compare_payload.get("comparisons", [])}
    if "baseline-observed" not in comparisons:
        raise KeyError("Comparison report is missing baseline-observed.")

    baseline_row = comparisons["baseline-observed"]
    baseline_paths = {
        left_backend: Path(str(baseline_row["left_loadformat_path"])),
        right_backend: Path(str(baseline_row["right_loadformat_path"])),
    }
    selected_variant_ids = [str(item) for item in variant_ids]
    selected_variables = [str(item) for item in variables]

    series_cache: dict[tuple[str, str], tuple[list[str], dict[str, list[float]]]] = {}

    def _payload_for(backend: str, path: Path) -> tuple[list[str], dict[str, list[float]]]:
        key = (backend, str(path))
        if key not in series_cache:
            periods, series = _read_loadformat_payload(path)
            if backend == "fp-r":
                fp_r_series_path = path.with_name("fp_r_series.csv")
                fp_r_periods, fp_r_series = _read_fp_r_series_payload(fp_r_series_path)
                if fp_r_periods and fp_r_periods == periods:
                    merged = {name: [float(value) for value in values] for name, values in series.items()}
                    for name, values in fp_r_series.items():
                        if name not in merged:
                            merged[name] = [float(value) for value in values]
                    series = merged
            series_cache[key] = (periods, series)
        return series_cache[key]

    def _window_series(periods: list[str], series: dict[str, list[float]], variable: str) -> tuple[list[str], np.ndarray]:
        indices = _periods_in_forecast_window(periods)
        values = series.get(variable)
        if values is None:
            return [periods[idx] for idx in indices], np.asarray([], dtype=float)
        return [periods[idx] for idx in indices], np.asarray([float(values[idx]) for idx in indices], dtype=float)

    variant_rows: list[dict[str, object]] = []
    max_gap_ratio = 0.0
    for variant_id in selected_variant_ids:
        if variant_id not in comparisons:
            raise KeyError(f"Comparison report is missing variant: {variant_id}")
        row = comparisons[variant_id]
        scenario_paths = {
            left_backend: Path(str(row["left_loadformat_path"])),
            right_backend: Path(str(row["right_loadformat_path"])),
        }
        variable_rows: dict[str, dict[str, object]] = {}
        for variable in selected_variables:
            backend_deltas: dict[str, dict[str, object]] = {}
            for backend in (left_backend, right_backend):
                baseline_periods, baseline_series = _payload_for(backend, baseline_paths[backend])
                scenario_periods, scenario_series = _payload_for(backend, scenario_paths[backend])
                if baseline_periods != scenario_periods:
                    raise ValueError(f"Baseline and scenario periods do not match for backend {backend} and variable {variable}.")
                window_periods, baseline_values = _window_series(baseline_periods, baseline_series, variable)
                _window_periods_check, scenario_values = _window_series(scenario_periods, scenario_series, variable)
                if baseline_values.size == 0 or scenario_values.size == 0:
                    backend_deltas[backend] = {
                        "evaluated": False,
                        "period_start": window_periods[0] if window_periods else None,
                        "period_end": window_periods[-1] if window_periods else None,
                    }
                    continue
                delta = scenario_values - baseline_values
                abs_delta = np.abs(delta)
                max_idx = int(np.argmax(abs_delta))
                backend_deltas[backend] = {
                    "evaluated": True,
                    "period_start": window_periods[0],
                    "period_end": window_periods[-1],
                    "baseline_first": float(baseline_values[0]),
                    "scenario_first": float(scenario_values[0]),
                    "delta_first": float(delta[0]),
                    "baseline_last": float(baseline_values[-1]),
                    "scenario_last": float(scenario_values[-1]),
                    "delta_last": float(delta[-1]),
                    "max_abs_delta": float(abs_delta[max_idx]),
                    "period_of_max_abs_delta": window_periods[max_idx],
                    "delta_at_max_abs": float(delta[max_idx]),
                }
            left_delta = dict(backend_deltas[left_backend])
            right_delta = dict(backend_deltas[right_backend])
            first_gap_ratio = None
            last_gap_ratio = None
            if left_delta.get("evaluated") and right_delta.get("evaluated"):
                right_first = float(right_delta["delta_first"])
                right_last = float(right_delta["delta_last"])
                left_first = float(left_delta["delta_first"])
                left_last = float(left_delta["delta_last"])
                if abs(right_first) > 1e-12:
                    first_gap_ratio = abs(left_first) / abs(right_first)
                    max_gap_ratio = max(max_gap_ratio, abs(first_gap_ratio))
                if abs(right_last) > 1e-12:
                    last_gap_ratio = abs(left_last) / abs(right_last)
                    max_gap_ratio = max(max_gap_ratio, abs(last_gap_ratio))
            variable_rows[variable] = {
                left_backend: left_delta,
                right_backend: right_delta,
                "first_gap_ratio_abs": first_gap_ratio,
                "last_gap_ratio_abs": last_gap_ratio,
                "first_sign_match": (
                    None
                    if not left_delta.get("evaluated") or not right_delta.get("evaluated")
                    else bool(
                        np.sign(float(left_delta["delta_first"])) == np.sign(float(right_delta["delta_first"]))
                    )
                ),
                "last_sign_match": (
                    None
                    if not left_delta.get("evaluated") or not right_delta.get("evaluated")
                    else bool(
                        np.sign(float(left_delta["delta_last"])) == np.sign(float(right_delta["delta_last"]))
                    )
                ),
            }
        variant_rows.append(
            {
                "variant_id": variant_id,
                "left_backend": left_backend,
                "right_backend": right_backend,
                "variables": variable_rows,
            }
        )

    payload = {
        "compare_report_path": str(compare_report_path),
        "left_backend": left_backend,
        "right_backend": right_backend,
        "variant_ids": selected_variant_ids,
        "variables": selected_variables,
        "summary": {
            "variant_count": len(variant_rows),
            "max_gap_ratio_abs": float(max_gap_ratio),
        },
        "variants": variant_rows,
    }
    report_path = report_path or (paths.runtime_distribution_reports_root / "analyze_phase1_distribution_policy_gap.json")
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "variant_count": len(variant_rows),
        "max_gap_ratio_abs": float(max_gap_ratio),
    }


def analyze_phase1_distribution_first_levels(
    *,
    compare_report_path: Path | None = None,
    variant_ids: tuple[str, ...] | list[str] = _DISTRIBUTION_FIRST_LEVEL_GAP_VARIANT_IDS,
    variables: tuple[str, ...] | list[str] = _DISTRIBUTION_FIRST_LEVEL_GAP_VARIABLES,
    report_path: Path | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    compare_report_path = compare_report_path or (
        paths.runtime_distribution_reports_root / "compare_phase1_distribution_backends.json"
    )
    compare_payload = json.loads(compare_report_path.read_text(encoding="utf-8"))
    left_backend = str(compare_payload["left_backend"])
    right_backend = str(compare_payload["right_backend"])
    comparisons = {str(row["variant_id"]): dict(row) for row in compare_payload.get("comparisons", [])}
    if "baseline-observed" not in comparisons:
        raise KeyError("Comparison report is missing baseline-observed.")

    baseline_row = comparisons["baseline-observed"]
    baseline_paths = {
        left_backend: Path(str(baseline_row["left_loadformat_path"])),
        right_backend: Path(str(baseline_row["right_loadformat_path"])),
    }
    selected_variant_ids = [str(item) for item in variant_ids]
    selected_variables = [str(item) for item in variables]

    series_cache: dict[tuple[str, str], tuple[list[str], dict[str, list[float]]]] = {}

    def _payload_for(backend: str, path: Path) -> tuple[list[str], dict[str, list[float]]]:
        key = (backend, str(path))
        if key not in series_cache:
            periods, series = _read_loadformat_payload(path)
            merged = {name: [float(value) for value in values] for name, values in series.items()}
            if backend == "fp-r":
                fp_r_series_path = path.with_name("fp_r_series.csv")
                fp_r_periods, fp_r_series = _read_fp_r_series_payload(fp_r_series_path)
                if fp_r_periods and fp_r_periods == periods:
                    for name, values in fp_r_series.items():
                        merged[name] = [float(value) for value in values]
            series_cache[key] = (periods, merged)
        return series_cache[key]

    def _first_window_point(periods: list[str], series: dict[str, list[float]], variable: str) -> dict[str, object]:
        indices = _periods_in_forecast_window(periods)
        if not indices:
            return {"evaluated": False, "period": None, "value": None}
        values = series.get(variable)
        if values is None:
            return {"evaluated": False, "period": periods[indices[0]], "value": None}
        idx = int(indices[0])
        return {
            "evaluated": True,
            "period": periods[idx],
            "value": float(values[idx]),
        }

    variant_rows: list[dict[str, object]] = []
    max_level_abs_diff = 0.0
    max_delta_abs_diff = 0.0
    for variant_id in selected_variant_ids:
        if variant_id not in comparisons:
            raise KeyError(f"Comparison report is missing variant: {variant_id}")
        row = comparisons[variant_id]
        scenario_paths = {
            left_backend: Path(str(row["left_loadformat_path"])),
            right_backend: Path(str(row["right_loadformat_path"])),
        }
        variable_rows: dict[str, dict[str, object]] = {}
        for variable in selected_variables:
            backend_levels: dict[str, dict[str, object]] = {}
            for backend in (left_backend, right_backend):
                baseline_periods, baseline_series = _payload_for(backend, baseline_paths[backend])
                scenario_periods, scenario_series = _payload_for(backend, scenario_paths[backend])
                if baseline_periods != scenario_periods:
                    raise ValueError(
                        f"Baseline and scenario periods do not match for backend {backend} and variable {variable}."
                    )
                baseline_first = _first_window_point(baseline_periods, baseline_series, variable)
                scenario_first = _first_window_point(scenario_periods, scenario_series, variable)
                if not baseline_first.get("evaluated") or not scenario_first.get("evaluated"):
                    backend_levels[backend] = {
                        "evaluated": False,
                        "period": scenario_first.get("period") or baseline_first.get("period"),
                    }
                    continue
                delta_first = float(scenario_first["value"]) - float(baseline_first["value"])
                backend_levels[backend] = {
                    "evaluated": True,
                    "period": str(scenario_first["period"]),
                    "baseline_first": float(baseline_first["value"]),
                    "scenario_first": float(scenario_first["value"]),
                    "delta_first": delta_first,
                }
            left_level = dict(backend_levels[left_backend])
            right_level = dict(backend_levels[right_backend])
            level_abs_diff = None
            delta_abs_diff = None
            delta_ratio_abs = None
            delta_sign_match = None
            if left_level.get("evaluated") and right_level.get("evaluated"):
                level_abs_diff = abs(float(left_level["scenario_first"]) - float(right_level["scenario_first"]))
                delta_abs_diff = abs(float(left_level["delta_first"]) - float(right_level["delta_first"]))
                right_delta = float(right_level["delta_first"])
                if abs(right_delta) > 1e-12:
                    delta_ratio_abs = abs(float(left_level["delta_first"])) / abs(right_delta)
                delta_sign_match = bool(
                    np.sign(float(left_level["delta_first"])) == np.sign(float(right_level["delta_first"]))
                )
                max_level_abs_diff = max(max_level_abs_diff, float(level_abs_diff))
                max_delta_abs_diff = max(max_delta_abs_diff, float(delta_abs_diff))
            variable_rows[variable] = {
                left_backend: left_level,
                right_backend: right_level,
                "level_abs_diff": level_abs_diff,
                "delta_abs_diff": delta_abs_diff,
                "delta_ratio_abs": delta_ratio_abs,
                "delta_sign_match": delta_sign_match,
            }
        variant_rows.append(
            {
                "variant_id": variant_id,
                "left_backend": left_backend,
                "right_backend": right_backend,
                "variables": variable_rows,
            }
        )

    payload = {
        "compare_report_path": str(compare_report_path),
        "left_backend": left_backend,
        "right_backend": right_backend,
        "variant_ids": selected_variant_ids,
        "variables": selected_variables,
        "summary": {
            "variant_count": len(variant_rows),
            "max_level_abs_diff": float(max_level_abs_diff),
            "max_delta_abs_diff": float(max_delta_abs_diff),
        },
        "variants": variant_rows,
    }
    report_path = report_path or (paths.runtime_distribution_reports_root / "analyze_phase1_distribution_first_levels.json")
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "variant_count": len(variant_rows),
        "max_level_abs_diff": float(max_level_abs_diff),
        "max_delta_abs_diff": float(max_delta_abs_diff),
    }


def assess_phase1_distribution_backend_boundary(
    *,
    first_levels_report_path: Path | None = None,
    identity_report_path: Path | None = None,
    driver_gap_report_path: Path | None = None,
    report_path: Path | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    first_levels_report_path = first_levels_report_path or (
        paths.runtime_distribution_reports_root / "analyze_phase1_distribution_first_levels.json"
    )
    identity_report_path = identity_report_path or (
        paths.runtime_distribution_reports_root / "validate_phase1_distribution_identities.fp-r.json"
    )
    driver_gap_report_path = driver_gap_report_path or (
        paths.runtime_distribution_reports_root / "analyze_phase1_distribution_driver_gap.json"
    )

    first_levels_payload = json.loads(first_levels_report_path.read_text(encoding="utf-8"))
    identity_payload = json.loads(identity_report_path.read_text(encoding="utf-8"))
    driver_gap_payload = json.loads(driver_gap_report_path.read_text(encoding="utf-8"))

    variants = {str(row["variant_id"]): dict(row) for row in first_levels_payload.get("variants", [])}
    identity_variants = {str(row["variant_id"]): dict(row) for row in identity_payload.get("variants", [])}

    def _variable_stats(variant_id: str, variables: tuple[str, ...]) -> dict[str, object]:
        variant = variants.get(variant_id, {})
        variable_rows = dict(variant.get("variables", {}))
        ratios: list[float] = []
        sign_match_count = 0
        sign_mismatch_count = 0
        evaluated_count = 0
        details: dict[str, dict[str, object]] = {}
        for variable in variables:
            row = dict(variable_rows.get(variable, {}))
            if not row:
                continue
            left = dict(row.get(first_levels_payload["left_backend"], {}))
            right = dict(row.get(first_levels_payload["right_backend"], {}))
            evaluated = bool(left.get("evaluated")) and bool(right.get("evaluated"))
            if not evaluated:
                details[variable] = {"evaluated": False}
                continue
            evaluated_count += 1
            ratio = row.get("delta_ratio_abs")
            sign_match = row.get("delta_sign_match")
            if ratio is not None:
                ratios.append(float(ratio))
            if sign_match is True:
                sign_match_count += 1
            elif sign_match is False:
                sign_mismatch_count += 1
            details[variable] = {
                "evaluated": True,
                "delta_ratio_abs": None if ratio is None else float(ratio),
                "delta_sign_match": None if sign_match is None else bool(sign_match),
                "level_abs_diff": None if row.get("level_abs_diff") is None else float(row["level_abs_diff"]),
                "delta_abs_diff": None if row.get("delta_abs_diff") is None else float(row["delta_abs_diff"]),
            }
        median_ratio = None if not ratios else float(np.median(np.asarray(ratios, dtype=float)))
        max_ratio = None if not ratios else float(np.max(np.asarray(ratios, dtype=float)))
        return {
            "evaluated_count": evaluated_count,
            "median_delta_ratio_abs": median_ratio,
            "max_delta_ratio_abs": max_ratio,
            "sign_match_count": sign_match_count,
            "sign_mismatch_count": sign_mismatch_count,
            "variables": details,
        }

    ui_stats = _variable_stats("ui-relief", _DISTRIBUTION_BOUNDARY_UI_VARIABLES)
    transfer_direct_stats = _variable_stats(
        "transfer-composite-medium", _DISTRIBUTION_BOUNDARY_TRANSFER_DIRECT_VARIABLES
    )
    transfer_macro_stats = _variable_stats(
        "transfer-composite-medium", _DISTRIBUTION_BOUNDARY_TRANSFER_MACRO_VARIABLES
    )

    ui_identity = dict(identity_variants.get("ui-relief", {}))
    transfer_identity = dict(identity_variants.get("transfer-composite-medium", {}))
    identity_pass = bool(identity_payload.get("passes"))
    if "passes" not in identity_payload:
        required_flags = [
            bool(dict(row).get("passes_required_identities"))
            for row in identity_payload.get("variants", [])
            if "passes_required_identities" in dict(row)
        ]
        identity_pass = bool(required_flags) and all(required_flags)

    ui_assessment = {
        "identity_passes_required": bool(ui_identity.get("passes_required_identities")),
        "assessment": (
            "open_attenuation"
            if ui_stats["evaluated_count"] and ui_stats["sign_mismatch_count"] == 0 and (
                ui_stats["median_delta_ratio_abs"] is not None and float(ui_stats["median_delta_ratio_abs"]) < 0.1
            )
            else "mixed"
        ),
        "reason": (
            "Signs match in the first quarter, but the policy deltas are strongly attenuated relative to fpexe."
        ),
        "first_quarter_stats": ui_stats,
        "driver_gap_hook": {
            "lub_has_uifac_reference": bool(
                dict(driver_gap_payload.get("ui_retained_target_analysis", {}))
                .get("lub_equation_summary", {})
                .get("has_uifac_reference", False)
            ),
            "solve_step_from_previous": dict(
                dict(driver_gap_payload.get("ui_retained_target_analysis", {})).get("lub_uplift_breakdown", {})
            ).get("solve_step_from_previous"),
        },
    }

    transfer_assessment = {
        "identity_passes_required": bool(transfer_identity.get("passes_required_identities")),
        "direct_channel_assessment": (
            "pass_direct_channels"
            if transfer_direct_stats["evaluated_count"]
            and transfer_direct_stats["sign_mismatch_count"] == 0
            and (
                transfer_direct_stats["median_delta_ratio_abs"] is not None
                and float(transfer_direct_stats["median_delta_ratio_abs"]) >= 0.5
            )
            else "mixed"
        ),
        "macro_channel_assessment": (
            "block_macro_sign_flip"
            if transfer_macro_stats["sign_mismatch_count"] > 0
            else "open_macro"
        ),
        "reason": (
            "Direct transfer and financing channels are reasonably aligned, but the macro-income block still flips sign versus fpexe."
        ),
        "first_quarter_direct_stats": transfer_direct_stats,
        "first_quarter_macro_stats": transfer_macro_stats,
        "driver_gap_hook": {
            "first_quarter_yd_delta": dict(
                dict(driver_gap_payload.get("transfer_income_gap_analysis", {}))
                .get("periods", {})
                .get("2026.1", {})
                .get("scenario_minus_baseline", {})
            ).get("YD"),
            "first_quarter_rs_delta": dict(
                dict(driver_gap_payload.get("transfer_income_gap_analysis", {}))
                .get("periods", {})
                .get("2026.1", {})
                .get("scenario_minus_baseline", {})
            ).get("RS"),
        },
    }

    overall = {
        "identity_surface_passes": identity_pass,
        "replacement_readiness": (
            "not_ready"
            if ui_assessment["assessment"] != "mixed" or transfer_assessment["macro_channel_assessment"] == "block_macro_sign_flip"
            else "mixed"
        ),
        "recommendation": (
            "Do not treat fp-r as a full replacement for fpexe yet. "
            "The identity surface and direct transfer/fiscal mechanics are in good shape, "
            "but ui-relief remains strongly attenuated and transfer-composite-medium still has a macro-income sign flip."
        ),
    }

    payload = {
        "first_levels_report_path": str(first_levels_report_path),
        "identity_report_path": str(identity_report_path),
        "driver_gap_report_path": str(driver_gap_report_path),
        "overall": overall,
        "ui_relief": ui_assessment,
        "transfer_composite_medium": transfer_assessment,
    }
    report_path = report_path or (paths.runtime_distribution_reports_root / "assess_phase1_distribution_backend_boundary.json")
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "replacement_readiness": str(overall["replacement_readiness"]),
        "identity_surface_passes": bool(identity_pass),
    }


def assess_phase1_distribution_canonical_parity(
    *,
    freeze_report_path: Path | None = None,
    compare_report_path: Path | None = None,
    first_levels_report_path: Path | None = None,
    backend_boundary_report_path: Path | None = None,
    fp_r_identity_report_path: Path | None = None,
    fpexe_identity_report_path: Path | None = None,
    report_path: Path | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    freeze_report_path = freeze_report_path or (paths.runtime_distribution_reports_root / "assess_phase1_canonical_freeze.json")
    compare_report_path = compare_report_path or (paths.runtime_distribution_reports_root / "compare_phase1_distribution_backends.json")
    first_levels_report_path = first_levels_report_path or (
        paths.runtime_distribution_reports_root / "analyze_phase1_distribution_first_levels.json"
    )
    backend_boundary_report_path = backend_boundary_report_path or (
        paths.runtime_distribution_reports_root / "assess_phase1_distribution_backend_boundary.json"
    )
    fp_r_identity_report_path = fp_r_identity_report_path or (
        paths.runtime_distribution_reports_root / "validate_phase1_distribution_identities.fp-r.json"
    )
    fpexe_identity_report_path = fpexe_identity_report_path or (
        paths.runtime_distribution_reports_root / "validate_phase1_distribution_identities.fpexe.json"
    )

    freeze_payload = json.loads(Path(freeze_report_path).read_text(encoding="utf-8"))
    compare_payload = json.loads(Path(compare_report_path).read_text(encoding="utf-8"))
    first_levels_payload = json.loads(Path(first_levels_report_path).read_text(encoding="utf-8"))
    backend_boundary_payload = json.loads(Path(backend_boundary_report_path).read_text(encoding="utf-8"))
    fp_r_identity_payload = json.loads(Path(fp_r_identity_report_path).read_text(encoding="utf-8"))
    fpexe_identity_payload = json.loads(Path(fpexe_identity_report_path).read_text(encoding="utf-8"))

    freeze_summary = dict(freeze_payload.get("freeze_summary", {}) or {})
    overall_boundary = dict(backend_boundary_payload.get("overall", {}) or {})
    ui_boundary = dict(backend_boundary_payload.get("ui_relief", {}) or {})
    transfer_boundary = dict(backend_boundary_payload.get("transfer_composite_medium", {}) or {})
    first_level_rows = {
        str(row.get("variant_id")): dict(row)
        for row in first_levels_payload.get("variants", [])
    }

    canonical_variants = [str(item) for item in freeze_summary.get("public_distribution_variants", [])]
    covered_variants = [str(item) for item in compare_payload.get("variant_ids", [])]
    missing_variants = [variant_id for variant_id in canonical_variants if variant_id not in covered_variants]
    compare_summary = dict(compare_payload.get("summary", {}) or {})
    left_backend = str(compare_payload.get("left_backend", "")).strip().lower()
    right_backend = str(compare_payload.get("right_backend", "")).strip().lower()
    compare_surface_valid = not (
        left_backend
        and right_backend
        and left_backend != right_backend
        and float(compare_summary.get("max_abs_diff", 0.0) or 0.0) <= 1e-12
        and bool(compare_payload.get("comparisons"))
    )
    fp_r_identity_passes = bool(dict(fp_r_identity_payload.get("summary", {}) or {}).get("passes"))
    fpexe_identity_passes = bool(dict(fpexe_identity_payload.get("summary", {}) or {}).get("passes"))
    canonical_surface_preserved = bool(freeze_summary.get("canonical_distribution_catalog_preserved")) and bool(
        freeze_summary.get("canonical_transfer_core_preserved")
    )

    ui_first_levels = dict(first_level_rows.get("ui-relief", {}) or {}).get("variables", {})
    ui_shock_first_levels = dict(first_level_rows.get("ui-shock", {}) or {}).get("variables", {})
    transfer_first_levels = dict(first_level_rows.get("transfer-composite-medium", {}) or {}).get("variables", {})

    def _snapshot(variable_rows: dict[str, Any], variable: str) -> dict[str, object]:
        row = dict(variable_rows.get(variable, {}) or {})
        return {
            "delta_ratio_abs": row.get("delta_ratio_abs"),
            "delta_sign_match": row.get("delta_sign_match"),
            "delta_abs_diff": row.get("delta_abs_diff"),
            "level_abs_diff": row.get("level_abs_diff"),
        }

    ui_focus = {
        variable: _snapshot(ui_first_levels, variable)
        for variable in _DISTRIBUTION_BOUNDARY_UI_VARIABLES
        if variable in ui_first_levels
    }
    ui_shock_focus = {
        variable: _snapshot(ui_shock_first_levels, variable)
        for variable in _DISTRIBUTION_BOUNDARY_UI_VARIABLES
        if variable in ui_shock_first_levels
    }
    transfer_focus = {
        variable: _snapshot(transfer_first_levels, variable)
        for variable in (
            *_DISTRIBUTION_BOUNDARY_TRANSFER_DIRECT_VARIABLES,
            *_DISTRIBUTION_BOUNDARY_TRANSFER_MACRO_VARIABLES,
        )
        if variable in transfer_first_levels
    }

    def _ratio_values(variable_rows: dict[str, dict[str, object]]) -> list[float]:
        values: list[float] = []
        for row in variable_rows.values():
            value = row.get("delta_ratio_abs")
            if value is None:
                continue
            values.append(float(value))
        return values

    def _all_sign_match(variable_rows: dict[str, dict[str, object]]) -> bool:
        rows = list(variable_rows.values())
        if not rows:
            return False
        return all(bool(row.get("delta_sign_match")) for row in rows)

    transfer_ratio_values = _ratio_values(transfer_focus)
    ui_shock_ratio_values = _ratio_values(ui_shock_focus)
    ui_shock_focus_all_sign_match = _all_sign_match(ui_shock_focus)
    ui_shock_focus_max_ratio = max(ui_shock_ratio_values) if ui_shock_ratio_values else None
    transfer_focus_all_sign_match = _all_sign_match(transfer_focus)
    transfer_focus_max_ratio = max(transfer_ratio_values) if transfer_ratio_values else None

    blockers: list[str] = []
    if not canonical_surface_preserved:
        blockers.append("canonical public scenario definitions are not preserved against origin/main")
    if not compare_surface_valid:
        blockers.append("canonical fp-r/fpexe compare surface appears invalid or contaminated because all cross-backend differences collapsed to zero")
    if not fp_r_identity_passes:
        blockers.append("fp-r does not pass the required identity surface on the current canonical validation set")
    if str(ui_boundary.get("assessment", "")) == "open_attenuation":
        blockers.append("ui-relief remains strongly attenuated versus fpexe on the unchanged canonical scenario")
    if ui_shock_focus and not ui_shock_focus_all_sign_match:
        blockers.append("ui-shock remains opposite-sign versus fpexe on the unchanged canonical scenario")
    if transfer_focus and transfer_focus_all_sign_match and transfer_focus_max_ratio is not None and transfer_focus_max_ratio < 0.25:
        blockers.append("transfer-composite-medium remains strongly attenuated versus fpexe on the unchanged canonical scenario")
    elif str(transfer_boundary.get("macro_channel_assessment", "")) == "block_macro_sign_flip":
        blockers.append("transfer-composite-medium still has a macro-income sign flip versus fpexe on the unchanged canonical scenario")
    if missing_variants:
        blockers.append("canonical parity matrix coverage is incomplete for some public variants")

    canonical_parity_ready = canonical_surface_preserved and fp_r_identity_passes and not blockers
    if canonical_parity_ready:
        status = "canonical-parity-ready"
        next_step = "freeze fp-r as defensible on the unchanged canonical scenario surface"
    elif canonical_surface_preserved and fp_r_identity_passes:
        status = "canonical-parity-blocked-by-behavior"
        next_step = "fix backend behavior on unchanged canonical scenarios only; do not use intervention artifacts as final evidence"
    else:
        status = "canonical-parity-not-ready"
        next_step = "repair canonical scenario preservation or fp-r identity behavior before making any parity claim"

    payload = {
        "freeze_report_path": str(freeze_report_path),
        "compare_report_path": str(compare_report_path),
        "first_levels_report_path": str(first_levels_report_path),
        "backend_boundary_report_path": str(backend_boundary_report_path),
        "fp_r_identity_report_path": str(fp_r_identity_report_path),
        "fpexe_identity_report_path": str(fpexe_identity_report_path),
        "coverage": {
            "canonical_variant_count": len(canonical_variants),
            "covered_variant_count": len(covered_variants),
            "covered_variants": covered_variants,
            "missing_variants": missing_variants,
            "compare_surface_valid": compare_surface_valid,
        },
        "ui_relief": {
            "assessment": ui_boundary.get("assessment"),
            "reason": ui_boundary.get("reason"),
            "identity_passes_required": ui_boundary.get("identity_passes_required"),
            "focus_variables": ui_focus,
        },
        "ui_shock": {
            "first_level_signs_match": ui_shock_focus_all_sign_match,
            "first_level_max_delta_ratio_abs": ui_shock_focus_max_ratio,
            "focus_variables": ui_shock_focus,
        },
        "transfer_composite_medium": {
            "direct_channel_assessment": transfer_boundary.get("direct_channel_assessment"),
            "macro_channel_assessment": transfer_boundary.get("macro_channel_assessment"),
            "reason": transfer_boundary.get("reason"),
            "identity_passes_required": transfer_boundary.get("identity_passes_required"),
            "focus_variables": transfer_focus,
            "first_level_signs_match": transfer_focus_all_sign_match,
            "first_level_max_delta_ratio_abs": transfer_focus_max_ratio,
        },
        "overall": {
            "status": status,
            "canonical_surface_preserved": canonical_surface_preserved,
            "fp_r_identity_passes": fp_r_identity_passes,
            "fpexe_identity_passes": fpexe_identity_passes,
            "canonical_parity_ready": canonical_parity_ready,
            "blockers": blockers,
            "recommendation": str(overall_boundary.get("recommendation", "")).strip(),
            "next_step": next_step,
        },
    }
    report_path = report_path or (paths.runtime_distribution_reports_root / "assess_phase1_distribution_canonical_parity.json")
    Path(report_path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "status": status,
        "canonical_parity_ready": canonical_parity_ready,
        "covered_variant_count": len(covered_variants),
        "missing_variant_count": len(missing_variants),
    }


def analyze_phase1_distribution_ui_attenuation(
    *,
    policy_gap_report_path: Path | None = None,
    driver_gap_report_path: Path | None = None,
    report_path: Path | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    policy_gap_report_path = policy_gap_report_path or (
        paths.runtime_distribution_reports_root / "analyze_phase1_distribution_policy_gap.json"
    )
    driver_gap_report_path = driver_gap_report_path or (
        paths.runtime_distribution_reports_root / "analyze_phase1_distribution_driver_gap.json"
    )

    policy_gap_payload = json.loads(policy_gap_report_path.read_text(encoding="utf-8"))
    driver_gap_payload = json.loads(driver_gap_report_path.read_text(encoding="utf-8"))

    ui_variant = None
    for row in policy_gap_payload.get("variants", []):
        if str(row.get("variant_id")) == "ui-relief":
            ui_variant = dict(row)
            break
    if ui_variant is None:
        raise KeyError("Policy-gap report is missing ui-relief.")

    left_backend = str(policy_gap_payload["left_backend"])
    right_backend = str(policy_gap_payload["right_backend"])
    variable_rows = dict(ui_variant.get("variables", {}))

    def _metric(variable: str) -> dict[str, object]:
        return dict(variable_rows.get(variable, {}))

    core_ratios = [
        float(dict(variable_rows[name]).get("first_gap_ratio_abs"))
        for name in _DISTRIBUTION_UI_ATTENUATION_CORE_VARIABLES
        if dict(variable_rows.get(name, {})).get("first_gap_ratio_abs") is not None
    ]
    core_median_ratio = None if not core_ratios else float(np.median(np.asarray(core_ratios, dtype=float)))

    lub_row = _metric("LUB")
    ub_row = _metric("UB")
    default_lub_delta = dict(lub_row.get(left_backend, {})).get("delta_first")
    reference_lub_delta = dict(lub_row.get(right_backend, {})).get("delta_first")
    default_ub_delta = dict(ub_row.get(left_backend, {})).get("delta_first")
    reference_ub_delta = dict(ub_row.get(right_backend, {})).get("delta_first")

    ui_driver = dict(driver_gap_payload.get("ui_retained_target_analysis", {}))
    uplift = dict(ui_driver.get("lub_uplift_breakdown", {}))
    period_2026q1 = dict(dict(ui_driver.get("periods", {})).get("2026.1", {}))
    experiment_minus_default = dict(period_2026q1.get("experiment_minus_default", {}))
    lub_equation_summary = dict(ui_driver.get("lub_equation_summary", {}))
    solve_lift_vs_default = uplift.get("solve_lift_vs_default")
    solve_step_from_previous = uplift.get("solve_step_from_previous")
    solve_share = None
    if solve_lift_vs_default is not None and solve_step_from_previous is not None:
        if abs(float(solve_lift_vs_default)) > 1e-12:
            solve_share = float(solve_step_from_previous) / float(solve_lift_vs_default)

    default_lub_flat = default_lub_delta is not None and abs(float(default_lub_delta)) <= 1e-12
    has_uifac_reference = bool(lub_equation_summary.get("has_uifac_reference", False))
    assessment = (
        "structural_lub_channel_block"
        if default_lub_flat and not has_uifac_reference and (solve_share is None or float(solve_share) < 0.5)
        else "mixed"
    )

    payload = {
        "policy_gap_report_path": str(policy_gap_report_path),
        "driver_gap_report_path": str(driver_gap_report_path),
        "overall": {
            "assessment": assessment,
            "reason": (
                "Default fp-r leaves LUB flat on ui-relief, so UB only picks up the direct UIFAC scaling. "
                "The retained-target experiment recovers some LUB movement, but most of the first-quarter gain "
                "comes from carrying a higher starting point rather than a large same-quarter solve response."
            ),
        },
        "default_fp_r_signature": {
            "core_variables": list(_DISTRIBUTION_UI_ATTENUATION_CORE_VARIABLES),
            "first_quarter_core_median_gap_ratio_abs": core_median_ratio,
            "lub_delta_first": None if default_lub_delta is None else float(default_lub_delta),
            "reference_lub_delta_first": None if reference_lub_delta is None else float(reference_lub_delta),
            "ub_delta_first": None if default_ub_delta is None else float(default_ub_delta),
            "reference_ub_delta_first": None if reference_ub_delta is None else float(reference_ub_delta),
            "lub_is_flat": bool(default_lub_flat),
            "variables": {
                name: {
                    "first_gap_ratio_abs": dict(variable_rows[name]).get("first_gap_ratio_abs"),
                    "first_sign_match": dict(variable_rows[name]).get("first_sign_match"),
                    left_backend: dict(dict(variable_rows[name]).get(left_backend, {})),
                    right_backend: dict(dict(variable_rows[name]).get(right_backend, {})),
                }
                for name in ("LUB", "UB", *_DISTRIBUTION_UI_ATTENUATION_CORE_VARIABLES)
                if name in variable_rows
            },
        },
        "retained_target_counterfactual": {
            "lub_equation_has_uifac_reference": has_uifac_reference,
            "carry_lift_vs_default": uplift.get("carry_lift_vs_default"),
            "solve_lift_vs_default": solve_lift_vs_default,
            "solve_step_from_previous": solve_step_from_previous,
            "same_quarter_solve_share_of_total_lub_lift": solve_share,
            "first_quarter_lifts_vs_default": {
                name: experiment_minus_default.get(name)
                for name in ("LUB", "UB", "YD", "GDPR", "RYDPC", "TRLOWZ", "IPOVALL", "IPOVCH")
                if name in experiment_minus_default
            },
            "levels": {
                "default_2026q1_lub": uplift.get("default_2026q1_lub"),
                "retained_target_previous_value": uplift.get("retained_target_previous_value"),
                "retained_target_evaluated_value": uplift.get("retained_target_evaluated_value"),
            },
        },
    }
    report_path = report_path or (paths.runtime_distribution_reports_root / "analyze_phase1_distribution_ui_attenuation.json")
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "assessment": str(payload["overall"]["assessment"]),
        "core_median_gap_ratio_abs": core_median_ratio,
    }


def analyze_phase1_distribution_transfer_macro_block(
    *,
    boundary_report_path: Path | None = None,
    driver_gap_report_path: Path | None = None,
    report_path: Path | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    boundary_report_path = boundary_report_path or (
        paths.runtime_distribution_reports_root / "assess_phase1_distribution_backend_boundary.json"
    )
    driver_gap_report_path = driver_gap_report_path or (
        paths.runtime_distribution_reports_root / "analyze_phase1_distribution_driver_gap.json"
    )

    boundary_payload = json.loads(boundary_report_path.read_text(encoding="utf-8"))
    driver_gap_payload = json.loads(driver_gap_report_path.read_text(encoding="utf-8"))

    transfer_boundary = dict(boundary_payload.get("transfer_composite_medium", {}))
    transfer_gap = dict(driver_gap_payload.get("transfer_income_gap_analysis", {}))
    period_2026q1 = dict(dict(transfer_gap.get("periods", {})).get("2026.1", {}))
    period_2029q4 = dict(dict(transfer_gap.get("periods", {})).get("2029.4", {}))

    yd_breakdown_2026q1 = dict(dict(period_2026q1.get("yd_component_breakdown", {})).get("scenario_minus_baseline", {}))
    yd_terms_2026q1 = list(dict(period_2026q1.get("yd_term_breakdown", {})).get("top_abs_delta_terms", []))
    yd_breakdown_2029q4 = dict(dict(period_2029q4.get("yd_component_breakdown", {})).get("scenario_minus_baseline", {}))
    yd_terms_2029q4 = list(dict(period_2029q4.get("yd_term_breakdown", {})).get("top_abs_delta_terms", []))
    gdpr_breakdown_2026q1 = dict(
        dict(period_2026q1.get("gdpr_component_breakdown", {})).get("scenario_minus_baseline", {})
    )
    rs_comparison_2026q1 = dict(period_2026q1.get("rs_equation_comparison", {}))
    if not rs_comparison_2026q1:
        rs_comparison_2026q1 = dict(transfer_gap.get("rs_equation_comparison", {}))
    intg_bridge_2029q4 = dict(period_2029q4.get("intg_driver_bridge", {}))
    jf_bridge_2029q4 = dict(period_2029q4.get("jf_driver_bridge", {}))

    payload = {
        "boundary_report_path": str(boundary_report_path),
        "driver_gap_report_path": str(driver_gap_report_path),
        "overall": {
            "assessment": "macro_income_sign_flip_block",
            "reason": (
                "Direct transfer and financing mechanics are aligned, but the macro-income block still turns negative. "
                "The first-quarter net-income path is pulled down by higher deductions and lower INTG, and the later path "
                "stays negative through persistent INTG/AAG drag plus a lower JF path."
            ),
        },
        "first_quarter_signature": {
            "direct_channel_assessment": transfer_boundary.get("direct_channel_assessment"),
            "macro_channel_assessment": transfer_boundary.get("macro_channel_assessment"),
            "direct_median_delta_ratio_abs": dict(transfer_boundary.get("first_quarter_direct_stats", {})).get(
                "median_delta_ratio_abs"
            ),
            "macro_median_delta_ratio_abs": dict(transfer_boundary.get("first_quarter_macro_stats", {})).get(
                "median_delta_ratio_abs"
            ),
            "macro_sign_mismatch_count": dict(transfer_boundary.get("first_quarter_macro_stats", {})).get(
                "sign_mismatch_count"
            ),
            "yd_component_breakdown": yd_breakdown_2026q1,
            "yd_top_abs_delta_terms": yd_terms_2026q1[:5],
            "gdpr_component_breakdown": gdpr_breakdown_2026q1,
            "rs_equation_delta": {
                "last_iteration_evaluated_value_delta": rs_comparison_2026q1.get("last_iteration_evaluated_value_delta"),
                "compiled_reference_deltas": dict(rs_comparison_2026q1.get("last_iteration_compiled_reference_deltas", {})),
            },
        },
        "late_path_signature": {
            "propagation_summary": dict(transfer_gap.get("propagation_summary", {})),
            "dynamics_summary": dict(transfer_gap.get("dynamics_summary", {})),
            "yd_component_breakdown_2029q4": yd_breakdown_2029q4,
            "yd_top_abs_delta_terms_2029q4": yd_terms_2029q4[:5],
            "intg_driver_bridge_2029q4": {
                "scenario_minus_baseline": dict(intg_bridge_2029q4.get("scenario_minus_baseline", {})),
                "delta_breakdown": dict(intg_bridge_2029q4.get("delta_breakdown", {})),
            },
            "jf_driver_bridge_2029q4": {
                "scenario_minus_baseline": dict(jf_bridge_2029q4.get("scenario_minus_baseline", {})),
                "delta_breakdown": dict(jf_bridge_2029q4.get("delta_breakdown", {})),
            },
        },
    }
    report_path = report_path or (
        paths.runtime_distribution_reports_root / "analyze_phase1_distribution_transfer_macro_block.json"
    )
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "assessment": str(payload["overall"]["assessment"]),
        "macro_sign_mismatch_count": int(payload["first_quarter_signature"]["macro_sign_mismatch_count"] or 0),
    }


def _comparison_loadformat_path(compare_payload: dict[str, object], row: dict[str, object], backend: str) -> Path:
    left_backend = str(compare_payload["left_backend"])
    right_backend = str(compare_payload["right_backend"])
    if backend == left_backend:
        return Path(str(row["left_loadformat_path"]))
    if backend == right_backend:
        return Path(str(row["right_loadformat_path"]))
    raise KeyError(f"Comparison report does not include backend: {backend}")


def _series_by_period(series_path: Path) -> dict[str, dict[str, float]]:
    periods, series = _read_fp_r_series_payload(series_path)
    payload: dict[str, dict[str, float]] = {}
    for idx, period in enumerate(periods):
        payload[str(period)] = {name: float(values[idx]) for name, values in series.items()}
    return payload


def _snapshot_for_period(
    by_period: dict[str, dict[str, float]],
    period: str,
    variables: tuple[str, ...] | list[str],
) -> dict[str, float | None]:
    row = by_period.get(str(period), {})
    return {str(name): (float(row[str(name)]) if str(name) in row else None) for name in variables}


def _delta_snapshot(
    left: dict[str, float | None],
    right: dict[str, float | None],
) -> dict[str, float | None]:
    payload: dict[str, float | None] = {}
    for name in left:
        left_value = left.get(name)
        right_value = right.get(name)
        if left_value is None or right_value is None:
            payload[name] = None
            continue
        payload[name] = float(right_value) - float(left_value)
    return payload


def _pct_delta_snapshot(
    baseline: dict[str, float | None],
    scenario: dict[str, float | None],
) -> dict[str, float | None]:
    payload: dict[str, float | None] = {}
    for name in baseline:
        baseline_value = baseline.get(name)
        scenario_value = scenario.get(name)
        if baseline_value is None or scenario_value is None or abs(float(baseline_value)) <= 1e-12:
            payload[name] = None
            continue
        payload[name] = float(scenario_value) / float(baseline_value) - 1.0
    return payload


def _previous_period(period: str) -> str:
    year, quarter = _period_key(period)
    if quarter == 1:
        return f"{year - 1}.4"
    return f"{year}.{quarter - 1}"


def _row_value(row: dict[str, float], name: str) -> float | None:
    if name not in row:
        return None
    return float(row[name])


def _product_or_none(*values: float | None) -> float | None:
    if any(value is None for value in values):
        return None
    result = 1.0
    for value in values:
        result *= float(value)
    return result


def _sum_or_none(*values: float | None) -> float | None:
    if any(value is None for value in values):
        return None
    return float(sum(float(value) for value in values))


def _yd_component_breakdown(row: dict[str, float]) -> dict[str, float | None]:
    private_household_labor = _product_or_none(
        _row_value(row, "WF"),
        _row_value(row, "JF"),
        _sum_or_none(_row_value(row, "HN"), None if _row_value(row, "HO") is None else 1.5 * _row_value(row, "HO")),
    )
    federal_labor = _product_or_none(_row_value(row, "WG"), _row_value(row, "JG"), _row_value(row, "HG"))
    military_labor = _product_or_none(_row_value(row, "WM"), _row_value(row, "JM"), _row_value(row, "HM"))
    state_local_labor = _product_or_none(_row_value(row, "WS"), _row_value(row, "JS"), _row_value(row, "HS"))
    labor_total = _sum_or_none(private_household_labor, federal_labor, military_labor, state_local_labor)
    property_and_interest_income = _sum_or_none(
        _row_value(row, "RNT"),
        _row_value(row, "INTZ"),
        _row_value(row, "INTF"),
        _row_value(row, "INTG"),
        None if _row_value(row, "INTGR") is None else -_row_value(row, "INTGR"),
        _row_value(row, "INTS"),
        _row_value(row, "DF"),
        _row_value(row, "DB"),
        _row_value(row, "DR"),
        _row_value(row, "DG"),
        _row_value(row, "DS"),
    )
    transfers = _sum_or_none(
        _row_value(row, "TRFH"),
        _row_value(row, "TRGH"),
        _row_value(row, "TRSH"),
        _row_value(row, "UB"),
    )
    deductions = _sum_or_none(
        _row_value(row, "SIHG"),
        _row_value(row, "SIHS"),
        _row_value(row, "THG"),
        _row_value(row, "THS"),
        _row_value(row, "TRHR"),
        _row_value(row, "SIGG"),
        _row_value(row, "SISS"),
    )
    reconstructed = (
        None
        if None in (labor_total, property_and_interest_income, transfers, deductions)
        else float(labor_total) + float(property_and_interest_income) + float(transfers) - float(deductions)
    )
    yd_value = _row_value(row, "YD")
    return {
        "private_household_labor": private_household_labor,
        "federal_labor": federal_labor,
        "military_labor": military_labor,
        "state_local_labor": state_local_labor,
        "labor_total": labor_total,
        "property_and_interest_income": property_and_interest_income,
        "transfers": transfers,
        "deductions": deductions,
        "reconstructed_yd": reconstructed,
        "identity_residual": (
            None if yd_value is None or reconstructed is None else float(yd_value) - float(reconstructed)
        ),
    }


def _yd_term_contributions(row: dict[str, float]) -> dict[str, float | None]:
    return {
        "private_household_labor": _product_or_none(
            _row_value(row, "WF"),
            _row_value(row, "JF"),
            _sum_or_none(_row_value(row, "HN"), None if _row_value(row, "HO") is None else 1.5 * _row_value(row, "HO")),
        ),
        "federal_labor": _product_or_none(_row_value(row, "WG"), _row_value(row, "JG"), _row_value(row, "HG")),
        "military_labor": _product_or_none(_row_value(row, "WM"), _row_value(row, "JM"), _row_value(row, "HM")),
        "state_local_labor": _product_or_none(_row_value(row, "WS"), _row_value(row, "JS"), _row_value(row, "HS")),
        "RNT": _row_value(row, "RNT"),
        "INTZ": _row_value(row, "INTZ"),
        "INTF": _row_value(row, "INTF"),
        "INTG": _row_value(row, "INTG"),
        "INTGR_effect": None if _row_value(row, "INTGR") is None else -_row_value(row, "INTGR"),
        "INTS": _row_value(row, "INTS"),
        "DF": _row_value(row, "DF"),
        "DB": _row_value(row, "DB"),
        "DR": _row_value(row, "DR"),
        "DG": _row_value(row, "DG"),
        "DS": _row_value(row, "DS"),
        "TRFH": _row_value(row, "TRFH"),
        "TRGH": _row_value(row, "TRGH"),
        "TRSH": _row_value(row, "TRSH"),
        "UB": _row_value(row, "UB"),
        "SIHG_effect": None if _row_value(row, "SIHG") is None else -_row_value(row, "SIHG"),
        "SIHS_effect": None if _row_value(row, "SIHS") is None else -_row_value(row, "SIHS"),
        "THG_effect": None if _row_value(row, "THG") is None else -_row_value(row, "THG"),
        "THS_effect": None if _row_value(row, "THS") is None else -_row_value(row, "THS"),
        "TRHR_effect": None if _row_value(row, "TRHR") is None else -_row_value(row, "TRHR"),
        "SIGG_effect": None if _row_value(row, "SIGG") is None else -_row_value(row, "SIGG"),
        "SISS_effect": None if _row_value(row, "SISS") is None else -_row_value(row, "SISS"),
    }


def _gdpr_component_breakdown(row: dict[str, float]) -> dict[str, float | None]:
    sector_hours_adjustment = _product_or_none(
        _row_value(row, "PSI13"),
        _sum_or_none(
            _product_or_none(_row_value(row, "JG"), _row_value(row, "HG")),
            _product_or_none(_row_value(row, "JM"), _row_value(row, "HM")),
            _product_or_none(_row_value(row, "JS"), _row_value(row, "HS")),
        ),
    )
    output_y = _row_value(row, "Y")
    statp = _row_value(row, "STATP")
    reconstructed = _sum_or_none(output_y, sector_hours_adjustment, statp)
    gdpr_value = _row_value(row, "GDPR")
    return {
        "output_y": output_y,
        "sector_hours_adjustment": sector_hours_adjustment,
        "statp": statp,
        "reconstructed_gdpr": reconstructed,
        "identity_residual": (
            None if gdpr_value is None or reconstructed is None else float(gdpr_value) - float(reconstructed)
        ),
    }


def _top_abs_delta_terms(delta_map: dict[str, float | None], *, limit: int = 8) -> list[dict[str, float]]:
    rows = [
        {"term": str(name), "delta": float(value), "abs_delta": abs(float(value))}
        for name, value in delta_map.items()
        if value is not None
    ]
    rows.sort(key=lambda row: row["abs_delta"], reverse=True)
    return rows[:limit]


def _pcy_growth_bridge(
    current_row: dict[str, float],
    lag_row: dict[str, float],
) -> dict[str, float | None]:
    current_y = _row_value(current_row, "Y")
    lag_y = _row_value(lag_row, "Y")
    reconstructed = (
        None
        if current_y is None or lag_y is None or abs(float(lag_y)) <= 1e-12
        else 100.0 * ((float(current_y) / float(lag_y)) ** 4 - 1.0)
    )
    pcy_value = _row_value(current_row, "PCY")
    return {
        "current_y": current_y,
        "lagged_y": lag_y,
        "reconstructed_pcy": reconstructed,
        "identity_residual": (
            None if pcy_value is None or reconstructed is None else float(pcy_value) - float(reconstructed)
        ),
    }


def _transfer_intg_upstream_bridge(row: dict[str, float]) -> dict[str, float | None]:
    return {
        "INTG": _row_value(row, "INTG"),
        "INTGZ": _row_value(row, "INTGZ"),
        "RQG": _row_value(row, "RQG"),
        "AAG": _row_value(row, "AAG"),
    }


def _transfer_ths_upstream_bridge(row: dict[str, float]) -> dict[str, float | None]:
    return {
        "TRSHQ": _row_value(row, "TRSHQ"),
        "SSFAC": _row_value(row, "SSFAC"),
        "GDPD": _row_value(row, "GDPD"),
        "TRSH": _row_value(row, "TRSH"),
        "D1S": _row_value(row, "D1S"),
        "YT": _row_value(row, "YT"),
        "TSLSHR": _row_value(row, "TSLSHR"),
        "THS": _row_value(row, "THS"),
    }


def _transfer_private_labor_bridge(row: dict[str, float]) -> dict[str, float | None]:
    return {
        "WF": _row_value(row, "WF"),
        "JF": _row_value(row, "JF"),
        "HN": _row_value(row, "HN"),
        "HO": _row_value(row, "HO"),
        "private_household_labor": _product_or_none(
            _row_value(row, "WF"),
            _row_value(row, "JF"),
            _sum_or_none(_row_value(row, "HN"), None if _row_value(row, "HO") is None else 1.5 * _row_value(row, "HO")),
        ),
    }


def _transfer_sg_upstream_bridge(row: dict[str, float]) -> dict[str, float | None]:
    return {
        "SG": _row_value(row, "SG"),
        "RECG": _row_value(row, "RECG"),
        "EXPG": _row_value(row, "EXPG"),
        "PUG": _row_value(row, "PUG"),
        "TRGH": _row_value(row, "TRGH"),
        "TRGR": _row_value(row, "TRGR"),
        "TRGS": _row_value(row, "TRGS"),
        "INTG": _row_value(row, "INTG"),
        "SUBG": _row_value(row, "SUBG"),
        "IGZ": _row_value(row, "IGZ"),
        "UB": _row_value(row, "UB"),
    }


def _transfer_aag_upstream_bridge(
    row: dict[str, float],
    lag_row: dict[str, float],
) -> dict[str, float | None]:
    current_br_minus_bo = (
        None
        if _row_value(row, "BR") is None or _row_value(row, "BO") is None
        else _row_value(row, "BR") - _row_value(row, "BO")
    )
    lag_br_minus_bo = (
        None
        if _row_value(lag_row, "BR") is None or _row_value(lag_row, "BO") is None
        else _row_value(lag_row, "BR") - _row_value(lag_row, "BO")
    )
    return {
        "AG": _row_value(row, "AG"),
        "AAG": _row_value(row, "AAG"),
        "SG": _row_value(row, "SG"),
        "MG": _row_value(row, "MG"),
        "MG_lag": _row_value(lag_row, "MG"),
        "CUR": _row_value(row, "CUR"),
        "CUR_lag": _row_value(lag_row, "CUR"),
        "BR_minus_BO": current_br_minus_bo,
        "BR_minus_BO_lag": lag_br_minus_bo,
    }


def _extract_legacy_create_override(scenario_path: Path, variable: str) -> float | None:
    if not scenario_path.exists():
        return None
    pattern = re.compile(rf"CREATE\s+{re.escape(variable)}\s*=\s*([-+0-9.eE]+);", re.IGNORECASE)
    for line in scenario_path.read_text(encoding="utf-8").splitlines():
        matches = pattern.findall(line)
        if matches:
            return float(matches[-1])
    return None


def _extract_scenario_override(scenario_path: Path, variable: str) -> float | None:
    if not scenario_path.exists():
        return None
    try:
        payload = yaml.safe_load(scenario_path.read_text(encoding="utf-8"))
    except Exception:
        payload = None
    if isinstance(payload, dict):
        overrides = payload.get("overrides")
        if isinstance(overrides, dict):
            entry = overrides.get(variable)
            if isinstance(entry, dict):
                value = entry.get("value")
                if isinstance(value, (int, float)):
                    return float(value)
    return _extract_legacy_create_override(scenario_path, variable)


def _forecast_periods(by_period: dict[str, dict[str, float]]) -> list[str]:
    return sorted(period for period in by_period if str(period) >= _DISTRIBUTION_FORECAST_START)


def _series_delta_by_period(
    baseline_by_period: dict[str, dict[str, float]],
    scenario_by_period: dict[str, dict[str, float]],
    variable: str,
) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    for period in _forecast_periods(baseline_by_period):
        if period not in scenario_by_period:
            continue
        base_row = baseline_by_period[period]
        scenario_row = scenario_by_period[period]
        if variable not in base_row or variable not in scenario_row:
            continue
        rows.append(
            {
                "period": period,
                "delta": float(scenario_row[variable]) - float(base_row[variable]),
            }
        )
    return rows


def _all_nonincreasing(values: list[float], tol: float = 1e-12) -> bool:
    return all(values[idx] <= values[idx - 1] + tol for idx in range(1, len(values)))


def _all_nondecreasing(values: list[float], tol: float = 1e-12) -> bool:
    return all(values[idx] + tol >= values[idx - 1] for idx in range(1, len(values)))


def _all_constant(values: list[float], tol: float = 1e-12) -> bool:
    if not values:
        return True
    first = values[0]
    return all(abs(value - first) <= tol for value in values[1:])


def _driver_gap_ui_experiment_series_path(paths) -> Path:
    return (
        paths.runtime_distribution_root
        / "artifacts-cmp-fp-r-experiment"
        / "ui_relief_retain_reduced_eq_only_patched_full"
        / "work"
        / "fp_r_series.csv"
    )


def _resolve_artifact_companion_path(loadformat_path: Path, name: str) -> Path:
    direct = loadformat_path.with_name(name)
    if direct.exists():
        return direct
    work_child = loadformat_path.parent / "work" / name
    if work_child.exists():
        return work_child
    return direct


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _load_equation_input_rows(path: Path, *, period: str, target: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if str(row.get("period", "")) != str(period):
                continue
            if str(row.get("target", "")) != str(target):
                continue
            rows.append(row)
    return rows


def _format_reference_name(variable: str, lag: str) -> str:
    lag_value = int(str(lag))
    if lag_value == 0:
        return str(variable)
    return f"{variable}({lag_value})"


def _trace_target_summary(
    *,
    equation_input_path: Path,
    period: str,
    target: str,
) -> dict[str, object]:
    if not equation_input_path.exists():
        raise FileNotFoundError(f"Missing equation input snapshot: {equation_input_path}")

    target_rows = _load_equation_input_rows(equation_input_path, period=period, target=target)
    if not target_rows:
        raise KeyError(f"No equation-input rows for {target} at {period}")

    iterations = sorted({int(str(row.get("iteration", "0"))) for row in target_rows if str(row.get("iteration", "")).strip()})
    first_iteration = iterations[0]
    last_iteration = iterations[-1]
    first_rows = [row for row in target_rows if int(str(row.get("iteration", "0"))) == first_iteration]
    last_rows = [row for row in target_rows if int(str(row.get("iteration", "0"))) == last_iteration]

    def _trace_value(rows: list[dict[str, str]], name: str) -> float | None:
        match = next((row for row in rows if str(row.get("trace_kind", "")) == name), None)
        if match is None:
            return None
        return float(str(match["value"]))

    def _references(rows: list[dict[str, str]], trace_kind: str) -> dict[str, float]:
        payload: dict[str, float] = {}
        for row in rows:
            if str(row.get("trace_kind", "")) != trace_kind:
                continue
            variable = str(row.get("variable", ""))
            lag = str(row.get("lag", "0"))
            payload[_format_reference_name(variable, lag)] = float(str(row["value"]))
        return payload

    def _iteration_summary(rows: list[dict[str, str]], iteration: int) -> dict[str, object]:
        previous_value = _trace_value(rows, "previous_value")
        evaluated_structural = _trace_value(rows, "evaluated_structural")
        evaluated_value = _trace_value(rows, "evaluated_value")
        return {
            "iteration": int(iteration),
            "previous_value": previous_value,
            "evaluated_structural": evaluated_structural,
            "evaluated_value": evaluated_value,
            "evaluated_minus_previous": (
                None
                if previous_value is None or evaluated_value is None
                else float(evaluated_value) - float(previous_value)
            ),
            "structural_minus_previous": (
                None
                if previous_value is None or evaluated_structural is None
                else float(evaluated_structural) - float(previous_value)
            ),
            "compiled_references": _references(rows, "compiled_reference"),
            "active_fsr_references": _references(rows, "active_fsr_reference"),
        }

    return {
        "period": str(period),
        "target": str(target),
        "first_iteration": _iteration_summary(first_rows, first_iteration),
        "last_iteration": _iteration_summary(last_rows, last_iteration),
    }


def _maybe_trace_target_summary(
    *,
    equation_input_path: Path,
    period: str,
    target: str,
) -> dict[str, object] | None:
    try:
        return _trace_target_summary(
            equation_input_path=equation_input_path,
            period=period,
            target=target,
        )
    except (FileNotFoundError, KeyError):
        return None


def _equation_summary(
    *,
    equation_input_path: Path,
    estimation_equations_path: Path,
    period: str,
    target: str,
) -> dict[str, object]:
    if not equation_input_path.exists():
        raise FileNotFoundError(f"Missing equation input snapshot: {equation_input_path}")
    if not estimation_equations_path.exists():
        raise FileNotFoundError(f"Missing estimation equations snapshot: {estimation_equations_path}")

    target_rows = _load_equation_input_rows(equation_input_path, period=period, target=target)
    if not target_rows:
        raise KeyError(f"No equation-input rows for {target} at {period}")

    iterations = sorted({int(str(row.get("iteration", "0"))) for row in target_rows if str(row.get("iteration", "")).strip()})
    first_iteration = iterations[0]
    last_iteration = iterations[-1]
    first_rows = [row for row in target_rows if int(str(row.get("iteration", "0"))) == first_iteration]
    last_rows = [row for row in target_rows if int(str(row.get("iteration", "0"))) == last_iteration]

    estimation_rows = _load_csv_rows(estimation_equations_path)
    equation_row = next(
        (row for row in estimation_rows if str(row.get("target", "")) == str(target)),
        None,
    )
    if equation_row is None:
        raise KeyError(f"No estimation-equation row for target: {target}")

    def _trace_value(rows: list[dict[str, str]], name: str) -> float | None:
        match = next((row for row in rows if str(row.get("trace_kind", "")) == name), None)
        if match is None:
            return None
        return float(str(match["value"]))

    def _references(rows: list[dict[str, str]], trace_kind: str) -> dict[str, float]:
        payload: dict[str, float] = {}
        for row in rows:
            if str(row.get("trace_kind", "")) != trace_kind:
                continue
            variable = str(row.get("variable", ""))
            lag = str(row.get("lag", "0"))
            payload[_format_reference_name(variable, lag)] = float(str(row["value"]))
        return payload

    def _iteration_summary(rows: list[dict[str, str]], iteration: int) -> dict[str, object]:
        previous_value = _trace_value(rows, "previous_value")
        evaluated_structural = _trace_value(rows, "evaluated_structural")
        evaluated_value = _trace_value(rows, "evaluated_value")
        return {
            "iteration": int(iteration),
            "previous_value": previous_value,
            "evaluated_structural": evaluated_structural,
            "evaluated_value": evaluated_value,
            "evaluated_minus_previous": (
                None
                if previous_value is None or evaluated_value is None
                else float(evaluated_value) - float(previous_value)
            ),
            "structural_minus_previous": (
                None
                if previous_value is None or evaluated_structural is None
                else float(evaluated_structural) - float(previous_value)
            ),
            "compiled_references": _references(rows, "compiled_reference"),
            "active_fsr_references": _references(rows, "active_fsr_reference"),
        }

    fsr_names = tuple(
        name
        for name in str(equation_row.get("fsr_reference_names", "")).split()
        if name and name.upper() != "NA"
    )
    active_fsr_names = tuple(
        name
        for name in str(equation_row.get("active_fsr_reference_names", "")).split()
        if name and name.upper() != "NA"
    )

    return {
        "period": str(period),
        "target": str(target),
        "equation_number": int(str(equation_row.get("equation_number", "0"))),
        "method": str(equation_row.get("method", "")),
        "fsr_reference_names": list(fsr_names),
        "active_fsr_reference_names": list(active_fsr_names),
        "has_uifac_reference": bool("UIFAC" in fsr_names or "UIFAC" in active_fsr_names),
        "first_iteration": _iteration_summary(first_rows, first_iteration),
        "last_iteration": _iteration_summary(last_rows, last_iteration),
    }


def _maybe_equation_summary(
    *,
    equation_input_path: Path,
    estimation_equations_path: Path,
    period: str,
    target: str,
) -> dict[str, object] | None:
    try:
        return _equation_summary(
            equation_input_path=equation_input_path,
            estimation_equations_path=estimation_equations_path,
            period=period,
            target=target,
        )
    except (FileNotFoundError, KeyError):
        return None


def _compiled_reference_deltas(
    baseline_summary: dict[str, object] | None,
    scenario_summary: dict[str, object] | None,
) -> dict[str, float]:
    if baseline_summary is None or scenario_summary is None:
        return {}
    baseline_refs = dict(baseline_summary.get("last_iteration", {}).get("compiled_references", {}))
    scenario_refs = dict(scenario_summary.get("last_iteration", {}).get("compiled_references", {}))
    keys = sorted(set(baseline_refs) | set(scenario_refs))
    return {
        name: float(scenario_refs.get(name, 0.0)) - float(baseline_refs.get(name, 0.0))
        for name in keys
    }


def _transfer_intg_driver_identity(row: dict[str, float]) -> dict[str, float | None]:
    intgz = _row_value(row, "INTGZ")
    aag = _row_value(row, "AAG")
    reconstructed = _product_or_none(aag, intgz)
    intg = _row_value(row, "INTG")
    return {
        "INTG": intg,
        "INTGZ": intgz,
        "AAG": aag,
        "reconstructed_intg": reconstructed,
        "identity_residual": (
            None if intg is None or reconstructed is None else float(intg) - float(reconstructed)
        ),
    }


def _transfer_intg_driver_delta_breakdown(
    baseline_row: dict[str, float],
    scenario_row: dict[str, float],
) -> dict[str, float | None]:
    base_aag = _row_value(baseline_row, "AAG")
    base_intgz = _row_value(baseline_row, "INTGZ")
    scenario_aag = _row_value(scenario_row, "AAG")
    scenario_intgz = _row_value(scenario_row, "INTGZ")
    if None in (base_aag, base_intgz, scenario_aag, scenario_intgz):
        return {
            "aag_component": None,
            "intgz_component": None,
            "interaction_component": None,
            "reconstructed_delta": None,
        }
    aag_component = (float(scenario_aag) - float(base_aag)) * float(base_intgz)
    intgz_component = float(base_aag) * (float(scenario_intgz) - float(base_intgz))
    interaction_component = (float(scenario_aag) - float(base_aag)) * (float(scenario_intgz) - float(base_intgz))
    return {
        "aag_component": aag_component,
        "intgz_component": intgz_component,
        "interaction_component": interaction_component,
        "reconstructed_delta": aag_component + intgz_component + interaction_component,
    }


def _transfer_jf_driver_identity(
    row: dict[str, float],
    lag_row: dict[str, float],
) -> dict[str, float | None]:
    jf = _row_value(row, "JF")
    lagged_jf = _row_value(lag_row, "JF")
    ljf1 = _row_value(row, "LJF1")
    reconstructed = None if lagged_jf is None or ljf1 is None else float(lagged_jf) * float(np.exp(float(ljf1)))
    return {
        "JF": jf,
        "JF_lag": lagged_jf,
        "LJF1": ljf1,
        "reconstructed_jf": reconstructed,
        "identity_residual": (
            None if jf is None or reconstructed is None else float(jf) - float(reconstructed)
        ),
    }


def _transfer_jf_driver_delta_breakdown(
    baseline_row: dict[str, float],
    scenario_row: dict[str, float],
    baseline_lag_row: dict[str, float],
    scenario_lag_row: dict[str, float],
) -> dict[str, float | None]:
    base_lagged_jf = _row_value(baseline_lag_row, "JF")
    scenario_lagged_jf = _row_value(scenario_lag_row, "JF")
    base_ljf1 = _row_value(baseline_row, "LJF1")
    scenario_ljf1 = _row_value(scenario_row, "LJF1")
    if None in (base_lagged_jf, scenario_lagged_jf, base_ljf1, scenario_ljf1):
        return {
            "lagged_jf_component": None,
            "ljf1_component": None,
            "interaction_component": None,
            "reconstructed_delta": None,
        }
    base_exp = float(np.exp(float(base_ljf1)))
    scenario_exp = float(np.exp(float(scenario_ljf1)))
    lagged_jf_component = (float(scenario_lagged_jf) - float(base_lagged_jf)) * base_exp
    ljf1_component = float(base_lagged_jf) * (scenario_exp - base_exp)
    interaction_component = (float(scenario_lagged_jf) - float(base_lagged_jf)) * (scenario_exp - base_exp)
    return {
        "lagged_jf_component": lagged_jf_component,
        "ljf1_component": ljf1_component,
        "interaction_component": interaction_component,
        "reconstructed_delta": lagged_jf_component + ljf1_component + interaction_component,
    }


def _transfer_ly1_driver_identity(
    row: dict[str, float],
    lag_row: dict[str, float],
) -> dict[str, float | None]:
    y_value = _row_value(row, "Y")
    lagged_y = _row_value(lag_row, "Y")
    ly1_value = _row_value(row, "LY1")
    reconstructed = (
        None
        if y_value is None or lagged_y is None or float(y_value) <= 0.0 or float(lagged_y) <= 0.0
        else float(np.log(float(y_value) / float(lagged_y)))
    )
    return {
        "LY1": ly1_value,
        "Y": y_value,
        "Y_lag": lagged_y,
        "reconstructed_ly1": reconstructed,
        "identity_residual": (
            None if ly1_value is None or reconstructed is None else float(ly1_value) - float(reconstructed)
        ),
    }


def _transfer_ly1_driver_delta_breakdown(
    baseline_row: dict[str, float],
    scenario_row: dict[str, float],
    baseline_lag_row: dict[str, float],
    scenario_lag_row: dict[str, float],
) -> dict[str, float | None]:
    base_y = _row_value(baseline_row, "Y")
    scenario_y = _row_value(scenario_row, "Y")
    base_y_lag = _row_value(baseline_lag_row, "Y")
    scenario_y_lag = _row_value(scenario_lag_row, "Y")
    if None in (base_y, scenario_y, base_y_lag, scenario_y_lag):
        return {
            "current_y_component": None,
            "lagged_y_component": None,
            "reconstructed_delta": None,
        }
    if min(float(base_y), float(scenario_y), float(base_y_lag), float(scenario_y_lag)) <= 0.0:
        return {
            "current_y_component": None,
            "lagged_y_component": None,
            "reconstructed_delta": None,
        }
    current_y_component = float(np.log(float(scenario_y) / float(base_y)))
    lagged_y_component = -float(np.log(float(scenario_y_lag) / float(base_y_lag)))
    return {
        "current_y_component": current_y_component,
        "lagged_y_component": lagged_y_component,
        "reconstructed_delta": current_y_component + lagged_y_component,
    }


def _transfer_ag_driver_identity(
    row: dict[str, float],
    lag_row: dict[str, float],
) -> dict[str, float | None]:
    current_br_minus_bo = (
        None
        if _row_value(row, "BR") is None or _row_value(row, "BO") is None
        else _row_value(row, "BR") - _row_value(row, "BO")
    )
    lagged_br_minus_bo = (
        None
        if _row_value(lag_row, "BR") is None or _row_value(lag_row, "BO") is None
        else _row_value(lag_row, "BR") - _row_value(lag_row, "BO")
    )
    ag = _row_value(row, "AG")
    ag_lag = _row_value(lag_row, "AG")
    sg = _row_value(row, "SG")
    mg = _row_value(row, "MG")
    mg_lag = _row_value(lag_row, "MG")
    cur = _row_value(row, "CUR")
    cur_lag = _row_value(lag_row, "CUR")
    reconstructed = _sum_or_none(
        ag_lag,
        sg,
        None if mg is None else -mg,
        mg_lag,
        cur,
        None if cur_lag is None else -cur_lag,
        current_br_minus_bo,
        None if lagged_br_minus_bo is None else -lagged_br_minus_bo,
    )
    return {
        "AG": ag,
        "AG_lag": ag_lag,
        "SG": sg,
        "MG": mg,
        "MG_lag": mg_lag,
        "CUR": cur,
        "CUR_lag": cur_lag,
        "BR_minus_BO": current_br_minus_bo,
        "BR_minus_BO_lag": lagged_br_minus_bo,
        "reconstructed_ag": reconstructed,
        "identity_residual": None if ag is None or reconstructed is None else float(ag) - float(reconstructed),
    }


def _transfer_ag_driver_delta_breakdown(
    baseline_row: dict[str, float],
    scenario_row: dict[str, float],
    baseline_lag_row: dict[str, float],
    scenario_lag_row: dict[str, float],
) -> dict[str, float | None]:
    def _br_minus_bo(row: dict[str, float]) -> float | None:
        br = _row_value(row, "BR")
        bo = _row_value(row, "BO")
        if br is None or bo is None:
            return None
        return float(br) - float(bo)

    base_ag_lag = _row_value(baseline_lag_row, "AG")
    scenario_ag_lag = _row_value(scenario_lag_row, "AG")
    base_sg = _row_value(baseline_row, "SG")
    scenario_sg = _row_value(scenario_row, "SG")
    base_mg = _row_value(baseline_row, "MG")
    scenario_mg = _row_value(scenario_row, "MG")
    base_mg_lag = _row_value(baseline_lag_row, "MG")
    scenario_mg_lag = _row_value(scenario_lag_row, "MG")
    base_cur = _row_value(baseline_row, "CUR")
    scenario_cur = _row_value(scenario_row, "CUR")
    base_cur_lag = _row_value(baseline_lag_row, "CUR")
    scenario_cur_lag = _row_value(scenario_lag_row, "CUR")
    base_br_minus_bo = _br_minus_bo(baseline_row)
    scenario_br_minus_bo = _br_minus_bo(scenario_row)
    base_br_minus_bo_lag = _br_minus_bo(baseline_lag_row)
    scenario_br_minus_bo_lag = _br_minus_bo(scenario_lag_row)
    if None in (
        base_ag_lag,
        scenario_ag_lag,
        base_sg,
        scenario_sg,
        base_mg,
        scenario_mg,
        base_mg_lag,
        scenario_mg_lag,
        base_cur,
        scenario_cur,
        base_cur_lag,
        scenario_cur_lag,
        base_br_minus_bo,
        scenario_br_minus_bo,
        base_br_minus_bo_lag,
        scenario_br_minus_bo_lag,
    ):
        return {
            "ag_lag_component": None,
            "sg_component": None,
            "mg_component": None,
            "mg_lag_component": None,
            "cur_component": None,
            "cur_lag_component": None,
            "br_minus_bo_component": None,
            "br_minus_bo_lag_component": None,
            "reconstructed_delta": None,
        }
    ag_lag_component = float(scenario_ag_lag) - float(base_ag_lag)
    sg_component = float(scenario_sg) - float(base_sg)
    mg_component = -float(scenario_mg) + float(base_mg)
    mg_lag_component = float(scenario_mg_lag) - float(base_mg_lag)
    cur_component = float(scenario_cur) - float(base_cur)
    cur_lag_component = -float(scenario_cur_lag) + float(base_cur_lag)
    br_minus_bo_component = float(scenario_br_minus_bo) - float(base_br_minus_bo)
    br_minus_bo_lag_component = -float(scenario_br_minus_bo_lag) + float(base_br_minus_bo_lag)
    return {
        "ag_lag_component": ag_lag_component,
        "sg_component": sg_component,
        "mg_component": mg_component,
        "mg_lag_component": mg_lag_component,
        "cur_component": cur_component,
        "cur_lag_component": cur_lag_component,
        "br_minus_bo_component": br_minus_bo_component,
        "br_minus_bo_lag_component": br_minus_bo_lag_component,
        "reconstructed_delta": (
            ag_lag_component
            +
            sg_component
            + mg_component
            + mg_lag_component
            + cur_component
            + cur_lag_component
            + br_minus_bo_component
            + br_minus_bo_lag_component
        ),
    }


def analyze_phase1_distribution_driver_gap(
    *,
    compare_report_path: Path | None = None,
    ui_variant_id: str = _DISTRIBUTION_DRIVER_GAP_UI_VARIANT_ID,
    transfer_variant_id: str = _DISTRIBUTION_DRIVER_GAP_TRANSFER_VARIANT_ID,
    backend: str = _DISTRIBUTION_DEFAULT_BACKEND,
    ui_experiment_series_path: Path | None = None,
    report_path: Path | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    compare_report_path = compare_report_path or (
        paths.runtime_distribution_reports_root / "compare_phase1_distribution_backends.json"
    )
    compare_payload = json.loads(compare_report_path.read_text(encoding="utf-8"))
    comparisons = {str(row["variant_id"]): dict(row) for row in compare_payload.get("comparisons", [])}
    if "baseline-observed" not in comparisons:
        raise KeyError("Comparison report is missing baseline-observed.")
    if ui_variant_id not in comparisons:
        raise KeyError(f"Comparison report is missing {ui_variant_id}.")
    if transfer_variant_id not in comparisons:
        raise KeyError(f"Comparison report is missing {transfer_variant_id}.")

    baseline_loadformat_path = _comparison_loadformat_path(compare_payload, comparisons["baseline-observed"], backend)
    ui_loadformat_path = _comparison_loadformat_path(compare_payload, comparisons[ui_variant_id], backend)
    transfer_loadformat_path = _comparison_loadformat_path(compare_payload, comparisons[transfer_variant_id], backend)

    baseline_series_path = _resolve_artifact_companion_path(baseline_loadformat_path, "fp_r_series.csv")
    ui_series_path = _resolve_artifact_companion_path(ui_loadformat_path, "fp_r_series.csv")
    transfer_series_path = _resolve_artifact_companion_path(transfer_loadformat_path, "fp_r_series.csv")
    ui_experiment_series_path = ui_experiment_series_path or _driver_gap_ui_experiment_series_path(paths)
    ui_experiment_dir = ui_experiment_series_path.parent

    baseline_by_period = _series_by_period(baseline_series_path)
    ui_by_period = _series_by_period(ui_series_path)
    transfer_by_period = _series_by_period(transfer_series_path)
    ui_experiment_by_period = _series_by_period(ui_experiment_series_path)
    baseline_equation_input_path = _resolve_artifact_companion_path(baseline_loadformat_path, "EQUATION_INPUT_SNAPSHOT.csv")
    transfer_equation_input_path = _resolve_artifact_companion_path(transfer_loadformat_path, "EQUATION_INPUT_SNAPSHOT.csv")
    baseline_estimation_equations_path = _resolve_artifact_companion_path(baseline_loadformat_path, "ESTIMATION_EQUATIONS.csv")
    transfer_estimation_equations_path = _resolve_artifact_companion_path(transfer_loadformat_path, "ESTIMATION_EQUATIONS.csv")

    ui_period_rows: dict[str, dict[str, object]] = {}
    for period in _DISTRIBUTION_DRIVER_GAP_PERIODS:
        default_snapshot = _snapshot_for_period(ui_by_period, period, _DISTRIBUTION_DRIVER_GAP_UI_VARIABLES)
        experiment_snapshot = _snapshot_for_period(ui_experiment_by_period, period, _DISTRIBUTION_DRIVER_GAP_UI_VARIABLES)
        ui_period_rows[period] = {
            "default_fp_r": default_snapshot,
            "retained_target_experiment": experiment_snapshot,
            "experiment_minus_default": _delta_snapshot(default_snapshot, experiment_snapshot),
        }

    ui_lub_equation_summary = _equation_summary(
        equation_input_path=_resolve_artifact_companion_path(ui_experiment_dir / "LOADFORMAT.DAT", "EQUATION_INPUT_SNAPSHOT.csv"),
        estimation_equations_path=_resolve_artifact_companion_path(ui_experiment_dir / "LOADFORMAT.DAT", "ESTIMATION_EQUATIONS.csv"),
        period="2026.1",
        target="LUB",
    )
    ui_2026q1_default_lub = ui_period_rows.get("2026.1", {}).get("default_fp_r", {}).get("LUB")
    ui_first_iteration = dict(ui_lub_equation_summary["first_iteration"])
    ui_lub_uplift_breakdown = {
        "default_2026q1_lub": ui_2026q1_default_lub,
        "retained_target_previous_value": ui_first_iteration.get("previous_value"),
        "retained_target_evaluated_value": ui_first_iteration.get("evaluated_value"),
        "carry_lift_vs_default": (
            None
            if ui_2026q1_default_lub is None or ui_first_iteration.get("previous_value") is None
            else float(ui_first_iteration["previous_value"]) - float(ui_2026q1_default_lub)
        ),
        "solve_lift_vs_default": (
            None
            if ui_2026q1_default_lub is None or ui_first_iteration.get("evaluated_value") is None
            else float(ui_first_iteration["evaluated_value"]) - float(ui_2026q1_default_lub)
        ),
        "solve_step_from_previous": ui_first_iteration.get("evaluated_minus_previous"),
    }

    transfer_period_rows: dict[str, dict[str, object]] = {}
    for period in _DISTRIBUTION_DRIVER_GAP_PERIODS:
        baseline_row = baseline_by_period.get(period, {})
        scenario_row = transfer_by_period.get(period, {})
        baseline_lag_row = baseline_by_period.get(_previous_period(period), {})
        scenario_lag_row = transfer_by_period.get(_previous_period(period), {})
        baseline_snapshot = _snapshot_for_period(baseline_by_period, period, _DISTRIBUTION_DRIVER_GAP_TRANSFER_VARIABLES)
        scenario_snapshot = _snapshot_for_period(transfer_by_period, period, _DISTRIBUTION_DRIVER_GAP_TRANSFER_VARIABLES)
        pct_delta = _pct_delta_snapshot(baseline_snapshot, scenario_snapshot)
        scenario_rydpc = scenario_snapshot.get("RYDPC")
        scenario_yd = scenario_snapshot.get("YD")
        scenario_pop = scenario_snapshot.get("POP")
        scenario_ph = scenario_snapshot.get("PH")
        if None in (scenario_rydpc, scenario_yd, scenario_pop, scenario_ph) or abs(float(scenario_pop) * float(scenario_ph)) <= 1e-12:
            rydpc_identity_resid = None
        else:
            rydpc_identity_resid = float(scenario_rydpc) - (float(scenario_yd) / (float(scenario_pop) * float(scenario_ph)))
        baseline_yd_components = _yd_component_breakdown(baseline_row)
        scenario_yd_components = _yd_component_breakdown(scenario_row)
        baseline_yd_terms = _yd_term_contributions(baseline_row)
        scenario_yd_terms = _yd_term_contributions(scenario_row)
        yd_term_deltas = _delta_snapshot(baseline_yd_terms, scenario_yd_terms)
        baseline_gdpr_components = _gdpr_component_breakdown(baseline_row)
        scenario_gdpr_components = _gdpr_component_breakdown(scenario_row)
        baseline_pcy_bridge = _pcy_growth_bridge(baseline_row, baseline_lag_row)
        scenario_pcy_bridge = _pcy_growth_bridge(scenario_row, scenario_lag_row)
        baseline_ly1_identity = _transfer_ly1_driver_identity(baseline_row, baseline_lag_row)
        scenario_ly1_identity = _transfer_ly1_driver_identity(scenario_row, scenario_lag_row)
        baseline_intg_bridge = _transfer_intg_upstream_bridge(baseline_row)
        scenario_intg_bridge = _transfer_intg_upstream_bridge(scenario_row)
        baseline_intg_identity = _transfer_intg_driver_identity(baseline_row)
        scenario_intg_identity = _transfer_intg_driver_identity(scenario_row)
        baseline_ag_identity = _transfer_ag_driver_identity(baseline_row, baseline_lag_row)
        scenario_ag_identity = _transfer_ag_driver_identity(scenario_row, scenario_lag_row)
        baseline_jf_identity = _transfer_jf_driver_identity(baseline_row, baseline_lag_row)
        scenario_jf_identity = _transfer_jf_driver_identity(scenario_row, scenario_lag_row)
        baseline_intgz_equation_summary = _maybe_equation_summary(
            equation_input_path=baseline_equation_input_path,
            estimation_equations_path=baseline_estimation_equations_path,
            period=period,
            target="INTGZ",
        )
        scenario_intgz_equation_summary = _maybe_equation_summary(
            equation_input_path=transfer_equation_input_path,
            estimation_equations_path=transfer_estimation_equations_path,
            period=period,
            target="INTGZ",
        )
        baseline_ljf1_equation_summary = _maybe_equation_summary(
            equation_input_path=baseline_equation_input_path,
            estimation_equations_path=baseline_estimation_equations_path,
            period=period,
            target="LJF1",
        )
        scenario_ljf1_equation_summary = _maybe_equation_summary(
            equation_input_path=transfer_equation_input_path,
            estimation_equations_path=transfer_estimation_equations_path,
            period=period,
            target="LJF1",
        )
        baseline_intg_trace_summary = _maybe_trace_target_summary(
            equation_input_path=baseline_equation_input_path,
            period=period,
            target="INTG",
        )
        scenario_intg_trace_summary = _maybe_trace_target_summary(
            equation_input_path=transfer_equation_input_path,
            period=period,
            target="INTG",
        )
        baseline_jf_trace_summary = _maybe_trace_target_summary(
            equation_input_path=baseline_equation_input_path,
            period=period,
            target="JF",
        )
        scenario_jf_trace_summary = _maybe_trace_target_summary(
            equation_input_path=transfer_equation_input_path,
            period=period,
            target="JF",
        )
        baseline_ths_bridge = _transfer_ths_upstream_bridge(baseline_row)
        scenario_ths_bridge = _transfer_ths_upstream_bridge(scenario_row)
        baseline_private_labor_bridge = _transfer_private_labor_bridge(baseline_row)
        scenario_private_labor_bridge = _transfer_private_labor_bridge(scenario_row)
        baseline_sg_bridge = _transfer_sg_upstream_bridge(baseline_row)
        scenario_sg_bridge = _transfer_sg_upstream_bridge(scenario_row)
        baseline_aag_bridge = _transfer_aag_upstream_bridge(baseline_row, baseline_lag_row)
        scenario_aag_bridge = _transfer_aag_upstream_bridge(scenario_row, scenario_lag_row)
        transfer_period_rows[period] = {
            "baseline": baseline_snapshot,
            "scenario": scenario_snapshot,
            "scenario_minus_baseline": _delta_snapshot(baseline_snapshot, scenario_snapshot),
            "scenario_pct_delta": pct_delta,
            "ratio_decomposition": {
                "yd_pct_delta": pct_delta.get("YD"),
                "ph_pct_delta": pct_delta.get("PH"),
                "pop_pct_delta": pct_delta.get("POP"),
                "gdpr_pct_delta": pct_delta.get("GDPR"),
                "rydpc_pct_delta": pct_delta.get("RYDPC"),
            },
            "macro_indicator_deltas": {
                "UR": _delta_snapshot(baseline_snapshot, scenario_snapshot).get("UR"),
                "PCY": _delta_snapshot(baseline_snapshot, scenario_snapshot).get("PCY"),
                "RS": _delta_snapshot(baseline_snapshot, scenario_snapshot).get("RS"),
                "GDPD": _delta_snapshot(baseline_snapshot, scenario_snapshot).get("GDPD"),
                "THG": _delta_snapshot(baseline_snapshot, scenario_snapshot).get("THG"),
                "THS": _delta_snapshot(baseline_snapshot, scenario_snapshot).get("THS"),
                "RECG": _delta_snapshot(baseline_snapshot, scenario_snapshot).get("RECG"),
                "RECS": _delta_snapshot(baseline_snapshot, scenario_snapshot).get("RECS"),
                "SGP": _delta_snapshot(baseline_snapshot, scenario_snapshot).get("SGP"),
                "SSP": _delta_snapshot(baseline_snapshot, scenario_snapshot).get("SSP"),
            },
            "yd_component_breakdown": {
                "baseline": baseline_yd_components,
                "scenario": scenario_yd_components,
                "scenario_minus_baseline": _delta_snapshot(baseline_yd_components, scenario_yd_components),
            },
            "yd_term_breakdown": {
                "baseline": baseline_yd_terms,
                "scenario": scenario_yd_terms,
                "scenario_minus_baseline": yd_term_deltas,
                "top_abs_delta_terms": _top_abs_delta_terms(yd_term_deltas),
            },
            "gdpr_component_breakdown": {
                "baseline": baseline_gdpr_components,
                "scenario": scenario_gdpr_components,
                "scenario_minus_baseline": _delta_snapshot(baseline_gdpr_components, scenario_gdpr_components),
            },
            "pcy_growth_bridge": {
                "baseline": baseline_pcy_bridge,
                "scenario": scenario_pcy_bridge,
                "scenario_minus_baseline": _delta_snapshot(baseline_pcy_bridge, scenario_pcy_bridge),
            },
            "ly1_driver_bridge": {
                "baseline": baseline_ly1_identity,
                "scenario": scenario_ly1_identity,
                "scenario_minus_baseline": _delta_snapshot(baseline_ly1_identity, scenario_ly1_identity),
                "delta_breakdown": _transfer_ly1_driver_delta_breakdown(
                    baseline_row,
                    scenario_row,
                    baseline_lag_row,
                    scenario_lag_row,
                ),
            },
            "intg_upstream_bridge": {
                "baseline": baseline_intg_bridge,
                "scenario": scenario_intg_bridge,
                "scenario_minus_baseline": _delta_snapshot(baseline_intg_bridge, scenario_intg_bridge),
            },
            "intg_driver_bridge": {
                "baseline": baseline_intg_identity,
                "scenario": scenario_intg_identity,
                "scenario_minus_baseline": _delta_snapshot(baseline_intg_identity, scenario_intg_identity),
                "delta_breakdown": _transfer_intg_driver_delta_breakdown(baseline_row, scenario_row),
            },
            "intgz_equation_comparison": {
                "baseline": baseline_intgz_equation_summary,
                "scenario": scenario_intgz_equation_summary,
                "last_iteration_compiled_reference_deltas": _compiled_reference_deltas(
                    baseline_intgz_equation_summary,
                    scenario_intgz_equation_summary,
                ),
            },
            "intg_trace_comparison": {
                "baseline": baseline_intg_trace_summary,
                "scenario": scenario_intg_trace_summary,
                "last_iteration_compiled_reference_deltas": _compiled_reference_deltas(
                    baseline_intg_trace_summary,
                    scenario_intg_trace_summary,
                ),
            },
            "ths_upstream_bridge": {
                "baseline": baseline_ths_bridge,
                "scenario": scenario_ths_bridge,
                "scenario_minus_baseline": _delta_snapshot(baseline_ths_bridge, scenario_ths_bridge),
            },
            "private_labor_upstream_bridge": {
                "baseline": baseline_private_labor_bridge,
                "scenario": scenario_private_labor_bridge,
                "scenario_minus_baseline": _delta_snapshot(baseline_private_labor_bridge, scenario_private_labor_bridge),
            },
            "jf_driver_bridge": {
                "baseline": baseline_jf_identity,
                "scenario": scenario_jf_identity,
                "scenario_minus_baseline": _delta_snapshot(baseline_jf_identity, scenario_jf_identity),
                "delta_breakdown": _transfer_jf_driver_delta_breakdown(
                    baseline_row,
                    scenario_row,
                    baseline_lag_row,
                    scenario_lag_row,
                ),
            },
            "ljf1_equation_comparison": {
                "baseline": baseline_ljf1_equation_summary,
                "scenario": scenario_ljf1_equation_summary,
                "last_iteration_compiled_reference_deltas": _compiled_reference_deltas(
                    baseline_ljf1_equation_summary,
                    scenario_ljf1_equation_summary,
                ),
            },
            "jf_trace_comparison": {
                "baseline": baseline_jf_trace_summary,
                "scenario": scenario_jf_trace_summary,
                "last_iteration_compiled_reference_deltas": _compiled_reference_deltas(
                    baseline_jf_trace_summary,
                    scenario_jf_trace_summary,
                ),
            },
            "sg_upstream_bridge": {
                "baseline": baseline_sg_bridge,
                "scenario": scenario_sg_bridge,
                "scenario_minus_baseline": _delta_snapshot(baseline_sg_bridge, scenario_sg_bridge),
                "top_abs_delta_terms": _top_abs_delta_terms(_delta_snapshot(baseline_sg_bridge, scenario_sg_bridge)),
            },
            "aag_upstream_bridge": {
                "baseline": baseline_aag_bridge,
                "scenario": scenario_aag_bridge,
                "scenario_minus_baseline": _delta_snapshot(baseline_aag_bridge, scenario_aag_bridge),
            },
            "ag_driver_bridge": {
                "baseline": baseline_ag_identity,
                "scenario": scenario_ag_identity,
                "scenario_minus_baseline": _delta_snapshot(baseline_ag_identity, scenario_ag_identity),
                "delta_breakdown": _transfer_ag_driver_delta_breakdown(
                    baseline_row,
                    scenario_row,
                    baseline_lag_row,
                    scenario_lag_row,
                ),
            },
            "rydpc_identity_residual": rydpc_identity_resid,
        }

    first_private_labor_delta = _row_value(
        transfer_period_rows.get("2026.1", {}).get("private_labor_upstream_bridge", {}).get("scenario_minus_baseline", {}),
        "private_household_labor",
    )
    last_private_labor_delta = _row_value(
        transfer_period_rows.get("2029.4", {}).get("private_labor_upstream_bridge", {}).get("scenario_minus_baseline", {}),
        "private_household_labor",
    )
    transfer_propagation = {
        variable: _series_delta_by_period(baseline_by_period, transfer_by_period, variable)
        for variable in _DISTRIBUTION_DRIVER_GAP_TRANSFER_PROPAGATION_VARIABLES
    }
    transfer_propagation_summary = {
        "rs_gap_monotone_more_negative": _all_nonincreasing([row["delta"] for row in transfer_propagation["RS"]]),
        "yd_gap_monotone_more_negative": _all_nonincreasing([row["delta"] for row in transfer_propagation["YD"]]),
        "gdpr_gap_monotone_more_negative": _all_nonincreasing([row["delta"] for row in transfer_propagation["GDPR"]]),
        "ub_gap_constant": _all_constant([row["delta"] for row in transfer_propagation["UB"]]),
        "trgh_gap_monotone_smaller": _all_nonincreasing([row["delta"] for row in transfer_propagation["TRGH"]]),
        "trsh_gap_monotone_larger": _all_nondecreasing([row["delta"] for row in transfer_propagation["TRSH"]]),
    }
    sg_propagation = _series_delta_by_period(baseline_by_period, transfer_by_period, "SG")
    aag_propagation = _series_delta_by_period(baseline_by_period, transfer_by_period, "AAG")
    intg_propagation = _series_delta_by_period(baseline_by_period, transfer_by_period, "INTG")
    jf_propagation = _series_delta_by_period(baseline_by_period, transfer_by_period, "JF")
    ljf1_propagation = _series_delta_by_period(baseline_by_period, transfer_by_period, "LJF1")
    jf_gap_worsens_over_time = bool(
        jf_propagation
        and abs(float(jf_propagation[-1]["delta"])) > 1e-12
        and _all_nonincreasing([row["delta"] for row in jf_propagation])
    )
    jf_lag_gap_smaller_than_jf_gap = bool(
        jf_propagation
        and ljf1_propagation
        and abs(float(jf_propagation[-1]["delta"])) > 1e-12
        and abs(float(ljf1_propagation[-1]["delta"])) < abs(float(jf_propagation[-1]["delta"]))
    )
    private_labor_gap_worsens_between_checkpoints = bool(
        first_private_labor_delta is not None
        and last_private_labor_delta is not None
        and abs(float(last_private_labor_delta)) > abs(float(first_private_labor_delta)) + 1e-12
    )
    transfer_dynamics_summary = {
        "intg_sg_aag_loop_signature": bool(
            _all_nondecreasing([row["delta"] for row in sg_propagation])
            and _all_nonincreasing([row["delta"] for row in aag_propagation])
            and _all_nonincreasing([row["delta"] for row in intg_propagation])
        ),
        "jf_path_looks_lag_persistent": bool(
            jf_lag_gap_smaller_than_jf_gap
            or jf_gap_worsens_over_time
            or private_labor_gap_worsens_between_checkpoints
        ),
    }

    transfer_work_dir = transfer_series_path.parent
    baseline_work_dir = baseline_series_path.parent
    baseline_scenario_path = baseline_loadformat_path.parent / "scenario.yaml"
    transfer_scenario_path = transfer_loadformat_path.parent / "scenario.yaml"
    baseline_rs_equation_summary = _equation_summary(
        equation_input_path=_resolve_artifact_companion_path(baseline_loadformat_path, "EQUATION_INPUT_SNAPSHOT.csv"),
        estimation_equations_path=_resolve_artifact_companion_path(baseline_loadformat_path, "ESTIMATION_EQUATIONS.csv"),
        period="2026.1",
        target="RS",
    )
    transfer_rs_equation_summary = _equation_summary(
        equation_input_path=_resolve_artifact_companion_path(transfer_loadformat_path, "EQUATION_INPUT_SNAPSHOT.csv"),
        estimation_equations_path=_resolve_artifact_companion_path(transfer_loadformat_path, "ESTIMATION_EQUATIONS.csv"),
        period="2026.1",
        target="RS",
    )
    rs_reference_keys = sorted(
        set(baseline_rs_equation_summary["last_iteration"]["compiled_references"].keys())
        | set(transfer_rs_equation_summary["last_iteration"]["compiled_references"].keys())
    )
    transfer_rs_reference_deltas = {
        name: (
            float(transfer_rs_equation_summary["last_iteration"]["compiled_references"].get(name, float("nan")))
            - float(baseline_rs_equation_summary["last_iteration"]["compiled_references"].get(name, float("nan")))
        )
        for name in rs_reference_keys
        if (
            name in baseline_rs_equation_summary["last_iteration"]["compiled_references"]
            and name in transfer_rs_equation_summary["last_iteration"]["compiled_references"]
        )
    }

    payload = {
        "compare_report_path": str(compare_report_path),
        "backend": backend,
        "ui_variant_id": ui_variant_id,
        "transfer_variant_id": transfer_variant_id,
        "ui_retained_target_analysis": {
            "default_series_path": str(ui_series_path),
            "retained_target_series_path": str(ui_experiment_series_path),
            "periods": ui_period_rows,
            "lub_equation_summary": ui_lub_equation_summary,
            "lub_uplift_breakdown": ui_lub_uplift_breakdown,
        },
        "transfer_income_gap_analysis": {
            "scenario_spec": {
                "variant_id": transfer_variant_id,
                "ui_factor": float(phase1_scenario_by_variant()[transfer_variant_id].ui_factor),
                "trgh_delta_q": float(phase1_scenario_by_variant()[transfer_variant_id].trgh_delta_q),
                "trsh_factor": float(phase1_scenario_by_variant()[transfer_variant_id].trsh_factor),
                "trfin_fed_share": float(phase1_scenario_by_variant()[transfer_variant_id].trfin_fed_share),
                "trfin_sl_share": float(phase1_scenario_by_variant()[transfer_variant_id].trfin_sl_share),
            },
            "baseline_series_path": str(baseline_series_path),
            "scenario_series_path": str(transfer_series_path),
            "periods": transfer_period_rows,
            "propagation_profile": transfer_propagation,
            "propagation_summary": transfer_propagation_summary,
            "sg_propagation": sg_propagation,
            "aag_propagation": aag_propagation,
            "intg_propagation": intg_propagation,
            "jf_propagation": jf_propagation,
            "ljf1_propagation": ljf1_propagation,
            "dynamics_summary": transfer_dynamics_summary,
            "rs_equation_comparison": {
                "baseline": baseline_rs_equation_summary,
                "scenario": transfer_rs_equation_summary,
                "last_iteration_compiled_reference_deltas": transfer_rs_reference_deltas,
                "last_iteration_evaluated_value_delta": (
                    float(transfer_rs_equation_summary["last_iteration"]["evaluated_value"])
                    - float(baseline_rs_equation_summary["last_iteration"]["evaluated_value"])
                ),
            },
            "scenario_input_overrides": {
                "baseline": {
                    "UIFAC": _extract_scenario_override(baseline_scenario_path, "UIFAC"),
                    "SNAPDELTAQ": _extract_scenario_override(baseline_scenario_path, "SNAPDELTAQ"),
                    "SSFAC": _extract_scenario_override(baseline_scenario_path, "SSFAC"),
                    "TFEDSHR": _extract_scenario_override(baseline_scenario_path, "TFEDSHR"),
                    "TSLSHR": _extract_scenario_override(baseline_scenario_path, "TSLSHR"),
                },
                "scenario": {
                    "UIFAC": _extract_scenario_override(transfer_scenario_path, "UIFAC"),
                    "SNAPDELTAQ": _extract_scenario_override(transfer_scenario_path, "SNAPDELTAQ"),
                    "SSFAC": _extract_scenario_override(transfer_scenario_path, "SSFAC"),
                    "TFEDSHR": _extract_scenario_override(transfer_scenario_path, "TFEDSHR"),
                    "TSLSHR": _extract_scenario_override(transfer_scenario_path, "TSLSHR"),
                },
            },
        },
        "summary": {
            "ui_retained_target_lub_moves": bool(
                any(
                    (ui_period_rows[period]["experiment_minus_default"].get("LUB") or 0.0) > 0.0
                    for period in _DISTRIBUTION_DRIVER_GAP_PERIODS
                )
            ),
            "transfer_rydpc_negative": bool(
                any(
                    (transfer_period_rows[period]["scenario_minus_baseline"].get("RYDPC") or 0.0) < 0.0
                    for period in _DISTRIBUTION_DRIVER_GAP_PERIODS
                )
            ),
        },
    }
    report_path = report_path or (paths.runtime_distribution_reports_root / "analyze_phase1_distribution_driver_gap.json")
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "ui_variant_id": ui_variant_id,
        "transfer_variant_id": transfer_variant_id,
        "ui_retained_target_lub_moves": bool(payload["summary"]["ui_retained_target_lub_moves"]),
        "transfer_rydpc_negative": bool(payload["summary"]["transfer_rydpc_negative"]),
    }


def _canonical_run_report_scenario(
    *,
    run_report_path: Path,
    variant_id: str,
) -> dict[str, Any]:
    payload = json.loads(Path(run_report_path).read_text(encoding="utf-8"))
    scenarios = payload.get("scenarios", {})
    if isinstance(scenarios, dict):
        scenario = scenarios.get(variant_id)
    else:
        scenario = next(
            (row for row in scenarios if str(dict(row).get("variant_id", "")) == str(variant_id)),
            None,
        )
    if scenario is None:
        raise KeyError(f"Run report {run_report_path} is missing scenario {variant_id}.")
    return dict(scenario)


def _canonical_scenario_companion_path(
    *,
    run_report_path: Path,
    variant_id: str,
    companion_name: str,
) -> Path:
    scenario = _canonical_run_report_scenario(run_report_path=run_report_path, variant_id=variant_id)
    loadformat_path = scenario.get("loadformat_path")
    output_dir = scenario.get("output_dir")
    if loadformat_path:
        return _resolve_artifact_companion_path(Path(str(loadformat_path)), companion_name)
    if output_dir:
        companion_path = Path(str(output_dir)) / "work" / companion_name
        if companion_path.exists():
            return companion_path
    raise FileNotFoundError(
        f"Could not resolve {companion_name} for {variant_id} from {run_report_path}."
    )


def _trace_value_or_none(raw_value: Any) -> float | None:
    text = str(raw_value).strip()
    if not text or text.upper() == "NA":
        return None
    return float(text)


def _phase_trace_summary(
    *,
    trace_path: Path,
    period: str,
    variables: tuple[str, ...] | list[str],
) -> dict[str, object]:
    rows = _load_csv_rows(trace_path)
    wanted = {str(name) for name in variables}
    filtered = [
        row
        for row in rows
        if str(row.get("period", "")) == str(period) and str(row.get("variable", "")) in wanted
    ]
    phase_order: list[str] = []
    phase_values: dict[str, dict[str, float | None]] = {}
    for row in filtered:
        phase = str(row.get("phase", ""))
        variable = str(row.get("variable", ""))
        if phase not in phase_values:
            phase_values[phase] = {}
            phase_order.append(phase)
        phase_values[phase][variable] = _trace_value_or_none(row.get("value"))

    first_phase = phase_order[0] if phase_order else None
    final_phase = "final_pre_solve" if "final_pre_solve" in phase_values else (phase_order[-1] if phase_order else None)
    first_values = phase_values.get(first_phase or "", {})
    final_values = phase_values.get(final_phase or "", {})
    deltas: dict[str, float | None] = {}
    changed_variables: list[str] = []
    for variable in sorted(wanted):
        first_value = first_values.get(variable)
        final_value = final_values.get(variable)
        if first_value is None or final_value is None:
            deltas[variable] = None
            continue
        delta = float(final_value) - float(first_value)
        deltas[variable] = delta
        if abs(delta) > 1e-12:
            changed_variables.append(variable)
    return {
        "period": str(period),
        "phase_order": phase_order,
        "first_phase": first_phase,
        "final_phase": final_phase,
        "phase_values": phase_values,
        "first_to_final_pre_solve_deltas": deltas,
        "changed_variables_before_solve": changed_variables,
    }


def _phase_delta_summary(
    *,
    phase_summary: dict[str, object],
    start_phase: str,
    end_phase: str,
    variables: tuple[str, ...] | list[str],
) -> dict[str, float | None]:
    phase_values = dict(phase_summary.get("phase_values", {}) or {})
    start_values = dict(phase_values.get(start_phase, {}) or {})
    end_values = dict(phase_values.get(end_phase, {}) or {})
    deltas: dict[str, float | None] = {}
    for variable in variables:
        start_value = start_values.get(str(variable))
        end_value = end_values.get(str(variable))
        if start_value is None or end_value is None:
            deltas[str(variable)] = None
            continue
        deltas[str(variable)] = float(end_value) - float(start_value)
    return deltas


def _top_abs_mapping_items(mapping: dict[str, float], *, limit: int = 8) -> dict[str, float]:
    ranked = sorted(mapping.items(), key=lambda item: abs(float(item[1])), reverse=True)
    return {str(key): float(value) for key, value in ranked[:limit]}


def _first_level_variant_focus(
    *,
    first_levels_payload: dict[str, Any],
    variant_id: str,
    variables: tuple[str, ...] | list[str],
) -> dict[str, object]:
    rows = {
        str(row.get("variant_id")): dict(row)
        for row in first_levels_payload.get("variants", [])
    }
    variant_row = dict(rows.get(variant_id, {}) or {})
    variable_rows = dict(variant_row.get("variables", {}) or {})
    return {
        str(variable): dict(variable_rows.get(str(variable), {}) or {})
        for variable in variables
        if str(variable) in variable_rows
    }


def _compare_first_level_focus(
    retain_focus: dict[str, object],
    exclude_focus: dict[str, object],
) -> dict[str, object]:
    comparison: dict[str, dict[str, object]] = {}
    for variable in sorted(set(retain_focus) | set(exclude_focus)):
        retain_row = dict(retain_focus.get(variable, {}) or {})
        exclude_row = dict(exclude_focus.get(variable, {}) or {})
        retain_ratio = retain_row.get("delta_ratio_abs")
        exclude_ratio = exclude_row.get("delta_ratio_abs")
        comparison[variable] = {
            "retain_delta_ratio_abs": retain_ratio,
            "exclude_delta_ratio_abs": exclude_ratio,
            "retain_delta_sign_match": retain_row.get("delta_sign_match"),
            "exclude_delta_sign_match": exclude_row.get("delta_sign_match"),
            "exclude_minus_retain_delta_ratio_abs": (
                None
                if retain_ratio is None or exclude_ratio is None
                else float(exclude_ratio) - float(retain_ratio)
            ),
        }
    return {
        "variables": comparison,
        "retain_all_signs_match": all(
            bool(dict(retain_focus.get(variable, {}) or {}).get("delta_sign_match", False))
            for variable in comparison
            if variable in retain_focus
        ),
        "exclude_all_signs_match": all(
            bool(dict(exclude_focus.get(variable, {}) or {}).get("delta_sign_match", False))
            for variable in comparison
            if variable in exclude_focus
        ),
    }


def _compare_iteration_summary(
    retain_summary: dict[str, object] | None,
    exclude_summary: dict[str, object] | None,
) -> dict[str, object] | None:
    if retain_summary is None or exclude_summary is None:
        return None

    def _numeric_delta(name: str) -> float | None:
        retain_value = retain_summary.get(name)
        exclude_value = exclude_summary.get(name)
        if retain_value is None or exclude_value is None:
            return None
        return float(exclude_value) - float(retain_value)

    def _mapping_delta(name: str) -> dict[str, float]:
        retain_mapping = dict(retain_summary.get(name, {}) or {})
        exclude_mapping = dict(exclude_summary.get(name, {}) or {})
        keys = sorted(set(retain_mapping) | set(exclude_mapping))
        return {
            str(key): float(exclude_mapping.get(key, 0.0)) - float(retain_mapping.get(key, 0.0))
            for key in keys
            if abs(float(exclude_mapping.get(key, 0.0)) - float(retain_mapping.get(key, 0.0))) > 1e-12
        }

    return {
        "iteration": retain_summary.get("iteration"),
        "identical": (
            _numeric_delta("previous_value") in (None, 0.0)
            and _numeric_delta("evaluated_structural") in (None, 0.0)
            and _numeric_delta("evaluated_value") in (None, 0.0)
            and not _mapping_delta("compiled_references")
            and not _mapping_delta("active_fsr_references")
        ),
        "exclude_minus_retain": {
            "previous_value": _numeric_delta("previous_value"),
            "evaluated_structural": _numeric_delta("evaluated_structural"),
            "evaluated_value": _numeric_delta("evaluated_value"),
            "compiled_references": _mapping_delta("compiled_references"),
            "active_fsr_references": _mapping_delta("active_fsr_references"),
        },
    }


def _compare_trace_target_branches(
    retain_summary: dict[str, object] | None,
    exclude_summary: dict[str, object] | None,
) -> dict[str, object] | None:
    if retain_summary is None or exclude_summary is None:
        return None
    return {
        "period": retain_summary.get("period"),
        "target": retain_summary.get("target"),
        "first_iteration": _compare_iteration_summary(
            dict(retain_summary.get("first_iteration", {}) or {}),
            dict(exclude_summary.get("first_iteration", {}) or {}),
        ),
        "last_iteration": _compare_iteration_summary(
            dict(retain_summary.get("last_iteration", {}) or {}),
            dict(exclude_summary.get("last_iteration", {}) or {}),
        ),
    }


def _phase_value_map(
    *,
    trace_path: Path,
    period: str,
    variables: tuple[str, ...] | list[str],
) -> dict[str, dict[str, float | None]]:
    return dict(_phase_trace_summary(trace_path=trace_path, period=period, variables=variables).get("phase_values", {}) or {})


def _phase_gap_profile(
    *,
    baseline_phase_values: dict[str, dict[str, float | None]],
    scenario_phase_values: dict[str, dict[str, float | None]],
    first_level_focus: dict[str, object],
    variables: tuple[str, ...] | list[str],
    pre_solve_phase: str,
) -> dict[str, object]:
    profile: dict[str, dict[str, object]] = {}
    for variable in variables:
        baseline_pre = dict(baseline_phase_values.get(pre_solve_phase, {}) or {}).get(str(variable))
        scenario_pre = dict(scenario_phase_values.get(pre_solve_phase, {}) or {}).get(str(variable))
        pre_solve_gap = (
            None
            if baseline_pre is None or scenario_pre is None
            else float(scenario_pre) - float(baseline_pre)
        )
        focus_row = dict(first_level_focus.get(str(variable), {}) or {})
        fpexe_row = dict(focus_row.get("fpexe", {}) or {})
        fp_r_row = dict(focus_row.get("fp-r", {}) or {})
        fpexe_delta = fpexe_row.get("delta_first")
        fp_r_delta = fp_r_row.get("delta_first")
        pre_solve_ratio = (
            None
            if pre_solve_gap is None or fpexe_delta in (None, 0.0)
            else abs(float(pre_solve_gap)) / abs(float(fpexe_delta))
        )
        solve_added_gap = (
            None
            if fp_r_delta is None or pre_solve_gap is None
            else float(fp_r_delta) - float(pre_solve_gap)
        )
        solve_added_ratio = (
            None
            if solve_added_gap is None or fpexe_delta in (None, 0.0)
            else abs(float(solve_added_gap)) / abs(float(fpexe_delta))
        )
        profile[str(variable)] = {
            "pre_solve_gap": pre_solve_gap,
            "fp_r_final_gap": fp_r_delta,
            "fpexe_final_gap": fpexe_delta,
            "pre_solve_gap_ratio_abs": pre_solve_ratio,
            "fp_r_final_gap_ratio_abs": focus_row.get("delta_ratio_abs"),
            "solve_added_gap": solve_added_gap,
            "solve_added_gap_ratio_abs": solve_added_ratio,
            "delta_sign_match": focus_row.get("delta_sign_match"),
        }
    return {
        "variables": profile,
        "variables_with_zero_pre_solve_gap": [
            variable
            for variable, row in profile.items()
            if row.get("pre_solve_gap") in (None, 0.0)
        ],
        "variables_with_gap_created_inside_solve": [
            variable
            for variable, row in profile.items()
            if row.get("pre_solve_gap") in (None, 0.0)
            and row.get("fp_r_final_gap") not in (None, 0.0)
        ],
    }


def analyze_phase1_distribution_canonical_blocker_traces(
    *,
    baseline_run_report_path: Path | None = None,
    ui_relief_run_report_path: Path | None = None,
    ui_shock_run_report_path: Path | None = None,
    transfer_medium_run_report_path: Path | None = None,
    first_levels_report_path: Path | None = None,
    ui_relief_exclude_run_report_path: Path | None = None,
    ui_shock_exclude_run_report_path: Path | None = None,
    transfer_medium_exclude_run_report_path: Path | None = None,
    ui_relief_exclude_first_levels_report_path: Path | None = None,
    ui_shock_exclude_first_levels_report_path: Path | None = None,
    transfer_medium_exclude_first_levels_report_path: Path | None = None,
    period: str = "2026.1",
    report_path: Path | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    baseline_run_report_path = baseline_run_report_path or (
        paths.runtime_distribution_reports_root / "run_phase1_distribution_block.canon-baseline-fpr.json"
    )
    ui_relief_run_report_path = ui_relief_run_report_path or (
        paths.runtime_distribution_reports_root / "run_phase1_distribution_block.canon-ui-relief-fpr-remote-zsh.json"
    )
    ui_shock_run_report_path = ui_shock_run_report_path or (
        paths.runtime_distribution_reports_root / "run_phase1_distribution_block.canon-ui-shock-fpr-remote-zsh.json"
    )
    transfer_medium_run_report_path = transfer_medium_run_report_path or (
        paths.runtime_distribution_reports_root / "run_phase1_distribution_block.canon-transfer-medium-fpr-remote-zsh.json"
    )
    first_levels_report_path = first_levels_report_path or (
        paths.runtime_distribution_reports_root / "analyze_phase1_distribution_first_levels.canonical-clean-full.json"
    )
    ui_relief_exclude_run_report_path = ui_relief_exclude_run_report_path or (
        paths.runtime_distribution_reports_root / "run_phase1_distribution_block.canon-ui-relief-fpr-exclude-from-solve-merged.json"
    )
    ui_shock_exclude_run_report_path = ui_shock_exclude_run_report_path or (
        paths.runtime_distribution_reports_root / "run_phase1_distribution_block.canon-ui-shock-fpr-exclude-from-solve-merged.json"
    )
    transfer_medium_exclude_run_report_path = transfer_medium_exclude_run_report_path or (
        paths.runtime_distribution_reports_root / "run_phase1_distribution_block.canon-transfer-medium-fpr-exclude-from-solve-with-baseline.json"
    )
    ui_relief_exclude_first_levels_report_path = ui_relief_exclude_first_levels_report_path or (
        paths.runtime_distribution_reports_root / "analyze_phase1_distribution_first_levels.canon-ui-relief-exclude-from-solve-clean.json"
    )
    ui_shock_exclude_first_levels_report_path = ui_shock_exclude_first_levels_report_path or (
        paths.runtime_distribution_reports_root / "analyze_phase1_distribution_first_levels.canon-ui-shock-exclude-from-solve-clean.json"
    )
    transfer_medium_exclude_first_levels_report_path = transfer_medium_exclude_first_levels_report_path or (
        paths.runtime_distribution_reports_root / "analyze_phase1_distribution_first_levels.canon-transfer-medium-exclude-from-solve-clean.json"
    )

    first_levels_payload = json.loads(Path(first_levels_report_path).read_text(encoding="utf-8"))
    ui_relief_exclude_first_levels_payload = json.loads(
        Path(ui_relief_exclude_first_levels_report_path).read_text(encoding="utf-8")
    )
    ui_shock_exclude_first_levels_payload = json.loads(
        Path(ui_shock_exclude_first_levels_report_path).read_text(encoding="utf-8")
    )
    transfer_medium_exclude_first_levels_payload = json.loads(
        Path(transfer_medium_exclude_first_levels_report_path).read_text(encoding="utf-8")
    )

    baseline_equation_input_path = _canonical_scenario_companion_path(
        run_report_path=baseline_run_report_path,
        variant_id="baseline-observed",
        companion_name="EQUATION_INPUT_SNAPSHOT.csv",
    )
    baseline_estimation_equations_path = _canonical_scenario_companion_path(
        run_report_path=baseline_run_report_path,
        variant_id="baseline-observed",
        companion_name="ESTIMATION_EQUATIONS.csv",
    )

    def _ui_trace_bundle(
        run_report_path: Path,
        variant_id: str,
        first_level_source: dict[str, Any],
    ) -> dict[str, object]:
        equation_input_path = _canonical_scenario_companion_path(
            run_report_path=run_report_path,
            variant_id=variant_id,
            companion_name="EQUATION_INPUT_SNAPSHOT.csv",
        )
        estimation_equations_path = _canonical_scenario_companion_path(
            run_report_path=run_report_path,
            variant_id=variant_id,
            companion_name="ESTIMATION_EQUATIONS.csv",
        )
        solve_input_trace_path = _canonical_scenario_companion_path(
            run_report_path=run_report_path,
            variant_id=variant_id,
            companion_name="SOLVE_INPUT_TRACE.csv",
        )
        exogenous_trace_path = _canonical_scenario_companion_path(
            run_report_path=run_report_path,
            variant_id=variant_id,
            companion_name="EXOGENOUS_PATH_TRACE.csv",
        )
        equation_summary = _maybe_equation_summary(
            equation_input_path=equation_input_path,
            estimation_equations_path=estimation_equations_path,
            period=period,
            target="LUB",
        )
        solve_trace_summary = _phase_trace_summary(
            trace_path=solve_input_trace_path,
            period=period,
            variables=_DISTRIBUTION_CANONICAL_TRACE_UI_VARIABLES,
        )
        exogenous_trace_summary = _phase_trace_summary(
            trace_path=exogenous_trace_path,
            period=period,
            variables=("UIFAC", "LUB", "UB"),
        )
        exogenous_post_extrapolate_to_pre_solve = _phase_delta_summary(
            phase_summary=exogenous_trace_summary,
            start_phase="post_extrapolate",
            end_phase=(
                "pre_solve_stage_1"
                if "pre_solve_stage_1" in dict(exogenous_trace_summary.get("phase_values", {}) or {})
                else "final_pre_solve"
            ),
            variables=("UIFAC", "LUB", "UB"),
        )
        return {
            "first_level_focus": _first_level_variant_focus(
                first_levels_payload=first_level_source,
                variant_id=variant_id,
                variables=_DISTRIBUTION_BOUNDARY_UI_VARIABLES,
            ),
            "lub_equation_summary": equation_summary,
            "solve_input_trace_summary": solve_trace_summary,
            "exogenous_path_trace_summary": exogenous_trace_summary,
            "post_extrapolate_to_final_pre_solve_deltas": exogenous_post_extrapolate_to_pre_solve,
            "uifac_changes_before_solve": bool(
                exogenous_post_extrapolate_to_pre_solve.get("UIFAC")
                not in (None, 0.0)
            ),
            "lub_changes_before_solve": bool(
                exogenous_post_extrapolate_to_pre_solve.get("LUB")
                not in (None, 0.0)
            ),
            "lub_equation_rows_present": bool(equation_summary is not None),
        }

    ui_relief_trace = _ui_trace_bundle(ui_relief_run_report_path, "ui-relief", first_levels_payload)
    ui_shock_trace = _ui_trace_bundle(ui_shock_run_report_path, "ui-shock", first_levels_payload)
    ui_relief_exclude_trace = _ui_trace_bundle(
        ui_relief_exclude_run_report_path,
        "ui-relief",
        ui_relief_exclude_first_levels_payload,
    )
    ui_shock_exclude_trace = _ui_trace_bundle(
        ui_shock_exclude_run_report_path,
        "ui-shock",
        ui_shock_exclude_first_levels_payload,
    )

    transfer_equation_input_path = _canonical_scenario_companion_path(
        run_report_path=transfer_medium_run_report_path,
        variant_id="transfer-composite-medium",
        companion_name="EQUATION_INPUT_SNAPSHOT.csv",
    )
    transfer_estimation_equations_path = _canonical_scenario_companion_path(
        run_report_path=transfer_medium_run_report_path,
        variant_id="transfer-composite-medium",
        companion_name="ESTIMATION_EQUATIONS.csv",
    )
    transfer_solve_input_trace_path = _canonical_scenario_companion_path(
        run_report_path=transfer_medium_run_report_path,
        variant_id="transfer-composite-medium",
        companion_name="SOLVE_INPUT_TRACE.csv",
    )
    transfer_exclude_equation_input_path = _canonical_scenario_companion_path(
        run_report_path=transfer_medium_exclude_run_report_path,
        variant_id="transfer-composite-medium",
        companion_name="EQUATION_INPUT_SNAPSHOT.csv",
    )
    transfer_exclude_estimation_equations_path = _canonical_scenario_companion_path(
        run_report_path=transfer_medium_exclude_run_report_path,
        variant_id="transfer-composite-medium",
        companion_name="ESTIMATION_EQUATIONS.csv",
    )
    transfer_exclude_solve_input_trace_path = _canonical_scenario_companion_path(
        run_report_path=transfer_medium_exclude_run_report_path,
        variant_id="transfer-composite-medium",
        companion_name="SOLVE_INPUT_TRACE.csv",
    )
    baseline_intgz_equation_summary = _maybe_equation_summary(
        equation_input_path=baseline_equation_input_path,
        estimation_equations_path=baseline_estimation_equations_path,
        period=period,
        target="INTGZ",
    )
    scenario_intgz_equation_summary = _maybe_equation_summary(
        equation_input_path=transfer_equation_input_path,
        estimation_equations_path=transfer_estimation_equations_path,
        period=period,
        target="INTGZ",
    )
    baseline_ljf1_equation_summary = _maybe_equation_summary(
        equation_input_path=baseline_equation_input_path,
        estimation_equations_path=baseline_estimation_equations_path,
        period=period,
        target="LJF1",
    )
    scenario_ljf1_equation_summary = _maybe_equation_summary(
        equation_input_path=transfer_equation_input_path,
        estimation_equations_path=transfer_estimation_equations_path,
        period=period,
        target="LJF1",
    )
    baseline_intg_trace_summary = _maybe_trace_target_summary(
        equation_input_path=baseline_equation_input_path,
        period=period,
        target="INTG",
    )
    scenario_intg_trace_summary = _maybe_trace_target_summary(
        equation_input_path=transfer_equation_input_path,
        period=period,
        target="INTG",
    )
    baseline_jf_trace_summary = _maybe_trace_target_summary(
        equation_input_path=baseline_equation_input_path,
        period=period,
        target="JF",
    )
    scenario_jf_trace_summary = _maybe_trace_target_summary(
        equation_input_path=transfer_equation_input_path,
        period=period,
        target="JF",
    )
    exclude_intgz_equation_summary = _maybe_equation_summary(
        equation_input_path=transfer_exclude_equation_input_path,
        estimation_equations_path=transfer_exclude_estimation_equations_path,
        period=period,
        target="INTGZ",
    )
    exclude_ljf1_equation_summary = _maybe_equation_summary(
        equation_input_path=transfer_exclude_equation_input_path,
        estimation_equations_path=transfer_exclude_estimation_equations_path,
        period=period,
        target="LJF1",
    )
    exclude_intg_trace_summary = _maybe_trace_target_summary(
        equation_input_path=transfer_exclude_equation_input_path,
        period=period,
        target="INTG",
    )
    exclude_jf_trace_summary = _maybe_trace_target_summary(
        equation_input_path=transfer_exclude_equation_input_path,
        period=period,
        target="JF",
    )
    transfer_solve_trace_summary = _phase_trace_summary(
        trace_path=transfer_solve_input_trace_path,
        period=period,
        variables=_DISTRIBUTION_CANONICAL_TRACE_TRANSFER_VARIABLES,
    )
    transfer_exclude_solve_trace_summary = _phase_trace_summary(
        trace_path=transfer_exclude_solve_input_trace_path,
        period=period,
        variables=_DISTRIBUTION_CANONICAL_TRACE_TRANSFER_VARIABLES,
    )

    ui_relief_branch_comparison = {
        "first_level_focus_comparison": _compare_first_level_focus(
            dict(ui_relief_trace.get("first_level_focus", {}) or {}),
            dict(ui_relief_exclude_trace.get("first_level_focus", {}) or {}),
        ),
        "lub_equation_branch_comparison": _compare_trace_target_branches(
            dict(ui_relief_trace.get("lub_equation_summary", {}) or {}),
            dict(ui_relief_exclude_trace.get("lub_equation_summary", {}) or {}),
        ),
    }
    ui_shock_branch_comparison = {
        "first_level_focus_comparison": _compare_first_level_focus(
            dict(ui_shock_trace.get("first_level_focus", {}) or {}),
            dict(ui_shock_exclude_trace.get("first_level_focus", {}) or {}),
        ),
        "lub_equation_branch_comparison": _compare_trace_target_branches(
            dict(ui_shock_trace.get("lub_equation_summary", {}) or {}),
            dict(ui_shock_exclude_trace.get("lub_equation_summary", {}) or {}),
        ),
    }
    transfer_branch_comparison = {
        "first_level_focus_comparison": _compare_first_level_focus(
            _first_level_variant_focus(
                first_levels_payload=first_levels_payload,
                variant_id="transfer-composite-medium",
                variables=("INTG", "JF", "TRLOWZ", "YD", "GDPR", "RYDPC", "RS", "PCY", "UR", "GDPD"),
            ),
            _first_level_variant_focus(
                first_levels_payload=transfer_medium_exclude_first_levels_payload,
                variant_id="transfer-composite-medium",
                variables=("INTG", "JF", "TRGH", "THG", "TRSH", "YD", "GDPR", "RYDPC", "RS", "PCY", "UR", "GDPD"),
            ),
        ),
        "intgz_equation_branch_comparison": _compare_trace_target_branches(
            scenario_intgz_equation_summary,
            exclude_intgz_equation_summary,
        ),
        "ljf1_equation_branch_comparison": _compare_trace_target_branches(
            scenario_ljf1_equation_summary,
            exclude_ljf1_equation_summary,
        ),
        "intg_trace_branch_comparison": _compare_trace_target_branches(
            scenario_intg_trace_summary,
            exclude_intg_trace_summary,
        ),
        "jf_trace_branch_comparison": _compare_trace_target_branches(
            scenario_jf_trace_summary,
            exclude_jf_trace_summary,
        ),
    }

    overall_findings = [
        (
            "ui-relief and ui-shock both carry the UIFAC shock into pre-solve staging, "
            "but LUB has no UIFAC reference in the compiled equation and stays fixed before solve."
        ),
        (
            "transfer-composite-medium enters 2026.1 with INTGZ, LJF1, INTG, and JF unchanged through final_pre_solve, "
            "so the remaining attenuation sits in solved-equation behavior rather than pre-solve staging."
        ),
        (
            "retain_reduced_eq_only versus exclude_from_solve is now settled for transfer-composite-medium: "
            "INTGZ and LJF1 equation snapshots are identical across branches, so the defect is downstream of those targets."
        ),
        (
            "exclude_from_solve is also settled for the UI blockers: it fixes ui-shock sign only by pinning LUB to the baseline log path, "
            "and it worsens ui-relief magnitude."
        ),
    ]

    payload = {
        "period": str(period),
        "first_levels_report_path": str(first_levels_report_path),
        "ui_relief_exclude_first_levels_report_path": str(ui_relief_exclude_first_levels_report_path),
        "ui_shock_exclude_first_levels_report_path": str(ui_shock_exclude_first_levels_report_path),
        "transfer_medium_exclude_first_levels_report_path": str(transfer_medium_exclude_first_levels_report_path),
        "baseline_run_report_path": str(baseline_run_report_path),
        "ui_relief_run_report_path": str(ui_relief_run_report_path),
        "ui_shock_run_report_path": str(ui_shock_run_report_path),
        "transfer_medium_run_report_path": str(transfer_medium_run_report_path),
        "ui_relief_exclude_run_report_path": str(ui_relief_exclude_run_report_path),
        "ui_shock_exclude_run_report_path": str(ui_shock_exclude_run_report_path),
        "transfer_medium_exclude_run_report_path": str(transfer_medium_exclude_run_report_path),
        "overall_findings": overall_findings,
        "ui_relief": ui_relief_trace,
        "ui_shock": ui_shock_trace,
        "ui_relief_exclude_from_solve": ui_relief_exclude_trace,
        "ui_shock_exclude_from_solve": ui_shock_exclude_trace,
        "ui_relief_branch_comparison": ui_relief_branch_comparison,
        "ui_shock_branch_comparison": ui_shock_branch_comparison,
        "transfer_composite_medium": {
            "first_level_focus": _first_level_variant_focus(
                first_levels_payload=first_levels_payload,
                variant_id="transfer-composite-medium",
                variables=("INTG", "JF", "TRLOWZ", "YD", "GDPR", "RYDPC", "RS", "PCY", "UR", "GDPD"),
            ),
            "solve_input_trace_summary": transfer_solve_trace_summary,
            "intgz_equation_summary": {
                "baseline": baseline_intgz_equation_summary,
                "scenario": scenario_intgz_equation_summary,
                "top_compiled_reference_deltas": _top_abs_mapping_items(
                    _compiled_reference_deltas(baseline_intgz_equation_summary, scenario_intgz_equation_summary)
                ),
            },
            "ljf1_equation_summary": {
                "baseline": baseline_ljf1_equation_summary,
                "scenario": scenario_ljf1_equation_summary,
                "top_compiled_reference_deltas": _top_abs_mapping_items(
                    _compiled_reference_deltas(baseline_ljf1_equation_summary, scenario_ljf1_equation_summary)
                ),
            },
            "intg_trace_summary": {
                "baseline": baseline_intg_trace_summary,
                "scenario": scenario_intg_trace_summary,
            },
            "jf_trace_summary": {
                "baseline": baseline_jf_trace_summary,
                "scenario": scenario_jf_trace_summary,
            },
        },
        "transfer_composite_medium_exclude_from_solve": {
            "first_level_focus": _first_level_variant_focus(
                first_levels_payload=transfer_medium_exclude_first_levels_payload,
                variant_id="transfer-composite-medium",
                variables=("INTG", "JF", "TRGH", "THG", "TRSH", "YD", "GDPR", "RYDPC", "RS", "PCY", "UR", "GDPD"),
            ),
            "solve_input_trace_summary": transfer_exclude_solve_trace_summary,
            "intgz_equation_summary": exclude_intgz_equation_summary,
            "ljf1_equation_summary": exclude_ljf1_equation_summary,
            "intg_trace_summary": exclude_intg_trace_summary,
            "jf_trace_summary": exclude_jf_trace_summary,
        },
        "transfer_composite_medium_branch_comparison": transfer_branch_comparison,
    }
    report_path = report_path or (
        paths.runtime_distribution_reports_root / "analyze_phase1_distribution_canonical_blocker_traces.json"
    )
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "period": str(period),
        "ui_relief_has_uifac_reference": bool(
            dict(payload["ui_relief"]["lub_equation_summary"]).get("has_uifac_reference", False)
        ),
        "ui_shock_has_uifac_reference": bool(
            dict(payload["ui_shock"]["lub_equation_summary"]).get("has_uifac_reference", False)
        ),
        "transfer_pre_solve_changed_variables": list(
            dict(payload["transfer_composite_medium"]["solve_input_trace_summary"]).get("changed_variables_before_solve", [])
        ),
        "transfer_intgz_policy_branch_identical": bool(
            dict(
                dict(payload["transfer_composite_medium_branch_comparison"]).get(
                    "intgz_equation_branch_comparison", {}
                )
            )
            .get("first_iteration", {})
            .get("identical", False)
        ),
        "transfer_ljf1_policy_branch_identical": bool(
            dict(
                dict(payload["transfer_composite_medium_branch_comparison"]).get(
                    "ljf1_equation_branch_comparison", {}
                )
            )
            .get("first_iteration", {})
            .get("identical", False)
        ),
    }


def analyze_phase1_distribution_canonical_solved_path(
    *,
    baseline_run_report_path: Path | None = None,
    ui_relief_run_report_path: Path | None = None,
    ui_shock_run_report_path: Path | None = None,
    transfer_medium_run_report_path: Path | None = None,
    first_levels_report_path: Path | None = None,
    period: str = "2026.1",
    report_path: Path | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    baseline_run_report_path = baseline_run_report_path or (
        paths.runtime_distribution_reports_root / "run_phase1_distribution_block.canon-baseline-fpr.json"
    )
    ui_relief_run_report_path = ui_relief_run_report_path or (
        paths.runtime_distribution_reports_root / "run_phase1_distribution_block.canon-ui-relief-fpr-remote-zsh.json"
    )
    ui_shock_run_report_path = ui_shock_run_report_path or (
        paths.runtime_distribution_reports_root / "run_phase1_distribution_block.canon-ui-shock-fpr-remote-zsh.json"
    )
    transfer_medium_run_report_path = transfer_medium_run_report_path or (
        paths.runtime_distribution_reports_root / "run_phase1_distribution_block.canon-transfer-medium-fpr-remote-zsh.json"
    )
    first_levels_report_path = first_levels_report_path or (
        paths.runtime_distribution_reports_root / "analyze_phase1_distribution_first_levels.canonical-clean-full.json"
    )

    first_levels_payload = json.loads(Path(first_levels_report_path).read_text(encoding="utf-8"))
    baseline_exogenous_trace_path = _canonical_scenario_companion_path(
        run_report_path=baseline_run_report_path,
        variant_id="baseline-observed",
        companion_name="EXOGENOUS_PATH_TRACE.csv",
    )
    baseline_solve_input_trace_path = _canonical_scenario_companion_path(
        run_report_path=baseline_run_report_path,
        variant_id="baseline-observed",
        companion_name="SOLVE_INPUT_TRACE.csv",
    )

    ui_trace_variables = ("LUB", "UB", "YD", "GDPR", "UR", "RS", "PCY")
    transfer_trace_variables = ("INTG", "JF", "YD", "GDPR", "RYDPC", "RS", "PCY", "UR", "GDPD")

    baseline_ui_phase_values = _phase_value_map(
        trace_path=baseline_exogenous_trace_path,
        period=period,
        variables=ui_trace_variables,
    )
    baseline_transfer_phase_values = _phase_value_map(
        trace_path=baseline_solve_input_trace_path,
        period=period,
        variables=transfer_trace_variables,
    )

    def _ui_payload(run_report_path: Path, variant_id: str) -> dict[str, object]:
        scenario_phase_values = _phase_value_map(
            trace_path=_canonical_scenario_companion_path(
                run_report_path=run_report_path,
                variant_id=variant_id,
                companion_name="EXOGENOUS_PATH_TRACE.csv",
            ),
            period=period,
            variables=ui_trace_variables,
        )
        focus = _first_level_variant_focus(
            first_levels_payload=first_levels_payload,
            variant_id=variant_id,
            variables=("LUB", "UB", "YD", "GDPR", "UR", "RS", "PCY", "TRLOWZ", "IPOVALL", "IPOVCH", "RYDPC"),
        )
        return {
            "phase_gap_profile": _phase_gap_profile(
                baseline_phase_values=baseline_ui_phase_values,
                scenario_phase_values=scenario_phase_values,
                first_level_focus=focus,
                variables=ui_trace_variables,
                pre_solve_phase="pre_solve_stage_1",
            ),
            "first_level_focus": focus,
        }

    def _transfer_payload(run_report_path: Path, variant_id: str) -> dict[str, object]:
        scenario_phase_values = _phase_value_map(
            trace_path=_canonical_scenario_companion_path(
                run_report_path=run_report_path,
                variant_id=variant_id,
                companion_name="SOLVE_INPUT_TRACE.csv",
            ),
            period=period,
            variables=transfer_trace_variables,
        )
        focus = _first_level_variant_focus(
            first_levels_payload=first_levels_payload,
            variant_id=variant_id,
            variables=transfer_trace_variables,
        )
        return {
            "phase_gap_profile": _phase_gap_profile(
                baseline_phase_values=baseline_transfer_phase_values,
                scenario_phase_values=scenario_phase_values,
                first_level_focus=focus,
                variables=transfer_trace_variables,
                pre_solve_phase="final_pre_solve",
            ),
            "first_level_focus": focus,
        }

    ui_relief_payload = _ui_payload(ui_relief_run_report_path, "ui-relief")
    ui_shock_payload = _ui_payload(ui_shock_run_report_path, "ui-shock")
    transfer_payload = _transfer_payload(transfer_medium_run_report_path, "transfer-composite-medium")

    payload = {
        "period": str(period),
        "first_levels_report_path": str(first_levels_report_path),
        "baseline_run_report_path": str(baseline_run_report_path),
        "ui_relief_run_report_path": str(ui_relief_run_report_path),
        "ui_shock_run_report_path": str(ui_shock_run_report_path),
        "transfer_medium_run_report_path": str(transfer_medium_run_report_path),
        "overall_findings": [
            (
                "For the retained UI blockers, the tracked downstream variables still have zero pre-solve baseline gaps at 2026.1 "
                "and pick up their small final response only inside solve."
            ),
            (
                "For retained transfer-composite-medium, INTG/JF/YD/GDPR/RYDPC/RS/PCY/UR/GDPD also have zero pre-solve baseline gaps at 2026.1, "
                "so the attenuation is being created entirely inside solve."
            ),
        ],
        "ui_relief": ui_relief_payload,
        "ui_shock": ui_shock_payload,
        "transfer_composite_medium": transfer_payload,
    }
    report_path = report_path or (
        paths.runtime_distribution_reports_root / "analyze_phase1_distribution_canonical_solved_path.json"
    )
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "period": str(period),
        "ui_relief_inside_solve_variables": list(
            dict(ui_relief_payload["phase_gap_profile"]).get("variables_with_gap_created_inside_solve", [])
        ),
        "ui_shock_inside_solve_variables": list(
            dict(ui_shock_payload["phase_gap_profile"]).get("variables_with_gap_created_inside_solve", [])
        ),
        "transfer_inside_solve_variables": list(
            dict(transfer_payload["phase_gap_profile"]).get("variables_with_gap_created_inside_solve", [])
        ),
    }


def compare_phase1_distribution_backends(
    *,
    fp_home: Path,
    left_backend: str = _DISTRIBUTION_DEFAULT_BACKEND,
    right_backend: str = "fpexe",
    variant_ids: tuple[str, ...] | list[str] = _DISTRIBUTION_COMPARE_VARIANT_IDS,
    variables: tuple[str, ...] | list[str] = _DISTRIBUTION_COMPARE_VARIABLES,
    report_path: Path | None = None,
    left_fpr_timeout_seconds: int | None = None,
    right_fpr_timeout_seconds: int | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    left_tag = f"cmp-{str(left_backend).replace('_', '-').replace('.', '-')}"
    right_tag = f"cmp-{str(right_backend).replace('_', '-').replace('.', '-')}"
    left_report_path = _tagged_report_path(
        paths.runtime_distribution_reports_root / "run_phase1_distribution_block.json",
        left_tag,
    )
    right_report_path = _tagged_report_path(
        paths.runtime_distribution_reports_root / "run_phase1_distribution_block.json",
        right_tag,
    )
    left_run = run_phase1_distribution_block(
        fp_home=fp_home,
        backend=left_backend,
        scenarios_root=_tagged_runtime_dir(paths.runtime_distribution_scenarios_root, left_tag),
        artifacts_root=_tagged_runtime_dir(paths.runtime_distribution_artifacts_root, left_tag),
        overlay_root=_tagged_runtime_dir(paths.runtime_distribution_overlay_root, left_tag),
        report_path=left_report_path,
        variant_ids=variant_ids,
        fpr_timeout_seconds=left_fpr_timeout_seconds if str(left_backend).strip().lower() in {"fp-r", "fp_r", "fpr"} else None,
    )
    right_run = run_phase1_distribution_block(
        fp_home=fp_home,
        backend=right_backend,
        scenarios_root=_tagged_runtime_dir(paths.runtime_distribution_scenarios_root, right_tag),
        artifacts_root=_tagged_runtime_dir(paths.runtime_distribution_artifacts_root, right_tag),
        overlay_root=_tagged_runtime_dir(paths.runtime_distribution_overlay_root, right_tag),
        report_path=right_report_path,
        variant_ids=variant_ids,
        fpr_timeout_seconds=right_fpr_timeout_seconds if str(right_backend).strip().lower() in {"fp-r", "fp_r", "fpr"} else None,
    )
    return compare_phase1_distribution_reports(
        left_report_path=Path(left_run["report_path"]),
        right_report_path=Path(right_run["report_path"]),
        left_backend=left_backend,
        right_backend=right_backend,
        variant_ids=variant_ids,
        variables=variables,
        report_path=report_path,
    )


def compare_phase1_distribution_reports(
    *,
    left_report_path: Path,
    right_report_path: Path,
    left_backend: str,
    right_backend: str,
    variant_ids: tuple[str, ...] | list[str] = _DISTRIBUTION_COMPARE_VARIANT_IDS,
    variables: tuple[str, ...] | list[str] = _DISTRIBUTION_COMPARE_VARIABLES,
    report_path: Path | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    spec_map = phase1_scenario_by_variant()
    left_payload = _load_distribution_run_report(Path(left_report_path))
    right_payload = _load_distribution_run_report(Path(right_report_path))
    comparison_rows: list[dict[str, object]] = []
    for variant_id in [str(item) for item in variant_ids]:
        spec = spec_map[variant_id]
        left_scenario = dict(left_payload["scenarios"])[variant_id]
        right_scenario = dict(right_payload["scenarios"])[variant_id]
        left_forecast_start = str(left_payload.get("scenario_forecast_start", left_payload.get("forecast_window_start", "")) or "")
        left_forecast_end = str(left_payload.get("scenario_forecast_end", left_payload.get("forecast_window_end", "")) or "")
        right_forecast_start = str(
            right_payload.get("scenario_forecast_start", right_payload.get("forecast_window_start", "")) or ""
        )
        right_forecast_end = str(
            right_payload.get("scenario_forecast_end", right_payload.get("forecast_window_end", "")) or ""
        )
        first_level_comparison: dict[str, dict[str, float | None]] = {}
        last_level_comparison: dict[str, dict[str, float | None]] = {}
        left_identity_checks = _distribution_identity_checks_for_loadformat(
            Path(str(left_scenario["loadformat_path"])),
            spec,
            forecast_start=left_forecast_start or None,
            forecast_end=left_forecast_end or None,
        )
        right_identity_checks = _distribution_identity_checks_for_loadformat(
            Path(str(right_scenario["loadformat_path"])),
            spec,
            forecast_start=right_forecast_start or None,
            forecast_end=right_forecast_end or None,
        )
        identity_comparison: dict[str, dict[str, float | None]] = {}
        max_abs_diff = 0.0
        for variable in [str(item) for item in variables]:
            left_first = _scenario_level_for_backend(
                left_scenario,
                backend=left_backend,
                variable=variable,
                level_kind="first",
            )
            right_first = _scenario_level_for_backend(
                right_scenario,
                backend=right_backend,
                variable=variable,
                level_kind="first",
            )
            left_last = _scenario_level_for_backend(
                left_scenario,
                backend=left_backend,
                variable=variable,
                level_kind="last",
            )
            right_last = _scenario_level_for_backend(
                right_scenario,
                backend=right_backend,
                variable=variable,
                level_kind="last",
            )
            first_abs = None if left_first is None or right_first is None else abs(float(left_first) - float(right_first))
            last_abs = None if left_last is None or right_last is None else abs(float(left_last) - float(right_last))
            if first_abs is not None:
                max_abs_diff = max(max_abs_diff, first_abs)
            if last_abs is not None:
                max_abs_diff = max(max_abs_diff, last_abs)
            first_level_comparison[variable] = {
                "left": left_first,
                "right": right_first,
                "abs_diff": first_abs,
            }
            last_level_comparison[variable] = {
                "left": left_last,
                "right": right_last,
                "abs_diff": last_abs,
            }
        for identity_id in sorted(set(left_identity_checks) | set(right_identity_checks)):
            left_identity = dict(left_identity_checks.get(identity_id, {}))
            right_identity = dict(right_identity_checks.get(identity_id, {}))
            identity_comparison[identity_id] = {
                "left_max_abs_residual": left_identity.get("max_abs_residual"),
                "right_max_abs_residual": right_identity.get("max_abs_residual"),
                "left_terminal_abs_residual": left_identity.get("terminal_abs_residual"),
                "right_terminal_abs_residual": right_identity.get("terminal_abs_residual"),
            }
        comparison_rows.append(
            {
                "variant_id": variant_id,
                "left_backend": left_backend,
                "right_backend": right_backend,
                "left_loadformat_path": left_scenario.get("loadformat_path"),
                "right_loadformat_path": right_scenario.get("loadformat_path"),
                "left_success": left_scenario.get("success"),
                "right_success": right_scenario.get("success"),
                "first_levels": first_level_comparison,
                "last_levels": last_level_comparison,
                "left_identity_checks": left_identity_checks,
                "right_identity_checks": right_identity_checks,
                "identity_comparison": identity_comparison,
                "max_abs_diff": max_abs_diff,
            }
        )

    def _backend_identity_max(rows: list[dict[str, object]], side: str) -> float:
        maxima: list[float] = []
        key = f"{side}_identity_checks"
        for row in rows:
            for payload in dict(row.get(key, {})).values():
                value = dict(payload).get("max_abs_residual")
                if value is not None:
                    maxima.append(float(value))
        return max(maxima, default=0.0)

    report_payload = {
        "left_backend": left_backend,
        "right_backend": right_backend,
        "variant_ids": [str(item) for item in variant_ids],
        "variables": [str(item) for item in variables],
        "left_report_path": str(left_report_path),
        "right_report_path": str(right_report_path),
        "comparisons": comparison_rows,
        "summary": {
            "max_abs_diff": max((float(item["max_abs_diff"]) for item in comparison_rows), default=0.0),
            "left_backend_max_identity_residual": _backend_identity_max(comparison_rows, "left"),
            "right_backend_max_identity_residual": _backend_identity_max(comparison_rows, "right"),
            "variant_count": len(comparison_rows),
        },
    }
    report_path = report_path or (paths.runtime_distribution_reports_root / "compare_phase1_distribution_backends.json")
    report_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "left_report_path": str(left_report_path),
        "right_report_path": str(right_report_path),
        "variant_count": len(comparison_rows),
        "max_abs_diff": report_payload["summary"]["max_abs_diff"],
    }
