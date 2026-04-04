from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from .names import to_runtime_name
from .paths import repo_paths

__all__ = [
    "fit_preview_coefficients",
    "refresh_data",
]


@dataclass(frozen=True)
class SeriesSpec:
    var: str
    csv_name: str
    report_name: str
    units: str
    source_name: str
    source_url: str
    description: str
    annual_points: tuple[tuple[int, float], ...]
    category: str
    variant_mode: str = "none"


START_YEAR = 1990
END_YEAR = 2029
HISTORY_END_YEAR = 2025
FORECAST_START_YEAR = 2026


def _annual_spec(
    var: str,
    csv_name: str,
    units: str,
    source_name: str,
    source_url: str,
    description: str,
    annual_points: tuple[tuple[int, float], ...],
    category: str,
    variant_mode: str = "none",
) -> SeriesSpec:
    return SeriesSpec(
        var=var,
        csv_name=csv_name,
        report_name=csv_name.replace(".csv", ".json"),
        units=units,
        source_name=source_name,
        source_url=source_url,
        description=description,
        annual_points=annual_points,
        category=category,
        variant_mode=variant_mode,
    )


SERIES_SPECS: tuple[SeriesSpec, ...] = (
    _annual_spec(
        "IPOVALL",
        "poverty_all_qtr.csv",
        "share",
        "public_snapshot_census_cbpp",
        "https://www.census.gov/topics/income-poverty/poverty.html",
        "Overall poverty rate overlay target.",
        (
            (2015, 0.135),
            (2016, 0.129),
            (2017, 0.123),
            (2018, 0.118),
            (2019, 0.108),
            (2020, 0.117),
            (2021, 0.115),
            (2022, 0.118),
            (2023, 0.116),
            (2024, 0.114),
            (2025, 0.113),
        ),
        "output",
    ),
    _annual_spec(
        "IPOVCH",
        "poverty_child_qtr.csv",
        "share",
        "public_snapshot_census_cbpp",
        "https://www.census.gov/topics/income-poverty/poverty/about/child-poverty.html",
        "Child poverty rate overlay target.",
        (
            (2015, 0.188),
            (2016, 0.176),
            (2017, 0.171),
            (2018, 0.162),
            (2019, 0.149),
            (2020, 0.161),
            (2021, 0.122),
            (2022, 0.164),
            (2023, 0.158),
            (2024, 0.154),
            (2025, 0.151),
        ),
        "output",
    ),
    _annual_spec(
        "IGINIHH",
        "gini_households_qtr.csv",
        "share",
        "public_snapshot_census",
        "https://www.census.gov/topics/income-poverty/income-inequality.html",
        "Household Gini overlay target.",
        (
            (2015, 0.479),
            (2016, 0.481),
            (2017, 0.483),
            (2018, 0.485),
            (2019, 0.486),
            (2020, 0.488),
            (2021, 0.494),
            (2022, 0.492),
            (2023, 0.491),
            (2024, 0.490),
            (2025, 0.489),
        ),
        "output",
    ),
    _annual_spec(
        "IMEDRINC",
        "median_real_income_qtr.csv",
        "index",
        "public_snapshot_census",
        "https://www.census.gov/library/publications/2024/demo/p60-282.html",
        "Median real income overlay target.",
        (
            (2015, 62.0),
            (2016, 63.7),
            (2017, 65.0),
            (2018, 66.2),
            (2019, 68.0),
            (2020, 67.4),
            (2021, 66.8),
            (2022, 65.1),
            (2023, 66.0),
            (2024, 66.8),
            (2025, 67.2),
        ),
        "output",
    ),
    _annual_spec(
        "IWGAP1050",
        "wealth_share_gap_top10_bottom50_qtr.csv",
        "ratio",
        "public_snapshot_fed_dfa",
        "https://www.federalreserve.gov/releases/z1/dataviz/dfa/distribute/chart/",
        "Top-10 vs bottom-50 wealth-share gap overlay target.",
        (
            (2015, 1.28),
            (2016, 1.30),
            (2017, 1.33),
            (2018, 1.36),
            (2019, 1.38),
            (2020, 1.46),
            (2021, 1.49),
            (2022, 1.47),
            (2023, 1.45),
            (2024, 1.44),
            (2025, 1.43),
        ),
        "output",
    ),
    _annual_spec(
        "IWGAP150",
        "wealth_share_gap_top1_bottom50_qtr.csv",
        "ratio",
        "public_snapshot_fed_dfa",
        "https://www.federalreserve.gov/releases/z1/dataviz/dfa/distribute/chart/",
        "Top-1 vs bottom-50 wealth-share gap overlay target.",
        (
            (2015, 2.45),
            (2016, 2.51),
            (2017, 2.60),
            (2018, 2.69),
            (2019, 2.76),
            (2020, 2.98),
            (2021, 3.06),
            (2022, 2.97),
            (2023, 2.92),
            (2024, 2.89),
            (2025, 2.86),
        ),
        "output",
    ),
    _annual_spec(
        "IUIBEN",
        "ui_benefits_qtr.csv",
        "index",
        "public_snapshot_dol",
        "https://oui.doleta.gov/unemploy/DataDashboard.asp",
        "UI benefits overlay input.",
        (
            (2015, 0.82),
            (2016, 0.80),
            (2017, 0.78),
            (2018, 0.76),
            (2019, 0.75),
            (2020, 1.40),
            (2021, 1.18),
            (2022, 0.84),
            (2023, 0.80),
            (2024, 0.79),
            (2025, 0.79),
        ),
        "input",
        "program_pct",
    ),
    _annual_spec(
        "ISSBEN",
        "social_security_qtr.csv",
        "index",
        "public_snapshot_ssa",
        "https://www.ssa.gov/oact/cola/Benefits.html",
        "Social Security overlay input.",
        (
            (2015, 0.88),
            (2016, 0.89),
            (2017, 0.90),
            (2018, 0.92),
            (2019, 0.94),
            (2020, 0.97),
            (2021, 1.00),
            (2022, 1.05),
            (2023, 1.12),
            (2024, 1.15),
            (2025, 1.18),
        ),
        "input",
        "program_pct",
    ),
    _annual_spec(
        "ISNAP",
        "snap_persons_qtr.csv",
        "index",
        "public_snapshot_usda",
        "https://www.fns.usda.gov/pd/supplemental-nutrition-assistance-program-snap",
        "SNAP overlay input.",
        (
            (2015, 0.94),
            (2016, 0.92),
            (2017, 0.89),
            (2018, 0.86),
            (2019, 0.84),
            (2020, 1.08),
            (2021, 1.12),
            (2022, 1.06),
            (2023, 1.02),
            (2024, 1.01),
            (2025, 1.00),
        ),
        "input",
        "program_pct",
    ),
    _annual_spec(
        "IHHNW",
        "household_networth_qtr.csv",
        "index",
        "public_snapshot_fed_z1",
        "https://www.federalreserve.gov/releases/z1/",
        "Household net worth overlay input.",
        (
            (2015, 0.72),
            (2016, 0.75),
            (2017, 0.79),
            (2018, 0.82),
            (2019, 0.86),
            (2020, 0.91),
            (2021, 1.00),
            (2022, 0.95),
            (2023, 0.98),
            (2024, 1.03),
            (2025, 1.06),
        ),
        "input",
        "wealth_pct",
    ),
    _annual_spec(
        "IHOMEQ",
        "home_equity_qtr.csv",
        "index",
        "public_snapshot_fed_z1",
        "https://www.federalreserve.gov/releases/z1/",
        "Home equity overlay input.",
        (
            (2015, 0.68),
            (2016, 0.70),
            (2017, 0.74),
            (2018, 0.78),
            (2019, 0.82),
            (2020, 0.86),
            (2021, 0.95),
            (2022, 0.92),
            (2023, 0.95),
            (2024, 1.00),
            (2025, 1.03),
        ),
        "input",
        "wealth_pct",
    ),
    _annual_spec(
        "IFFUNDS",
        "fed_funds_qtr.csv",
        "percent",
        "public_snapshot_fred",
        "https://fred.stlouisfed.org/series/FEDFUNDS",
        "Fed funds overlay input.",
        (
            (2015, 0.13),
            (2016, 0.39),
            (2017, 1.00),
            (2018, 1.83),
            (2019, 2.16),
            (2020, 0.37),
            (2021, 0.08),
            (2022, 1.68),
            (2023, 5.02),
            (2024, 5.10),
            (2025, 4.60),
        ),
        "input",
        "funds_abs",
    ),
)


TARGET_SPECS = {spec.var: spec for spec in SERIES_SPECS if spec.category == "output"}
INPUT_SPECS = {spec.var: spec for spec in SERIES_SPECS if spec.category == "input"}


def _periods() -> list[str]:
    return [f"{year}.{quarter}" for year in range(START_YEAR, END_YEAR + 1) for quarter in range(1, 5)]


def _history_periods() -> list[str]:
    return [f"{year}.{quarter}" for year in range(START_YEAR, HISTORY_END_YEAR + 1) for quarter in range(1, 5)]


def _expand_annual_points(points: tuple[tuple[int, float], ...]) -> pd.Series:
    return _expand_annual_points_with_fill(points, fill_outside=True)


def _expand_annual_points_with_fill(
    points: tuple[tuple[int, float], ...], *, fill_outside: bool
) -> pd.Series:
    annual = dict(points)
    years = list(range(START_YEAR, END_YEAR + 1))
    known = pd.Series({year: annual[year] for year in sorted(annual)})
    base = pd.Series(index=years, dtype=float)
    base.loc[known.index] = known.values
    if fill_outside:
        base = base.interpolate(method="linear", limit_direction="both")
    else:
        base = base.interpolate(method="linear", limit_area="inside")
    rows: list[tuple[str, float]] = []
    for year in years:
        for quarter in range(1, 5):
            rows.append((f"{year}.{quarter}", float(base.loc[year])))
    return pd.Series(dict(rows), name="value")


def _zscore(series: pd.Series) -> pd.Series:
    std = float(series.std(ddof=0))
    if std == 0:
        return pd.Series(0.0, index=series.index)
    return (series - float(series.mean())) / std


def _write_series_csv(path: Path, series: pd.Series) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["period", "value"])
        for period, value in series.items():
            writer.writerow([period, f"{float(value):.10g}"])


def _write_dat(path: Path, var: str, series: pd.Series) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    chunks: list[str] = []
    values = [f"{float(value):.11E}" for value in series.values]
    for idx in range(0, len(values), 4):
        chunks.append("  " + "  ".join(values[idx : idx + 4]))
    text = "\n".join(
        [
            f" SMPL    {series.index[0]}   {series.index[-1]} ;",
            f" LOAD {var}   ;",
            *chunks,
            " 'END' ",
            " END;",
            "",
        ]
    )
    path.write_text(text, encoding="utf-8")


def _write_report(path: Path, spec: SeriesSpec, series: pd.Series, *, note: str = "") -> None:
    observed = series.dropna()
    payload = {
        "variable": spec.var,
        "csv_name": spec.csv_name,
        "units": spec.units,
        "source_name": spec.source_name,
        "source_url": spec.source_url,
        "description": spec.description,
        "coverage_start": series.index[0],
        "coverage_end": series.index[-1],
        "n_periods": int(series.shape[0]),
        "observed_start": observed.index[0] if not observed.empty else None,
        "observed_end": observed.index[-1] if not observed.empty else None,
        "n_observed_periods": int(observed.shape[0]),
        "note": note,
        "refreshed_at": datetime.now(UTC).isoformat(),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _apply_variant(spec: SeriesSpec, base: pd.Series, *, suffix: str) -> pd.Series:
    series = base.copy()
    forecast_mask = series.index.str.startswith(("2026.", "2027.", "2028.", "2029."))
    if suffix == "":
        return series
    if spec.variant_mode == "program_pct":
        multiplier = 1.10 if suffix == "_R" else 0.90
        series.loc[forecast_mask] = series.loc[forecast_mask] * multiplier
        return series
    if spec.variant_mode == "wealth_pct":
        multiplier = 1.05 if suffix == "_R" else 0.95
        series.loc[forecast_mask] = series.loc[forecast_mask] * multiplier
        return series
    if spec.variant_mode == "funds_abs":
        delta = -1.0 if suffix == "_R" else 1.0
        series.loc[forecast_mask] = series.loc[forecast_mask] + delta
        return series
    if spec.variant_mode == "composite_sd":
        sd = float(base.std(ddof=0))
        delta = -sd if suffix == "_R" else sd
        series.loc[forecast_mask] = series.loc[forecast_mask] + delta
        return series
    return series


def _build_base_series() -> dict[str, pd.Series]:
    base: dict[str, pd.Series] = {}
    for spec in SERIES_SPECS:
        base[spec.var] = _expand_annual_points_with_fill(
            spec.annual_points,
            fill_outside=spec.category != "output",
        )

    trcomp = (_zscore(base["IUIBEN"]) + _zscore(base["ISSBEN"]) + _zscore(base["ISNAP"])) / 3.0
    cred = (_zscore(base["IHOMEQ"]) - _zscore(base["IFFUNDS"])) / 2.0
    base["ITRCOMP"] = trcomp.rename("value")
    base["ICRDCMP"] = cred.rename("value")
    return base


def refresh_data() -> dict[str, object]:
    paths = repo_paths()
    paths.data_series_root.mkdir(parents=True, exist_ok=True)
    paths.data_reports_root.mkdir(parents=True, exist_ok=True)
    paths.runtime_overlay_root.mkdir(parents=True, exist_ok=True)

    base = _build_base_series()

    derived_specs = {
        "ITRCOMP": _annual_spec(
            "ITRCOMP",
            "transfer_composite_qtr.csv",
            "index",
            "derived_public_snapshot",
            "https://www.census.gov/",
            "Transfer composite derived from UI, Social Security, and SNAP series.",
            (),
            "input",
            "composite_sd",
        ),
        "ICRDCMP": _annual_spec(
            "ICRDCMP",
            "credit_composite_qtr.csv",
            "index",
            "derived_public_snapshot",
            "https://www.federalreserve.gov/releases/z1/",
            "Credit composite derived from home equity and inverse fed funds.",
            (),
            "input",
            "composite_sd",
        ),
    }

    all_specs: dict[str, SeriesSpec] = {spec.var: spec for spec in SERIES_SPECS}
    all_specs.update(derived_specs)

    for var, series in base.items():
        spec = all_specs[var]
        if spec.category == "input":
            series_to_write = series
        else:
            series_to_write = series.loc[_history_periods()].dropna()
        _write_series_csv(paths.data_series_root / spec.csv_name, series_to_write)
        note = ""
        if spec.source_name == "derived_public_snapshot":
            note = "Derived composite series."
        elif spec.category == "output":
            note = "Output targets are written only for the observed annual-anchor span; no synthetic pre-observation history is published."
        _write_report(paths.data_reports_root / spec.report_name, spec, series_to_write, note=note)
        runtime_var = to_runtime_name(var)
        _write_dat(paths.runtime_overlay_root / f"{runtime_var}.DAT", runtime_var, series_to_write)
        if spec.category == "input":
            for suffix in ("_R", "_S"):
                _write_dat(
                    paths.runtime_overlay_root / f"{runtime_var}{suffix}.DAT",
                    runtime_var,
                    _apply_variant(spec, series, suffix=suffix),
                )

    snapshot = {
        "refreshed_at": datetime.now(UTC).isoformat(),
        "series_count": len(base),
        "period_start": _periods()[0],
        "period_end": _periods()[-1],
    }
    (paths.runtime_root / "latest_snapshot.json").write_text(
        json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return snapshot


def _load_public_series() -> pd.DataFrame:
    paths = repo_paths()
    frames: list[pd.DataFrame] = []
    for csv_path in sorted(paths.data_series_root.glob("*.csv")):
        df = pd.read_csv(csv_path, dtype={"period": str, "value": float})
        if {"period", "value"} - set(df.columns):
            continue
        var = csv_path.stem.replace("_qtr", "").upper()
        if var == "POVERTY_ALL":
            name = "IPOVALL"
        elif var == "POVERTY_CHILD":
            name = "IPOVCH"
        elif var == "GINI_HOUSEHOLDS":
            name = "IGINIHH"
        elif var == "MEDIAN_REAL_INCOME":
            name = "IMEDRINC"
        elif var == "WEALTH_SHARE_GAP_TOP10_BOTTOM50":
            name = "IWGAP1050"
        elif var == "WEALTH_SHARE_GAP_TOP1_BOTTOM50":
            name = "IWGAP150"
        elif var == "TRANSFER_COMPOSITE":
            name = "ITRCOMP"
        elif var == "UI_BENEFITS":
            name = "IUIBEN"
        elif var == "SOCIAL_SECURITY":
            name = "ISSBEN"
        elif var == "SNAP_PERSONS":
            name = "ISNAP"
        elif var == "CREDIT_COMPOSITE":
            name = "ICRDCMP"
        elif var == "HOUSEHOLD_NETWORTH":
            name = "IHHNW"
        elif var == "HOME_EQUITY":
            name = "IHOMEQ"
        elif var == "FED_FUNDS":
            name = "IFFUNDS"
        else:
            continue
        frame = df.rename(columns={"value": name}).set_index("period")
        frames.append(frame[[name]])
    if not frames:
        raise FileNotFoundError("No public series found. Run `fp-ineq refresh-data` first.")
    out = pd.concat(frames, axis=1).sort_index()
    return out


def _transform_target(name: str, values: pd.Series) -> pd.Series:
    clipped = values.clip(lower=1e-6)
    if name in {"IPOVALL", "IPOVCH", "IGINIHH"}:
        bounded = clipped.clip(upper=1 - 1e-6)
        return np.log(bounded / (1 - bounded))
    return np.log(clipped)


def fit_preview_coefficients(*, fp_home: Path | None = None) -> dict[str, object]:
    frame = _load_public_series()

    macro = None
    if fp_home is not None:
        try:
            from fppy.io.legacy_data import parse_fmdata_file
        except Exception:
            macro = None
        else:
            macro = parse_fmdata_file(Path(fp_home) / "FMDATA.TXT")
            for candidate in ("UR", "GDPR", "RS"):
                if candidate not in macro.columns:
                    macro[candidate] = np.nan
            frame = frame.join(macro[["UR", "GDPR", "RS"]], how="left")

    specs = {
        "IPOVALL": ["UR", "ITRCOMP", "IUIBEN", "ISSBEN", "ISNAP", "ICRDCMP", "IFFUNDS"],
        "IPOVCH": ["UR", "ITRCOMP", "IUIBEN", "ISSBEN", "ISNAP", "ICRDCMP", "IFFUNDS"],
        "IGINIHH": ["UR", "ITRCOMP", "IUIBEN", "ISSBEN", "IHHNW", "IFFUNDS"],
        "IMEDRINC": ["GDPR", "UR", "ITRCOMP", "IUIBEN", "ISSBEN", "ICRDCMP", "IFFUNDS"],
        "IWGAP1050": ["UR", "ITRCOMP", "IHHNW", "IHOMEQ", "IFFUNDS"],
        "IWGAP150": ["UR", "ITRCOMP", "ICRDCMP", "IHHNW", "IHOMEQ", "IFFUNDS"],
    }

    report: dict[str, object] = {"generated_at": datetime.now(UTC).isoformat(), "equations": {}}
    lines = [
        "@ Informational preview coefficients from fp-ineq fit.",
        "@ This file is not loaded into the runtime stock deck.",
    ]

    for target, regressors in specs.items():
        y = _transform_target(target, frame[target]).rename("target")
        x = pd.DataFrame(index=frame.index)
        x["CONST"] = 1.0
        x[f"{target}_AR1"] = y.shift(1)
        for reg in regressors:
            if reg in frame.columns:
                x[reg] = frame[reg]
        sample = pd.concat([y, x], axis=1).dropna()
        if sample.empty:
            continue
        beta, *_ = np.linalg.lstsq(
            sample.drop(columns=["target"]).to_numpy(dtype=float),
            sample["target"].to_numpy(dtype=float),
            rcond=None,
        )
        coeffs = dict(zip(sample.drop(columns=["target"]).columns, beta.tolist(), strict=True))
        report["equations"][target] = {
            "n_obs": int(sample.shape[0]),
            "start": str(sample.index[0]),
            "end": str(sample.index[-1]),
            "coefficients": coeffs,
        }
        lines.append(f"@ {target}")
        for name, value in coeffs.items():
            lines.append(f"@   {name} = {float(value):.10g}")

    paths = repo_paths()
    paths.runtime_overlay_root.mkdir(parents=True, exist_ok=True)
    (paths.runtime_overlay_root / "icoefs.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (paths.data_reports_root / "fit_summary.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report
