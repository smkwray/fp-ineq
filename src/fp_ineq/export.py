from __future__ import annotations

import csv
import hashlib
import json
import re
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from .bridge import ensure_fp_wraptr_importable, locate_fp_wraptr_root
from .names import to_public_name
from .paths import repo_paths
from .phase1_catalog import phase1_family_by_id, phase1_public_bundle_specs

__all__ = ["export_phase1_bridge_artifacts", "export_phase1_full_bundle", "publish_phase1_bundle_to_docs"]


_PERIOD_RE = re.compile(r"^(?P<year>\d{4})\.(?P<sub>\d+)$")
_TIMESTAMP_RE = re.compile(r"(\d{8}_\d{6})$")
_EQUATION_TOKEN_RE = re.compile(r"[A-Z][A-Z0-9_]*")
_SOLVED_EXPORT_DENYLIST = {
    "NONE9",
    "NONE19",
    "NONE20",
    "NONE21",
    "NONE22",
    "NONE25",
    "PV0",
    "PVU",
    "PVT",
    "PVUI",
    "PVGH",
    "CG0",
    "CGU",
    "CGT",
    "CGUI",
    "CGGH",
    "GN0",
    "GNU",
    "GNT",
    "MD0",
    "MDR",
    "MDU",
    "WG0",
    "WGU",
    "WGT",
    "WGA",
    "UBBAR",
    "UBSTD",
    "TRGHBAR",
    "TRGHSTD",
    "TRSHBAR",
    "TRSHSTD",
    "UIFAC",
    "TFEDSHR",
    "TSLSHR",
    "SNAPDELTAQ",
    "SSFAC",
    "CRWEDGE",
    "UIMATCH",
    "HPEQW",
}
_PHASE1_PUBLIC_SUPPRESSION = {
    "ITRCOMP",
    "IUIBEN",
    "ISSBEN",
    "ISNAP",
    "ICRDCMP",
    "IHHNW",
    "IHOMEQ",
    "IFFUNDS",
    "IWGAP1050",
    "IWGAP150",
    "LWGAP150",
    "RSAEFF",
    "RMAEFF",
    "UBZ",
    "TRGHZ",
    "TRSHZ",
    "UIDEV",
    "GHSHDV",
    "UBPOL",
    "TRGHPOL",
    "TRSHPOL",
    "TXPKGF",
    "TXPKGS",
    "PKGGROSS",
    "PKGFIN",
    "PKGNET",
    "PKGGRZ",
    "PKGNETZ",
}
_PHASE1_FULL_PRESETS = [
    {
        "id": "headline-poverty-resources",
        "label": "Headline Poverty and Resources",
        "variables": ["IPOVALL", "IPOVCH", "RYDPC", "TRLOWZ", "UR", "YD"],
    },
    {
        "id": "transfer-channels",
        "label": "Transfer Channels",
        "variables": ["UB", "TRGH", "TRSH", "YD", "POP", "PH", "GDPD"],
    },
    {
        "id": "household-resources",
        "label": "Household Resources",
        "variables": ["YD", "RYDPC", "TRLOWZ", "UB", "TRGH", "TRSH"],
    },
    {
        "id": "provisional-distribution-diagnostics",
        "label": "Provisional Distribution Diagnostics",
        "variables": ["IGINIHH", "IMEDRINC", "LGINIHH", "LMEDINC", "LRYDPC"],
    },
    {
        "id": "labor-market",
        "label": "Labor Market",
        "variables": ["U", "UR", "L1", "L2", "L3", "LM", "WF", "PF", "WR"],
    },
    {
        "id": "macro-output-demand",
        "label": "Macro Output and Demand",
        "variables": ["Y", "GDP", "GDPR", "PCY", "PIEF", "CS", "CN", "CD", "IHH"],
    },
    {
        "id": "prices-rates",
        "label": "Prices and Rates",
        "variables": ["RS", "RB", "RM", "RSA", "RMA"],
    },
    {
        "id": "fiscal-closure",
        "label": "Fiscal Closure",
        "variables": ["SG", "EXPG", "RECG", "INTG", "SR", "AS", "SGP", "SSP"],
    },
    {
        "id": "housing-balance-sheet",
        "label": "Housing and Balance Sheet",
        "variables": ["SH", "AH", "HN", "PH", "RNT"],
    },
    {
        "id": "expert-distribution-states",
        "label": "Expert Distribution States",
        "variables": ["TRLOWZ", "RYDPC", "LRYDPC", "LPOVALL", "LPOVCHGAP", "LGINIHH", "LMEDINC"],
    },
]
_PHASE1_DEFAULT_PRESET_IDS = ["headline-poverty-resources"]
_PHASE1_DEFAULT_HEADLINE_FAMILY_ID = "transfer-composite"
_PHASE1_PUBLIC_DEFAULT_RUN_IDS = (
    "ineq-baseline-observed",
    "ineq-federal-transfer-relief",
    "ineq-federal-transfer-shock",
    "ineq-state-local-transfer-relief",
    "ineq-state-local-transfer-shock",
)
_BRIDGE_EXPORT_VERSION = "v1"
_BRIDGE_EXPORT_HORIZONS = (2, 4, 8)
_BRIDGE_DOSE_METRIC = "delta_trlowz"
_BRIDGE_CHANNELS_BY_FAMILY = {
    "ui": "ui",
    "federal-transfers": "broad_federal_transfers",
    "transfer-composite": "transfer_composite",
}
_BRIDGE_HEADLINE_METRICS = ("TRLOWZ", "IPOVALL", "IPOVCH", "RYDPC")
_BRIDGE_SECONDARY_METRICS = ("IGINIHH", "IMEDRINC")
_BRIDGE_ROW_COLUMNS = [
    "bridge_version",
    "repo",
    "scenario_id",
    "scenario_label",
    "channel",
    "family",
    "h",
    "baseline_id",
    "dose_metric",
    "dose_value",
    "delta_trlowz",
    "delta_ipovall",
    "delta_ipovch",
    "delta_rydpc",
    "delta_iginihh",
    "delta_imedrinc",
    "notes",
]
_EQUATION_FUNCTION_NAMES = {"ABS", "EXP", "LOG", "MAX", "MIN"}
_FORECAST_ONLY_SERIES = ("IPOVALL", "IPOVCH", "IGINIHH", "IMEDRINC")
_FORECAST_WINDOW_NOTE = (
    "Exported series are limited to the forecast window. "
    "The integrated distribution block seeds history through 2025.4 and solves these series endogenously from 2026.1 onward."
)
_RUN_PANEL_NOTE = (
    "Default selection shows the public-default-safe fp-r results set. "
    "Published legacy-split runs remain available below with explicit shared-modern labeling."
)


def _dictionary_base_path() -> Path:
    fp_wraptr_root = locate_fp_wraptr_root()
    public_dictionary_path = fp_wraptr_root / "public" / "model-runs" / "dictionary.json"
    if public_dictionary_path.exists():
        return public_dictionary_path
    return fp_wraptr_root / "src" / "fp_wraptr" / "data" / "dictionary.json"


def _stock_dictionary_path() -> Path:
    fp_wraptr_root = locate_fp_wraptr_root()
    return fp_wraptr_root / "src" / "fp_wraptr" / "data" / "dictionary.json"


def _merge_record_fields(*records: dict[str, object]) -> dict[str, object]:
    merged: dict[str, object] = {}
    for record in records:
        for key, value in (record or {}).items():
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            if isinstance(value, list) and not value:
                continue
            if isinstance(value, dict) and not value:
                continue
            merged[key] = value
    return merged


def _normalize_run_ids(values: list[object] | None) -> list[str]:
    run_ids: list[str] = []
    for value in values or []:
        token = str(value or "").strip()
        if token and token not in run_ids:
            run_ids.append(token)
    return run_ids


def _normalize_variable_names(values: list[object] | None) -> list[str]:
    names: list[str] = []
    for value in values or []:
        token = str(value or "").strip().upper()
        if token and token not in names:
            names.append(token)
    return names


def _extract_rhs_variables(expression: str, *, lhs_variables: list[str]) -> list[str]:
    rhs_variables: list[str] = []
    for token in _EQUATION_TOKEN_RE.findall(str(expression or "").upper()):
        if token in _EQUATION_FUNCTION_NAMES or token in lhs_variables:
            continue
        if token not in rhs_variables:
            rhs_variables.append(token)
    return rhs_variables


def _parse_formula_equation_fields(
    formula: str,
    *,
    lhs_expr: str = "",
    lhs_variables: list[str] | None = None,
) -> tuple[str, list[str], list[str]]:
    normalized_lhs_variables = _normalize_variable_names(lhs_variables)
    normalized_lhs_expr = str(lhs_expr or "").strip().upper()
    rhs_segments: list[str] = []
    statements = [part.strip() for part in str(formula or "").split(";") if part.strip()]

    if statements and any("=" in statement for statement in statements):
        for statement in statements:
            if "=" not in statement:
                continue
            lhs_text, rhs_text = statement.split("=", 1)
            lhs_name = str(lhs_text or "").strip().upper()
            if lhs_name and lhs_name not in normalized_lhs_variables:
                normalized_lhs_variables.append(lhs_name)
            rhs_segments.append(rhs_text)
        if not normalized_lhs_expr and normalized_lhs_variables:
            normalized_lhs_expr = ", ".join(normalized_lhs_variables)
    else:
        if not normalized_lhs_variables and normalized_lhs_expr:
            normalized_lhs_variables = [normalized_lhs_expr]
        rhs_segments.append(str(formula or ""))

    rhs_variables: list[str] = []
    for segment in rhs_segments:
        for token in _extract_rhs_variables(segment, lhs_variables=normalized_lhs_variables):
            if token not in rhs_variables:
                rhs_variables.append(token)

    return normalized_lhs_expr, normalized_lhs_variables, rhs_variables


def _default_equation_type(eq_id: str) -> str:
    if eq_id.startswith("genr:"):
        return "genr"
    if eq_id.startswith("identity:"):
        return "identity"
    if eq_id.isdigit():
        return "equation"
    return "identity"


def _default_equation_label(eq_id: str, *, lhs_expr: str, equation_type: str) -> str:
    if equation_type == "genr" and lhs_expr:
        return f"GENR {lhs_expr}"
    if equation_type == "identity" and lhs_expr:
        return f"IDENT {lhs_expr}"
    if eq_id.isdigit():
        return f"Eq {eq_id}"
    return f"Equation {eq_id}"


def _normalize_equation_record(
    eq_id: str,
    record: dict[str, object],
    *,
    source_runs: list[str] | None = None,
) -> dict[str, object]:
    normalized_id = str(record.get("id", eq_id) or eq_id).strip()
    equation_type = str(record.get("type", "") or _default_equation_type(normalized_id)).strip()
    formula = " ".join(str(record.get("formula", record.get("rhs", "")) or "").split()).strip()
    raw_lhs_expr = str(record.get("lhs_expr", record.get("lhs", "")) or "").strip().upper()
    lhs_expr, lhs_variables, rhs_variables = _parse_formula_equation_fields(
        formula,
        lhs_expr=raw_lhs_expr,
        lhs_variables=record.get("lhs_variables"),  # type: ignore[arg-type]
    )
    if not lhs_expr and lhs_variables:
        lhs_expr = ", ".join(lhs_variables)

    return {
        **record,
        "id": normalized_id,
        "display_id": str(record.get("display_id", "") or (f"Eq {normalized_id}" if normalized_id.isdigit() else normalized_id)),
        "type": equation_type,
        "label": str(record.get("label", "") or _default_equation_label(normalized_id, lhs_expr=lhs_expr, equation_type=equation_type)),
        "formula": formula,
        "lhs_expr": lhs_expr,
        "lhs_variables": lhs_variables,
        "rhs_variables": rhs_variables if not record.get("rhs_variables") else _normalize_variable_names(record.get("rhs_variables")),  # type: ignore[arg-type]
        "source_runs": _normalize_run_ids(source_runs if source_runs is not None else record.get("source_runs")),  # type: ignore[arg-type]
    }


def _equation_sort_key(eq_id: str) -> tuple[int, int | str]:
    token = str(eq_id).strip()
    if token.isdigit():
        return (0, int(token))
    return (1, token)


def _bundle_equation_id(kind: str, name: str, raw: str) -> str:
    digest = hashlib.sha1(str(raw).encode("utf-8")).hexdigest()[:10]
    return f"{kind}:{name}:{digest}"


def _equation_ref_value(eq_id: str) -> int | str:
    token = str(eq_id).strip()
    return int(token) if token.isdigit() else token


def _equation_lhs_names(record: dict[str, object]) -> list[str]:
    names: list[str] = []
    lhs_expr = str(record.get("lhs_expr", "") or "").strip().upper()
    if lhs_expr:
        names.append(lhs_expr)
    for value in record.get("lhs_variables", []) or []:
        token = str(value or "").strip().upper()
        if token and token not in names:
            names.append(token)
    return names


def _equation_rhs_names(record: dict[str, object]) -> list[str]:
    names: list[str] = []
    for value in record.get("rhs_variables", []) or []:
        token = str(value or "").strip().upper()
        if token and token not in names:
            names.append(token)
    return names


def _synthesize_variable_description(code: str, equation: dict[str, object] | None) -> str:
    if not equation:
        return ""
    display = str(
        equation.get("display_id")
        or equation.get("label")
        or equation.get("id")
        or f"equation for {code}"
    ).strip()
    formula = " ".join(str(equation.get("formula", "") or "").split()).strip()
    if formula:
        return f"Model series defined by {display}: {formula}"
    return f"Model series defined by {display}."


def _fallback_variable_description(code: str, record: dict[str, object]) -> str:
    construction = " ".join(str(record.get("construction", "") or "").split()).strip()
    category = str(record.get("category", "") or "").strip().lower()
    if construction:
        return f"Model variable constructed as {construction}."
    if category in {"policy", "control", "exogenous"}:
        return "Model control or exogenous input exposed through the public dictionary."
    if code in {"ABS", "LOG", "ΔLOG"}:
        return "Equation helper token exposed through the public equation catalog."
    return "Model variable exposed through the public dictionary."


def _preferred_defining_equation(defining_equations: list[str]) -> str | None:
    if not defining_equations:
        return None
    numeric_ids = [eq_id for eq_id in defining_equations if str(eq_id).isdigit()]
    if numeric_ids:
        return sorted(numeric_ids, key=_equation_sort_key)[0]
    return sorted(defining_equations, key=_equation_sort_key)[0]


def _enrich_dictionary_payload(payload: dict[str, object]) -> dict[str, object]:
    variables = {
        str(name).strip().upper(): dict(record or {})
        for name, record in dict(payload.get("variables", {})).items()
    }
    equations = {
        str(eq_id).strip(): _normalize_equation_record(str(eq_id).strip(), dict(record or {}))
        for eq_id, record in dict(payload.get("equations", {})).items()
    }

    lhs_to_equations: dict[str, list[str]] = {}
    rhs_to_equations: dict[str, list[str]] = {}
    for eq_id, record in equations.items():
        normalized_id = str(record.get("id", eq_id) or eq_id).strip()
        record["id"] = normalized_id
        for name in _equation_lhs_names(record):
            lhs_to_equations.setdefault(name, []).append(normalized_id)
            variables.setdefault(name, {"name": name})
        for name in _equation_rhs_names(record):
            rhs_to_equations.setdefault(name, []).append(normalized_id)
            variables.setdefault(name, {"name": name})

    for code, record in variables.items():
        record["name"] = code
        record.setdefault("code", code)
        short_name = str(record.get("short_name", "") or "").strip()
        if not short_name:
            record["short_name"] = code

        defined_by = str(record.get("defined_by_equation", "") or "").strip()
        if defined_by not in equations:
            defined_by = ""
        if not defined_by:
            defined_by = _preferred_defining_equation(lhs_to_equations.get(code, [])) or ""
        record["defined_by_equation"] = _equation_ref_value(defined_by) if defined_by else None
        record["used_in_equations"] = [
            _equation_ref_value(eq_id)
            for eq_id in sorted(rhs_to_equations.get(code, []), key=_equation_sort_key)
        ]

        description = str(record.get("description", "") or "").strip()
        if not description:
            eq_id = str(record.get("defined_by_equation", "") or "").strip()
            equation = equations.get(eq_id) if eq_id else None
            generated_description = _synthesize_variable_description(code, equation)
            if generated_description:
                record["description"] = generated_description
        if not str(record.get("description", "") or "").strip():
            record["description"] = _fallback_variable_description(code, record)

    payload["variables"] = variables
    payload["equations"] = equations
    return payload


def _safe_dictionary_payload() -> dict[str, object]:
    paths = repo_paths()
    overlay_path = paths.overlay_source_root / "dictionary_overlays" / "ineq.json"
    fallback_dictionary_path = paths.docs_root / "dictionary.json"
    try:
        stock_dictionary_path = _stock_dictionary_path()
        base_dictionary_path = _dictionary_base_path()
        stock_payload = json.loads(stock_dictionary_path.read_text(encoding="utf-8"))
        base_payload = json.loads(base_dictionary_path.read_text(encoding="utf-8"))
        overlay_payload = json.loads(overlay_path.read_text(encoding="utf-8"))
        merged_variables = {
            name: _merge_record_fields(
                stock_payload.get("variables", {}).get(name, {}) or {},
                base_payload.get("variables", {}).get(name, {}) or {},
                overlay_payload.get("variables", {}).get(name, {}) or {},
            )
            for name in set(stock_payload.get("variables", {}))
            | set(base_payload.get("variables", {}))
            | set(overlay_payload.get("variables", {}))
        }
        merged_equations = {
            str(eq_id): _merge_record_fields(
                stock_payload.get("equations", {}).get(str(eq_id), {}) or {},
                base_payload.get("equations", {}).get(str(eq_id), {}) or {},
                overlay_payload.get("equations", {}).get(str(eq_id), {}) or {},
            )
            for eq_id in set(stock_payload.get("equations", {}))
            | set(base_payload.get("equations", {}))
            | set(overlay_payload.get("equations", {}))
        }
        payload = {
            "schema_version": 1,
            "variables": merged_variables,
            "equations": merged_equations,
        }
    except FileNotFoundError:
        if not fallback_dictionary_path.exists():
            raise
        payload = json.loads(fallback_dictionary_path.read_text(encoding="utf-8"))
    return _enrich_dictionary_payload(payload)


def _bundle_input_equations(bundle_run_ids: list[str]) -> dict[str, dict[str, object]]:
    paths = repo_paths()
    input_path = paths.runtime_distribution_overlay_root / "fminput.txt"
    if not input_path.exists():
        return {}

    ensure_fp_wraptr_importable()
    from fp_wraptr.io.input_parser import parse_fp_input

    parsed = parse_fp_input(input_path)
    equations: dict[str, dict[str, object]] = {}

    for kind, parsed_key in (("genr", "generated_vars"), ("identity", "identities")):
        for item in parsed.get(parsed_key, []) or []:
            name = str(item.get("name", "") or "").strip().upper()
            formula = " ".join(str(item.get("expression", item.get("value", "")) or "").split()).strip()
            raw = str(item.get("raw", "") or f"{name}={formula}").strip()
            if not name or not formula:
                continue
            eq_id = _bundle_equation_id(kind, name, raw)
            equations[eq_id] = _normalize_equation_record(
                eq_id,
                {
                    "display_id": f"{kind.upper()} {name}",
                    "label": f"{kind.upper()} {name}",
                    "type": kind,
                    "lhs_expr": name,
                    "lhs_variables": [name],
                    "formula": formula,
                },
                source_runs=bundle_run_ids,
            )
    return equations


def _period_key(token: str) -> tuple[int, int]:
    match = _PERIOD_RE.match(str(token).strip())
    if not match:
        raise ValueError(f"Invalid period token: {token!r}")
    return int(match.group("year")), int(match.group("sub"))


def _periods_in_window(periods: list[str], *, start: str, end: str) -> list[str]:
    start_key = _period_key(start)
    end_key = _period_key(end)
    return [token for token in periods if start_key <= _period_key(token) <= end_key]


def _phase1_solved_dictionary(
    available_variables: list[str],
    *,
    bundle_run_ids: list[str],
    variable_run_ids: dict[str, list[str]],
) -> dict[str, object]:
    payload = _safe_dictionary_payload()
    base_variables = dict(payload.get("variables", {}))
    base_equations = dict(payload.get("equations", {}))
    numeric_equations = {
        str(eq_id): _normalize_equation_record(str(eq_id), dict(record or {}), source_runs=bundle_run_ids)
        for eq_id, record in base_equations.items()
        if str(eq_id).isdigit()
    }
    supporting_equations = {
        str(eq_id): _normalize_equation_record(str(eq_id), dict(record or {}), source_runs=bundle_run_ids)
        for eq_id, record in base_equations.items()
        if not str(eq_id).isdigit()
    }
    numeric_lhs_names = {
        lhs_name
        for record in numeric_equations.values()
        for lhs_name in _equation_lhs_names(record)
    }
    bundle_equations = {
        eq_id: record
        for eq_id, record in _bundle_input_equations(bundle_run_ids).items()
        if not any(lhs_name in numeric_lhs_names for lhs_name in _equation_lhs_names(record))
    }
    merged_payload = _enrich_dictionary_payload(
        {
            "schema_version": 1,
            "variables": base_variables,
            "equations": {**numeric_equations, **supporting_equations, **bundle_equations},
        }
    )
    merged_variables = dict(merged_payload.get("variables", {}))
    for name in available_variables:
        if name in merged_variables:
            continue
        merged_variables[name] = {
            "name": name,
            "code": name,
            "short_name": name,
            "description": "Solved phase-1 stock Fair series exposed through the explorer export.",
            "units": "level",
            "category": "model",
            "defined_by_equation": None,
            "used_in_equations": [],
        }
    for name, record in merged_variables.items():
        record["source_runs"] = variable_run_ids.get(name, [])
    merged_payload["variables"] = {
        name: record
        for name, record in merged_variables.items()
        if name not in _PHASE1_PUBLIC_SUPPRESSION
    }
    return merged_payload


def _default_manifest_run_ids(manifest_runs: list[dict[str, object]]) -> list[str]:
    available_run_ids = {
        str(item.get("run_id", "") or "").strip()
        for item in manifest_runs
        if str(item.get("run_id", "") or "").strip()
    }
    preferred_defaults = [
        run_id for run_id in _PHASE1_PUBLIC_DEFAULT_RUN_IDS if run_id in available_run_ids
    ]
    if preferred_defaults:
        return preferred_defaults

    runs_by_family: dict[str, list[str]] = {}
    for item in manifest_runs:
        family_id = str(item.get("family_id", "") or "").strip()
        run_id = str(item.get("run_id", "") or "").strip()
        if family_id and run_id:
            runs_by_family.setdefault(family_id, []).append(run_id)

    baseline_run_ids = runs_by_family.get("baseline", [])
    headline_family_id = ""
    if _PHASE1_DEFAULT_HEADLINE_FAMILY_ID in runs_by_family:
        headline_family_id = _PHASE1_DEFAULT_HEADLINE_FAMILY_ID
    else:
        for item in manifest_runs:
            family_id = str(item.get("family_id", "") or "").strip()
            if family_id and family_id != "baseline":
                headline_family_id = family_id
                break

    selected: list[str] = []
    for run_id in baseline_run_ids:
        if run_id not in selected:
            selected.append(run_id)
    for run_id in runs_by_family.get(headline_family_id, []):
        if run_id not in selected:
            selected.append(run_id)
    if selected:
        return selected
    return [str(item["run_id"]) for item in manifest_runs]


def _copy_static_shell(out_dir: Path) -> None:
    paths = repo_paths()
    out_dir.mkdir(parents=True, exist_ok=True)
    for file_name in (".nojekyll", "index.html", "app.js", "styles.css"):
        source = paths.docs_root / file_name
        if source.exists():
            shutil.copy2(source, out_dir / file_name)
    assets_source = paths.docs_root / "assets"
    assets_target = out_dir / "assets"
    if assets_source.exists():
        if assets_target.exists():
            shutil.rmtree(assets_target)
        shutil.copytree(assets_source, assets_target)


def publish_phase1_bundle_to_docs(
    *,
    source_dir: Path | None = None,
    docs_dir: Path | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    source_dir = source_dir or paths.runtime_solved_public_root
    docs_dir = docs_dir or paths.docs_root
    if not source_dir.exists():
        raise FileNotFoundError(f"Phase-1 bundle directory not found: {source_dir}")

    docs_dir.mkdir(parents=True, exist_ok=True)
    file_names = [
        ".nojekyll",
        "index.html",
        "app.js",
        "styles.css",
        "manifest.json",
        "presets.json",
        "dictionary.json",
        "bridge_results.csv",
        "bridge_metadata.json",
    ]
    dir_names = ["assets", "runs"]

    for file_name in file_names:
        source = source_dir / file_name
        if source.exists():
            shutil.copy2(source, docs_dir / file_name)

    for dir_name in dir_names:
        source = source_dir / dir_name
        target = docs_dir / dir_name
        if not source.exists():
            continue
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(source, target)

    stale_export_report = docs_dir / "export_report.json"
    if stale_export_report.exists():
        stale_export_report.unlink()

    manifest = json.loads((source_dir / "manifest.json").read_text(encoding="utf-8"))
    (docs_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "docs_dir": str(docs_dir),
        "source_dir": str(source_dir),
        "run_count": len(manifest.get("runs", [])),
        "variable_count": len(manifest.get("available_variables", [])),
    }


def _artifact_timestamp(output_dir: str) -> str:
    token = Path(output_dir).name
    match = _TIMESTAMP_RE.search(token)
    if match:
        return match.group(1)
    return ""


def _run_id_for_variant(variant_id: str) -> str:
    return f"ineq-{variant_id}"


def _public_scenario_name(variant_id: str) -> str:
    return f"ineq_distribution_{variant_id.replace('-', '_')}"


def _phase1_solved_runs() -> list[dict[str, str]]:
    families = phase1_family_by_id()
    return [
        {
            "variant_id": spec.variant_id,
            "run_id": _run_id_for_variant(spec.variant_id),
            "label": spec.label,
            "summary": spec.summary,
            "group": spec.group,
            "family_id": spec.family_id,
            "family_label": families[spec.family_id].label,
            "family_maturity": families[spec.family_id].maturity,
        }
        for spec in phase1_public_bundle_specs()
    ]


def _phase1_solved_spec_by_variant() -> dict[str, dict[str, str]]:
    return {str(item["variant_id"]): dict(item) for item in _phase1_solved_runs()}


def _phase1_manifest_families(run_specs: list[dict[str, str]]) -> list[dict[str, object]]:
    families_by_id = phase1_family_by_id()
    run_ids_by_family: dict[str, list[str]] = {}
    for spec in run_specs:
        family_id = str(spec["family_id"])
        run_ids_by_family.setdefault(family_id, []).append(str(spec["run_id"]))

    manifest_families: list[dict[str, object]] = []
    for family_id, run_ids in run_ids_by_family.items():
        family = families_by_id[family_id]
        manifest_families.append(
            {
                "family_id": family.family_id,
                "label": family.label,
                "summary": family.summary,
                "maturity": family.maturity,
                "run_ids": run_ids,
            }
        )
    return manifest_families


def _visible_export_series(series: dict[str, list[float]]) -> list[str]:
    visible: list[str] = []
    seen_public_names: set[str] = set()
    for name, values in series.items():
        public_name = to_public_name(name)
        if (
            name in _SOLVED_EXPORT_DENYLIST
            or public_name in _SOLVED_EXPORT_DENYLIST
            or name in _PHASE1_PUBLIC_SUPPRESSION
            or public_name in _PHASE1_PUBLIC_SUPPRESSION
        ):
            continue
        if not values or public_name in seen_public_names:
            continue
        seen_public_names.add(public_name)
        visible.append(name)
    return visible


def _loadformat_window(
    loadformat_path: Path,
    *,
    variables: list[str] | None,
    forecast_start: str,
    forecast_end: str,
) -> tuple[list[str], dict[str, list[float]]]:
    ensure_fp_wraptr_importable()
    from fp_wraptr.io.loadformat import add_derived_series, read_loadformat

    periods, series = read_loadformat(loadformat_path)
    series = add_derived_series(series)
    window_periods = _periods_in_window(list(periods), start=forecast_start, end=forecast_end)
    if not window_periods:
        raise ValueError(f"No periods found in {loadformat_path} for window {forecast_start}..{forecast_end}")
    index_by_period = {period: idx for idx, period in enumerate(periods)}
    window_series: dict[str, list[float]] = {}
    selected_variables = variables if variables is not None else _visible_export_series(series)
    for name in selected_variables:
        values = series.get(name)
        if not values:
            continue
        public_name = to_public_name(name)
        if public_name in window_series:
            continue
        window_series[public_name] = [float(values[index_by_period[period]]) for period in window_periods]
    return window_periods, window_series


def _filter_public_series(series: dict[str, list[float]]) -> dict[str, list[float]]:
    return {
        name: values
        for name, values in series.items()
        if name not in _PHASE1_PUBLIC_SUPPRESSION and values
    }


def _advance_quarter_period(period: str, quarters_ahead: int) -> str:
    year, sub = _period_key(period)
    offset = (year * 4) + (sub - 1) + quarters_ahead
    return f"{offset // 4}.{(offset % 4) + 1}"


def _bridge_notes_for_family(family_id: str) -> str:
    notes = ["secondary_metrics_provisional"]
    if family_id == "transfer-composite":
        notes.append("financed_transfer_package")
    return "; ".join(notes)


def _bridge_metric_value(run_json: dict[str, object], metric: str, period: str) -> float:
    periods = list(run_json.get("periods", []))
    series = dict(run_json.get("series", {}))
    if period not in periods:
        raise ValueError(f"Bridge period {period} missing from run {run_json.get('run_id', '<unknown>')}")
    if metric not in series:
        raise ValueError(f"Bridge metric {metric} missing from run {run_json.get('run_id', '<unknown>')}")
    index = periods.index(period)
    values = list(series[metric])
    return float(values[index])


def _build_bridge_rows(
    *,
    manifest_runs: list[dict[str, object]],
    run_payloads: dict[str, dict[str, object]],
    forecast_start: str,
) -> list[dict[str, object]]:
    baseline_run = next((item for item in manifest_runs if str(item.get("family_id")) == "baseline"), None)
    if baseline_run is None:
        return []
    baseline_id = str(baseline_run["run_id"])
    baseline_payload = run_payloads.get(baseline_id)
    if baseline_payload is None:
        raise ValueError(f"Bridge baseline payload missing for {baseline_id}")

    bridge_rows: list[dict[str, object]] = []
    metrics = (*_BRIDGE_HEADLINE_METRICS, *_BRIDGE_SECONDARY_METRICS)
    for run in manifest_runs:
        family_id = str(run.get("family_id", ""))
        channel = _BRIDGE_CHANNELS_BY_FAMILY.get(family_id)
        if channel is None:
            continue
        run_id = str(run["run_id"])
        run_payload = run_payloads.get(run_id)
        if run_payload is None:
            raise ValueError(f"Bridge payload missing for {run_id}")
        for horizon in _BRIDGE_EXPORT_HORIZONS:
            period = _advance_quarter_period(forecast_start, horizon)
            deltas = {
                metric: _bridge_metric_value(run_payload, metric, period)
                - _bridge_metric_value(baseline_payload, metric, period)
                for metric in metrics
            }
            bridge_rows.append(
                {
                    "bridge_version": _BRIDGE_EXPORT_VERSION,
                    "repo": "fp",
                    "scenario_id": run_id,
                    "scenario_label": str(run.get("label", run_id)),
                    "channel": channel,
                    "family": family_id,
                    "h": horizon,
                    "baseline_id": baseline_id,
                    "dose_metric": _BRIDGE_DOSE_METRIC,
                    "dose_value": deltas["TRLOWZ"],
                    "delta_trlowz": deltas["TRLOWZ"],
                    "delta_ipovall": deltas["IPOVALL"],
                    "delta_ipovch": deltas["IPOVCH"],
                    "delta_rydpc": deltas["RYDPC"],
                    "delta_iginihh": deltas["IGINIHH"],
                    "delta_imedrinc": deltas["IMEDRINC"],
                    "notes": _bridge_notes_for_family(family_id),
                }
            )
    return bridge_rows


def _write_bridge_export(
    *,
    out_dir: Path,
    manifest_runs: list[dict[str, object]],
    run_payloads: dict[str, dict[str, object]],
    forecast_start: str,
) -> tuple[str, str, int]:
    rows = _build_bridge_rows(
        manifest_runs=manifest_runs,
        run_payloads=run_payloads,
        forecast_start=forecast_start,
    )
    bridge_path = out_dir / "bridge_results.csv"
    with bridge_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=_BRIDGE_ROW_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    metadata = {
        "bridge_version": _BRIDGE_EXPORT_VERSION,
        "repo": "fp",
        "schema_columns": list(_BRIDGE_ROW_COLUMNS),
        "channels_by_family": dict(_BRIDGE_CHANNELS_BY_FAMILY),
        "horizons": list(_BRIDGE_EXPORT_HORIZONS),
        "horizon_rule": "quarters_ahead_from_forecast_start",
        "dose_metric": _BRIDGE_DOSE_METRIC,
        "headline_metrics": list(_BRIDGE_HEADLINE_METRICS),
        "secondary_metrics": list(_BRIDGE_SECONDARY_METRICS),
        "row_count": len(rows),
    }
    metadata_path = out_dir / "bridge_metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return bridge_path.name, metadata_path.name, len(rows)


def _phase1_export_inputs(
    *,
    report_path: Path,
    forecast_start: str,
    forecast_end: str,
    family_maturities: tuple[str, ...],
    family_ids: tuple[str, ...] | None,
) -> dict[str, object]:
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    scenarios = dict(payload.get("scenarios", {}))
    selected_specs = phase1_public_bundle_specs(
        family_maturities=family_maturities,
        family_ids=family_ids,
    )
    if not selected_specs:
        raise ValueError("No public phase-1 scenarios matched the requested family filters")
    run_specs = {
        str(spec.variant_id): {
            "variant_id": spec.variant_id,
            "run_id": _run_id_for_variant(spec.variant_id),
            "label": spec.label,
            "summary": spec.summary,
            "group": spec.group,
            "family_id": spec.family_id,
            "family_label": phase1_family_by_id()[spec.family_id].label,
            "family_maturity": phase1_family_by_id()[spec.family_id].maturity,
        }
        for spec in selected_specs
    }
    required_variants = [spec.variant_id for spec in selected_specs]
    missing_variants = [variant_id for variant_id in required_variants if variant_id not in scenarios]
    if missing_variants:
        raise ValueError(f"Phase-1 solved report is missing scenarios: {', '.join(missing_variants)}")

    manifest_runs: list[dict[str, object]] = []
    run_payloads: dict[str, dict[str, object]] = {}
    available_variables: list[str] = []
    seen_variables: set[str] = set()
    variable_run_ids: dict[str, list[str]] = {}

    for variant_id in required_variants:
        scenario_payload = scenarios[variant_id]
        loadformat_path_raw = scenario_payload.get("loadformat_path")
        if not loadformat_path_raw:
            raise ValueError(f"Scenario {variant_id} has no loadformat_path in {report_path}")
        loadformat_path = Path(loadformat_path_raw)
        periods, public_series = _loadformat_window(
            loadformat_path,
            variables=None,
            forecast_start=forecast_start,
            forecast_end=forecast_end,
        )
        public_series = _filter_public_series(public_series)
        for name in public_series:
            if name not in seen_variables:
                seen_variables.add(name)
                available_variables.append(name)

        run_spec = run_specs[variant_id]
        run_id = str(run_spec["run_id"])
        for name in public_series:
            variable_run_ids.setdefault(name, [])
            if run_id not in variable_run_ids[name]:
                variable_run_ids[name].append(run_id)
        run_json = {
            "forecast_end": forecast_end,
            "forecast_start": forecast_start,
            "forecast_window_note": _FORECAST_WINDOW_NOTE,
            "forecast_only_series": [name for name in _FORECAST_ONLY_SERIES if name in public_series],
            "history_seeded_through": "2025.4",
            "periods": periods,
            "run_id": run_id,
            "scenario_name": _public_scenario_name(variant_id),
            "schema_version": 1,
            "series": public_series,
            "timestamp": _artifact_timestamp(str(scenario_payload.get("output_dir", ""))),
        }
        run_payloads[run_id] = run_json
        manifest_runs.append(
            {
                "data_path": f"runs/{run_id}.json",
                "forecast_end": forecast_end,
                "forecast_start": forecast_start,
                "family_id": str(run_spec["family_id"]),
                "family_label": str(run_spec["family_label"]),
                "family_maturity": str(run_spec["family_maturity"]),
                "forecast_window_note": _FORECAST_WINDOW_NOTE,
                "group": str(run_spec["group"]),
                "history_seeded_through": "2025.4",
                "label": str(run_spec["label"]),
                "run_id": run_id,
                "scenario_name": _public_scenario_name(variant_id),
                "summary": str(run_spec["summary"]),
                "timestamp": run_json["timestamp"],
            }
        )

    return {
        "manifest_runs": manifest_runs,
        "run_payloads": run_payloads,
        "available_variables": available_variables,
        "variable_run_ids": variable_run_ids,
        "run_specs": run_specs,
    }


def export_phase1_bridge_artifacts(
    *,
    report_path: Path | None = None,
    out_dir: Path | None = None,
    forecast_start: str = "2026.1",
    forecast_end: str = "2029.4",
    family_maturities: tuple[str, ...] = ("public",),
    family_ids: tuple[str, ...] | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    repo_root = paths.repo_root.resolve()

    def _display_path(path: Path) -> str:
        resolved = path.resolve()
        try:
            return str(resolved.relative_to(repo_root))
        except ValueError:
            return str(resolved)

    report_path = report_path or (paths.runtime_distribution_reports_root / "run_phase1_distribution_block.json")
    out_dir = out_dir or (paths.repo_root / "reports" / "phase1_distribution_block")
    if not report_path.exists():
        raise FileNotFoundError(
            f"Phase-1 solved report not found: {report_path}. Run `fp-ineq run-phase1-distribution-block` first."
        )

    export_inputs = _phase1_export_inputs(
        report_path=report_path,
        forecast_start=forecast_start,
        forecast_end=forecast_end,
        family_maturities=family_maturities,
        family_ids=family_ids,
    )
    manifest_runs = list(export_inputs["manifest_runs"])
    run_payloads = dict(export_inputs["run_payloads"])

    out_dir.mkdir(parents=True, exist_ok=True)
    bridge_results_path, bridge_metadata_path, bridge_row_count = _write_bridge_export(
        out_dir=out_dir,
        manifest_runs=manifest_runs,
        run_payloads=run_payloads,
        forecast_start=forecast_start,
    )

    bridge_export_report = {
        "out_dir": _display_path(out_dir),
        "report_path": _display_path(report_path),
        "bridge_metadata_path": _display_path(out_dir / bridge_metadata_path),
        "bridge_results_path": _display_path(out_dir / bridge_results_path),
        "bridge_row_count": bridge_row_count,
        "run_count": len(manifest_runs),
        "family_ids": sorted({str(item["family_id"]) for item in manifest_runs}),
        "family_maturities": list(family_maturities),
        "forecast_window_start": forecast_start,
        "forecast_window_end": forecast_end,
        "run_ids": [str(item["run_id"]) for item in manifest_runs],
    }
    (out_dir / "bridge_export_report.json").write_text(
        json.dumps(bridge_export_report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return bridge_export_report


def export_phase1_full_bundle(
    *,
    report_path: Path | None = None,
    out_dir: Path | None = None,
    forecast_start: str = "2026.1",
    forecast_end: str = "2029.4",
    family_maturities: tuple[str, ...] = ("public",),
    family_ids: tuple[str, ...] | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    report_path = report_path or (paths.runtime_distribution_reports_root / "run_phase1_distribution_block.json")
    out_dir = out_dir or paths.runtime_solved_public_root
    if not report_path.exists():
        raise FileNotFoundError(
            f"Phase-1 solved report not found: {report_path}. Run `fp-ineq run-phase1-distribution-block` first."
        )

    selected_specs = phase1_public_bundle_specs(
        family_maturities=family_maturities,
        family_ids=family_ids,
    )
    if not selected_specs:
        raise ValueError("No public phase-1 scenarios matched the requested family filters")

    if out_dir.exists():
        shutil.rmtree(out_dir)
    (out_dir / "runs").mkdir(parents=True, exist_ok=True)
    _copy_static_shell(out_dir)

    export_inputs = _phase1_export_inputs(
        report_path=report_path,
        forecast_start=forecast_start,
        forecast_end=forecast_end,
        family_maturities=family_maturities,
        family_ids=family_ids,
    )
    manifest_runs = list(export_inputs["manifest_runs"])
    run_payloads = dict(export_inputs["run_payloads"])
    available_variables = list(export_inputs["available_variables"])
    seen_variables = set(available_variables)
    variable_run_ids = dict(export_inputs["variable_run_ids"])
    run_specs = dict(export_inputs["run_specs"])

    for run_json in run_payloads.values():
        run_id = str(run_json["run_id"])
        run_path = out_dir / "runs" / f"{run_id}.json"
        run_path.write_text(json.dumps(run_json, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    bridge_results_path, bridge_metadata_path, bridge_row_count = _write_bridge_export(
        out_dir=out_dir,
        manifest_runs=manifest_runs,
        run_payloads=run_payloads,
        forecast_start=forecast_start,
    )

    presets = [
        {
            **preset,
            "variables": [name for name in preset["variables"] if name in seen_variables],
        }
        for preset in _PHASE1_FULL_PRESETS
    ]
    presets = [preset for preset in presets if preset["variables"]]
    (out_dir / "presets.json").write_text(
        json.dumps({"presets": presets}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (out_dir / "dictionary.json").write_text(
        json.dumps(
            _phase1_solved_dictionary(
                available_variables,
                bundle_run_ids=[item["run_id"] for item in manifest_runs],
                variable_run_ids=variable_run_ids,
            ),
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    manifest = {
        "available_variables": available_variables,
        "default_preset_ids": [
            preset_id for preset_id in _PHASE1_DEFAULT_PRESET_IDS if any(p["id"] == preset_id for p in presets)
        ],
        "default_run_ids": _default_manifest_run_ids(manifest_runs),
        "bridge_metadata_path": bridge_metadata_path,
        "bridge_results_path": bridge_results_path,
        "dictionary_path": "dictionary.json",
        "families": _phase1_manifest_families(list(run_specs.values())),
        "forecast_only_series": list(_FORECAST_ONLY_SERIES),
        "forecast_window_end": forecast_end,
        "forecast_window_note": _FORECAST_WINDOW_NOTE,
        "forecast_window_start": forecast_start,
        "generated_at": datetime.now(UTC).isoformat(),
        "history_seeded_through": "2025.4",
        "included_family_ids": [family["family_id"] for family in _phase1_manifest_families(list(run_specs.values()))],
        "included_family_maturities": list(family_maturities),
        "presets_path": "presets.json",
        "run_panel_note": _RUN_PANEL_NOTE,
        "runs": manifest_runs,
        "schema_version": 1,
        "site_subpath": "model-runs",
        "title": "FP Inequality Runs Explorer",
    }
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    export_report = {
        "out_dir": str(out_dir),
        "manifest_path": str(manifest_path),
        "report_path": str(report_path),
        "bridge_metadata_path": str(out_dir / bridge_metadata_path),
        "bridge_results_path": str(out_dir / bridge_results_path),
        "bridge_row_count": bridge_row_count,
        "run_count": len(manifest_runs),
        "variable_count": len(available_variables),
        "family_ids": [family["family_id"] for family in manifest["families"]],
        "family_maturities": list(family_maturities),
        "forecast_only_series": list(_FORECAST_ONLY_SERIES),
        "forecast_window_note": _FORECAST_WINDOW_NOTE,
        "run_ids": [item["run_id"] for item in manifest_runs],
    }
    (out_dir / "export_report.json").write_text(
        json.dumps(export_report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return export_report
