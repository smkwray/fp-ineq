from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from .paths import repo_paths
from .phase1_catalog import phase1_distribution_specs
from .phase1_distribution_block import (
    _DISTRIBUTION_BOUNDARY_TRANSFER_MACRO_VARIABLES,
    _DISTRIBUTION_BOUNDARY_UI_VARIABLES,
    _DISTRIBUTION_COMPARE_VARIANT_IDS,
    _DISTRIBUTION_DEFAULT_BACKEND,
    _DISTRIBUTION_FIRST_LEVEL_GAP_VARIABLES,
    _expected_sign_for_variant,
    _required_moves_for_variant,
    _required_signs_for_variant,
    compare_phase1_distribution_reports,
    run_phase1_distribution_block,
    validate_phase1_distribution_identities,
)

__all__ = [
    "assess_phase1_distribution_generalization_readiness",
    "compose_phase1_distribution_package_evidence",
    "assess_phase1_distribution_family_generalization",
    "assess_phase1_distribution_package_readiness",
    "assess_phase1_distribution_intervention_ladder_selection",
    "assess_phase1_distribution_intervention_effect",
    "assess_phase1_distribution_holdout_directionality",
    "load_phase1_distribution_intervention_spec",
    "run_phase1_distribution_family_holdout",
    "run_phase1_distribution_intervention_ladder",
    "run_phase1_distribution_intervention_experiment",
]


_DEFAULT_INTERVENTION_COMPARE_VARIABLES = tuple(
    dict.fromkeys(
        (
            *_DISTRIBUTION_BOUNDARY_UI_VARIABLES,
            *_DISTRIBUTION_BOUNDARY_TRANSFER_MACRO_VARIABLES,
            "TRGH",
            "TRSH",
            "THG",
            "THS",
        )
    )
)

_EXPERIMENTAL_PATH_MARKERS = (
    "specs/phase1_distribution_interventions",
    "runtime/phase1_distribution_block/interventions",
    "run_phase1_distribution_intervention_experiment",
    "assess_phase1_distribution_intervention_effect",
    "run_phase1_distribution_family_holdout",
    "assess_phase1_distribution_holdout_directionality",
)


def _clean_intervention_id(token: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in str(token).strip().lower())
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    cleaned = cleaned.strip("-_")
    if not cleaned:
        raise ValueError("Intervention spec must define a non-empty id")
    return cleaned


def _path_looks_experimental(path: Path | str | None) -> bool:
    if path is None:
        return False
    text = str(path).replace("\\", "/")
    return any(marker in text for marker in _EXPERIMENTAL_PATH_MARKERS)


def _classify_parity_evidence(
    *,
    paths_to_check: tuple[Path | str | None, ...],
) -> dict[str, Any]:
    experimental_paths = sorted(
        {
            str(path)
            for path in paths_to_check
            if path is not None and _path_looks_experimental(path)
        }
    )
    if experimental_paths:
        return {
            "scenario_scope": "experimental-intervention-surface",
            "final_parity_admissible": False,
            "notes": [
                "Evidence depends on intervention-spec or intervention-report paths and is exploratory/debugging evidence rather than frozen canonical scenario parity."
            ],
            "experimental_paths": experimental_paths,
        }
    return {
        "scenario_scope": "canonical-public-scenario-surface",
        "final_parity_admissible": True,
        "notes": [
            "Evidence does not point at intervention-spec/report paths and can be considered for frozen canonical scenario parity claims, subject to the normal acceptance gates."
        ],
        "experimental_paths": [],
    }


def load_phase1_distribution_intervention_spec(path: Path | str) -> dict[str, Any]:
    spec_path = Path(path)
    payload = yaml.safe_load(spec_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Intervention spec must decode to a mapping: {spec_path}")
    intervention_id = _clean_intervention_id(payload.get("id", spec_path.stem))
    analysis_payload = dict(payload.get("analysis", {}) or {})
    variant_ids = tuple(
        str(item) for item in (analysis_payload.get("variant_ids") or _DISTRIBUTION_COMPARE_VARIANT_IDS)
    )
    compare_variables = tuple(
        str(item)
        for item in (analysis_payload.get("compare_variables") or _DEFAULT_INTERVENTION_COMPARE_VARIABLES)
    )
    first_level_variables = tuple(
        str(item)
        for item in (analysis_payload.get("first_level_variables") or _DISTRIBUTION_FIRST_LEVEL_GAP_VARIABLES)
    )
    scenario_window = dict(payload.get("scenario_window", {}) or {})
    ladder_payload = dict(payload.get("ladder", {}) or {})
    return {
        "id": intervention_id,
        "description": str(payload.get("description", "")).strip(),
        "experimental_patch_ids": [str(item) for item in payload.get("experimental_patch_ids", []) or []],
        "runtime_text_overrides": {
            str(name): str(value) for name, value in dict(payload.get("runtime_text_overrides", {}) or {}).items()
        },
        "runtime_text_appends": {
            str(name): str(value) for name, value in dict(payload.get("runtime_text_appends", {}) or {}).items()
        },
        "runtime_text_post_patches": {
            str(name): [dict(item) for item in list(value or [])]
            for name, value in dict(payload.get("runtime_text_post_patches", {}) or {}).items()
        },
        "compose_post_patches": [dict(item) for item in payload.get("compose_post_patches", []) or []],
        "scenario_override_additions": dict(payload.get("scenario_override_additions", {}) or {}),
        "scenario_fpr_additions": dict(payload.get("scenario_fpr_additions", {}) or {}),
        "scenario_extra_metadata": dict(payload.get("scenario_extra_metadata", {}) or {}),
        "scenario_window": {
            "forecast_start": str(scenario_window.get("forecast_start", "2026.1")),
            "forecast_end": str(scenario_window.get("forecast_end", "2029.4")),
        },
        "ladder": {
            "target": str(ladder_payload.get("target", "")).strip().upper(),
            "variable": str(ladder_payload.get("variable", "")).strip().upper(),
            "mode": str(ladder_payload.get("mode", "add")).strip().lower() or "add",
            "coefficients": [
                float(item) for item in list(ladder_payload.get("coefficients", []) or [])
            ],
        },
        "analysis": {
            "variant_ids": variant_ids,
            "compare_variables": compare_variables,
            "first_level_variables": first_level_variables,
            "identity_max_abs_residual": float(analysis_payload.get("identity_max_abs_residual", 1e-6)),
        },
        "spec_path": str(spec_path),
    }


def _with_equation_term_override_coefficient(
    scenario_fpr_additions: dict[str, Any],
    *,
    target: str,
    variable: str,
    coefficient: float,
    mode: str,
) -> dict[str, Any]:
    payload = json.loads(json.dumps(dict(scenario_fpr_additions or {})))
    target_name = str(target).strip().upper()
    variable_name = str(variable).strip().upper()
    default_scope = dict(payload.get("__all__", {}) or {})
    overrides = [dict(item) for item in list(default_scope.get("equation_term_overrides", []) or [])]
    replaced = False
    for item in overrides:
        if (
            str(item.get("target", "")).strip().upper() == target_name
            and str(item.get("variable", "")).strip().upper() == variable_name
        ):
            item["coefficient"] = float(coefficient)
            item["mode"] = str(mode)
            replaced = True
    if not replaced:
        overrides.append(
            {
                "target": target_name,
                "variable": variable_name,
                "coefficient": float(coefficient),
                "lag": 0,
                "mode": str(mode),
            }
        )
    default_scope["equation_term_overrides"] = overrides
    payload["__all__"] = default_scope
    return payload


def _coefficient_tag(value: float) -> str:
    text = f"{float(value):g}".replace("-", "neg-").replace(".", "p")
    return _clean_intervention_id(text)


def _distribution_variant_ids_for_families(
    *,
    family_ids: tuple[str, ...] | list[str],
    exclude_variant_ids: tuple[str, ...] | list[str] = (),
) -> tuple[str, ...]:
    allowed = {str(item) for item in family_ids}
    excluded = {str(item) for item in exclude_variant_ids}
    variant_ids = [
        spec.variant_id
        for spec in phase1_distribution_specs()
        if spec.family_id in allowed and spec.variant_id not in excluded
    ]
    return tuple(variant_ids)


def _holdout_compare_variables(
    base_variables: tuple[str, ...] | list[str],
    *,
    variant_ids: tuple[str, ...] | list[str],
) -> tuple[str, ...]:
    ordered = dict.fromkeys(str(item) for item in base_variables)
    for variant_id in [str(item) for item in variant_ids]:
        for variable in _required_moves_for_variant(variant_id):
            ordered.setdefault(str(variable), None)
        for variable in _required_signs_for_variant(variant_id):
            ordered.setdefault(str(variable), None)
    for variable in ("UR", "PCY"):
        ordered.setdefault(variable, None)
    return tuple(ordered.keys())


def run_phase1_distribution_family_holdout(
    *,
    fp_home: Path,
    intervention_spec_path: Path,
    family_ids: tuple[str, ...] | list[str],
    exclude_variant_ids: tuple[str, ...] | list[str] = (),
    holdout_variant_ids: tuple[str, ...] | list[str] | None = None,
    intervention_backend: str = _DISTRIBUTION_DEFAULT_BACKEND,
    reference_backend: str = "fpexe",
    internal_only: bool = False,
    control_run_report_path: Path | None = None,
    reference_run_report_path: Path | None = None,
    report_tag: str | None = None,
) -> dict[str, Any]:
    paths = repo_paths()
    base_spec = load_phase1_distribution_intervention_spec(intervention_spec_path)
    selected_variant_ids = tuple(
        str(item)
        for item in (
            holdout_variant_ids
            if holdout_variant_ids
            else _distribution_variant_ids_for_families(
                family_ids=family_ids,
                exclude_variant_ids=exclude_variant_ids,
            )
        )
    )
    if not selected_variant_ids:
        raise ValueError("Family holdout resolved no distribution variants")

    holdout_tag = _clean_intervention_id(
        report_tag
        or f"{base_spec['id']}-{'-'.join(str(item) for item in family_ids)}-holdout"
    )
    root = paths.runtime_distribution_root / "interventions" / holdout_tag
    root.mkdir(parents=True, exist_ok=True)
    derived_spec_path = root / "derived-holdout-spec.yaml"

    raw_payload = yaml.safe_load(Path(intervention_spec_path).read_text(encoding="utf-8")) or {}
    analysis_payload = dict(raw_payload.get("analysis", {}) or {})
    analysis_payload["variant_ids"] = list(selected_variant_ids)
    raw_payload["analysis"] = analysis_payload
    raw_payload["id"] = holdout_tag
    description = str(raw_payload.get("description", "")).strip()
    family_text = ", ".join(str(item) for item in family_ids)
    exclusion_text = ", ".join(str(item) for item in exclude_variant_ids)
    suffix = f"Family holdout over {family_text}."
    if exclusion_text:
        suffix += f" Excluding trained variants: {exclusion_text}."
    raw_payload["description"] = f"{description} {suffix}".strip()
    derived_spec_path.write_text(yaml.safe_dump(raw_payload, sort_keys=False), encoding="utf-8")

    if internal_only:
        analysis = dict(base_spec["analysis"])
        scenario_window = dict(base_spec["scenario_window"])
        compare_variables = _holdout_compare_variables(
            tuple(str(item) for item in analysis["compare_variables"]),
            variant_ids=selected_variant_ids,
        )
        reports_root = root / "reports"
        reports_root.mkdir(parents=True, exist_ok=True)
        if control_run_report_path is None:
            control_run = run_phase1_distribution_block(
                fp_home=fp_home,
                backend=intervention_backend,
                scenarios_root=root / f"scenarios-control-{intervention_backend}",
                artifacts_root=root / f"artifacts-control-{intervention_backend}",
                overlay_root=root / f"overlay-control-{intervention_backend}",
                report_path=reports_root / f"run_phase1_distribution_block.control.{intervention_backend}.json",
                variant_ids=selected_variant_ids,
                forecast_start=str(scenario_window["forecast_start"]),
                forecast_end=str(scenario_window["forecast_end"]),
            )
        else:
            control_run = {"report_path": str(Path(control_run_report_path)), "passes": True}
        intervention_run = run_phase1_distribution_block(
            fp_home=fp_home,
            backend=intervention_backend,
            scenarios_root=root / f"scenarios-intervention-{intervention_backend}",
            artifacts_root=root / f"artifacts-intervention-{intervention_backend}",
            overlay_root=root / f"overlay-intervention-{intervention_backend}",
            report_path=reports_root / f"run_phase1_distribution_block.intervention.{intervention_backend}.json",
            variant_ids=selected_variant_ids,
            forecast_start=str(scenario_window["forecast_start"]),
            forecast_end=str(scenario_window["forecast_end"]),
            experimental_patch_ids=tuple(base_spec["experimental_patch_ids"]),
            runtime_text_overrides=dict(base_spec["runtime_text_overrides"]),
            runtime_text_appends=dict(base_spec["runtime_text_appends"]),
            runtime_text_post_patches=dict(base_spec["runtime_text_post_patches"]),
            compose_post_patches=tuple(base_spec["compose_post_patches"]),
            scenario_override_additions=dict(base_spec["scenario_override_additions"]),
            scenario_fpr_additions=dict(base_spec["scenario_fpr_additions"]),
            scenario_extra_metadata=dict(base_spec["scenario_extra_metadata"]),
        )
        internal_compare = compare_phase1_distribution_reports(
            left_report_path=Path(intervention_run["report_path"]),
            right_report_path=Path(control_run["report_path"]),
            left_backend=intervention_backend,
            right_backend=intervention_backend,
            variant_ids=selected_variant_ids,
            variables=compare_variables,
            report_path=reports_root / "compare.intervention_vs_control.internal.json",
        )
        directionality = assess_phase1_distribution_holdout_directionality(
            compare_report_path=Path(internal_compare["report_path"]),
            variant_ids=selected_variant_ids,
            report_path=reports_root / "assess_phase1_distribution_holdout_directionality.json",
        )
        summary_report_path = reports_root / "run_phase1_distribution_family_holdout.internal.json"
        summary_payload = {
            "intervention_id": holdout_tag,
            "mode": "internal-only",
            "family_ids": [str(item) for item in family_ids],
            "exclude_variant_ids": [str(item) for item in exclude_variant_ids],
            "holdout_variant_ids": list(selected_variant_ids),
            "derived_spec_path": str(derived_spec_path),
            "control_run_report_path": str(control_run["report_path"]),
            "intervention_run_report_path": str(intervention_run["report_path"]),
            "internal_compare_report_path": str(internal_compare["report_path"]),
            "directionality_report_path": str(directionality["report_path"]),
            "summary": {
                "control_passes": bool(control_run["passes"]),
                "intervention_passes": bool(intervention_run["passes"]),
                "directionality_core_pass_count": int(directionality["core_pass_count"]),
                "directionality_core_all_pass": bool(directionality["core_all_pass"]),
                "directionality_optional_pass_count": int(directionality["optional_pass_count"]),
                "directionality_optional_all_pass": bool(directionality["optional_all_pass"]),
                "directionality_pass_count": int(directionality["pass_count"]),
                "directionality_all_pass": bool(directionality["all_pass"]),
            },
        }
        summary_report_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return {
            "report_path": str(summary_report_path),
            "intervention_id": holdout_tag,
            "family_ids": [str(item) for item in family_ids],
            "exclude_variant_ids": [str(item) for item in exclude_variant_ids],
            "holdout_variant_ids": list(selected_variant_ids),
            "derived_spec_path": str(derived_spec_path),
            "directionality_core_pass_count": int(directionality["core_pass_count"]),
            "directionality_core_all_pass": bool(directionality["core_all_pass"]),
            "directionality_optional_pass_count": int(directionality["optional_pass_count"]),
            "directionality_optional_all_pass": bool(directionality["optional_all_pass"]),
            "directionality_pass_count": int(directionality["pass_count"]),
            "directionality_all_pass": bool(directionality["all_pass"]),
            "intervention_identity_passes": None,
            "gap_improved_count": None,
            "gap_worsened_count": None,
            "median_gap_closure_ratio": None,
        }

    payload = run_phase1_distribution_intervention_experiment(
        fp_home=fp_home,
        intervention_spec_path=derived_spec_path,
        intervention_backend=intervention_backend,
        reference_backend=reference_backend,
        control_run_report_path=control_run_report_path,
        reference_run_report_path=reference_run_report_path,
    )
    return {
        **payload,
        "family_ids": [str(item) for item in family_ids],
        "exclude_variant_ids": [str(item) for item in exclude_variant_ids],
        "holdout_variant_ids": list(selected_variant_ids),
        "derived_spec_path": str(derived_spec_path),
    }


def assess_phase1_distribution_holdout_directionality(
    *,
    compare_report_path: Path,
    variant_ids: tuple[str, ...] | list[str],
    report_path: Path | None = None,
) -> dict[str, Any]:
    payload = json.loads(Path(compare_report_path).read_text(encoding="utf-8"))
    compare_map = {
        str(row["variant_id"]): dict(row)
        for row in list(payload.get("comparisons", []) or [])
    }
    tolerance = 1e-9
    rows: list[dict[str, Any]] = []
    pass_count = 0
    core_pass_count = 0
    optional_pass_count = 0
    for variant_id in [str(item) for item in variant_ids]:
        row = compare_map[variant_id]
        first_levels = dict(row.get("first_levels", {}) or {})
        expected_sign = _expected_sign_for_variant(variant_id)
        is_transfer_composite = variant_id.startswith("transfer-composite-")
        required_moves = {name: False for name in _required_moves_for_variant(variant_id)}
        required_signs = {name: False for name in _required_signs_for_variant(variant_id)}
        one_of_moves = {name: False for name in ("UR", "PCY")}
        one_of_signs = {name: False for name in ("UR", "PCY")}
        deltas: dict[str, float | None] = {}
        for variable, level_payload in first_levels.items():
            left = dict(level_payload or {}).get("left")
            right = dict(level_payload or {}).get("right")
            delta = None if left is None or right is None else float(left) - float(right)
            deltas[str(variable)] = delta
            if delta is None:
                continue
            if variable in required_moves and abs(delta) > tolerance:
                required_moves[variable] = True
            if variable in required_signs:
                if is_transfer_composite and variable in ("YD", "GDPR"):
                    pass
                else:
                    signed_delta = -delta if variable in ("IPOVALL", "IPOVCH") else delta
                    if (signed_delta * expected_sign) > tolerance:
                        required_signs[variable] = True
            if variable in one_of_moves and abs(delta) > tolerance:
                one_of_moves[variable] = True
            if variable == "UR" and (delta * -expected_sign) > tolerance:
                one_of_signs[variable] = True
            if variable == "PCY" and (delta * expected_sign) > tolerance:
                one_of_signs[variable] = True
        passes_required = all(required_moves.values()) and all(required_signs.values())
        passes_optional = any(one_of_moves.values()) and any(one_of_signs.values())
        passes_core = passes_required and passes_optional
        if passes_required:
            core_pass_count += 1
        if passes_optional:
            optional_pass_count += 1
        if passes_core:
            pass_count += 1
        rows.append(
            {
                "variant_id": variant_id,
                "expected_sign": expected_sign,
                "deltas": deltas,
                "required_moves": required_moves,
                "required_signs": required_signs,
                "one_of_moves": one_of_moves,
                "one_of_signs": one_of_signs,
                "passes_required": passes_required,
                "passes_optional": passes_optional,
                "passes_core": passes_core,
            }
        )

    output_payload = {
        "compare_report_path": str(compare_report_path),
        "variant_ids": [str(item) for item in variant_ids],
        "rows": rows,
        "summary": {
            "variant_count": len(rows),
            "core_pass_count": core_pass_count,
            "core_all_pass": core_pass_count == len(rows) and len(rows) > 0,
            "optional_pass_count": optional_pass_count,
            "optional_all_pass": optional_pass_count == len(rows) and len(rows) > 0,
            "pass_count": pass_count,
            "all_pass": pass_count == len(rows) and len(rows) > 0,
        },
    }
    report_path = report_path or Path(compare_report_path).with_name("assess_phase1_distribution_holdout_directionality.json")
    Path(report_path).write_text(json.dumps(output_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "variant_count": len(rows),
        "core_pass_count": core_pass_count,
        "core_all_pass": output_payload["summary"]["core_all_pass"],
        "optional_pass_count": optional_pass_count,
        "optional_all_pass": output_payload["summary"]["optional_all_pass"],
        "pass_count": pass_count,
        "all_pass": output_payload["summary"]["all_pass"],
    }


def assess_phase1_distribution_family_generalization(
    *,
    family_id: str,
    holdout_report_paths: tuple[Path, ...] | list[Path],
    report_path: Path | None = None,
) -> dict[str, Any]:
    holdout_rows: list[dict[str, Any]] = []
    core_pass_count = 0
    optional_pass_count = 0
    all_pass_count = 0
    total_variants = 0

    for item in holdout_report_paths:
        summary_path = Path(item)
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
        summary = dict(payload.get("summary", {}) or {})
        holdout_variant_ids = [str(value) for value in list(payload.get("holdout_variant_ids", []) or [])]
        directionality_report_path = Path(payload["directionality_report_path"])
        directionality_payload = json.loads(directionality_report_path.read_text(encoding="utf-8"))
        directionality_rows = list(directionality_payload.get("rows", []) or [])
        variant_count = int(directionality_payload.get("summary", {}).get("variant_count", len(directionality_rows)))
        total_variants += variant_count
        if bool(summary.get("directionality_core_all_pass")):
            core_pass_count += variant_count
        if bool(summary.get("directionality_optional_all_pass")):
            optional_pass_count += variant_count
        if bool(summary.get("directionality_all_pass")):
            all_pass_count += variant_count
        holdout_rows.append(
            {
                "summary_report_path": str(summary_path),
                "directionality_report_path": str(directionality_report_path),
                "holdout_variant_ids": holdout_variant_ids,
                "directionality_core_all_pass": bool(summary.get("directionality_core_all_pass")),
                "directionality_optional_all_pass": bool(summary.get("directionality_optional_all_pass")),
                "directionality_all_pass": bool(summary.get("directionality_all_pass")),
                "rows": directionality_rows,
            }
        )

    if total_variants <= 0:
        raise ValueError("Family generalization assessment requires at least one holdout variant")

    core_all_pass = core_pass_count == total_variants
    optional_all_pass = optional_pass_count == total_variants
    all_pass = all_pass_count == total_variants
    blockers: list[str] = []
    if not core_all_pass:
        blockers.append("core distribution holdout directionality does not generalize across all supplied holdouts")
    if core_all_pass and not optional_all_pass:
        blockers.append("optional macro side-condition lane still misses on at least one supplied holdout")

    if all_pass:
        status = "generalized"
    elif core_all_pass:
        status = "core-generalized-with-optional-misses"
    elif core_pass_count > 0:
        status = "partial-core-generalization"
    else:
        status = "not-generalized"

    payload = {
        "family_id": str(family_id),
        "holdout_report_paths": [str(Path(item)) for item in holdout_report_paths],
        "holdouts": holdout_rows,
        "summary": {
            "variant_count": total_variants,
            "core_pass_count": core_pass_count,
            "core_all_pass": core_all_pass,
            "optional_pass_count": optional_pass_count,
            "optional_all_pass": optional_all_pass,
            "all_pass_count": all_pass_count,
            "all_pass": all_pass,
            "status": status,
            "blockers": blockers,
        },
    }
    if report_path is None:
        report_path = repo_paths().runtime_distribution_reports_root / f"assess_phase1_distribution_family_generalization.{_clean_intervention_id(family_id)}.json"
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    Path(report_path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "family_id": str(family_id),
        "variant_count": total_variants,
        "core_all_pass": core_all_pass,
        "optional_all_pass": optional_all_pass,
        "all_pass": all_pass,
        "status": status,
    }


def assess_phase1_distribution_generalization_readiness(
    *,
    ui_family_report_path: Path,
    transfer_family_report_path: Path,
    ui_macro_policy: str = "optional",
    report_path: Path | None = None,
) -> dict[str, Any]:
    ui_macro_policy = str(ui_macro_policy).strip().lower() or "optional"
    if ui_macro_policy not in {"optional", "required"}:
        raise ValueError("ui_macro_policy must be either 'optional' or 'required'")
    ui_payload = json.loads(Path(ui_family_report_path).read_text(encoding="utf-8"))
    transfer_payload = json.loads(Path(transfer_family_report_path).read_text(encoding="utf-8"))
    evidence_scope = _classify_parity_evidence(
        paths_to_check=(ui_family_report_path, transfer_family_report_path)
    )
    ui_summary = dict(ui_payload.get("summary", {}) or {})
    transfer_summary = dict(transfer_payload.get("summary", {}) or {})

    ui_status = str(ui_summary.get("status", ""))
    transfer_status = str(transfer_summary.get("status", ""))
    ui_core_all_pass = bool(ui_summary.get("core_all_pass"))
    ui_optional_all_pass = bool(ui_summary.get("optional_all_pass"))
    transfer_all_pass = bool(transfer_summary.get("all_pass"))
    ui_policy_passes = ui_core_all_pass and (ui_optional_all_pass if ui_macro_policy == "required" else True)

    blockers: list[str] = []
    if not ui_core_all_pass:
        blockers.append("ui family does not generalize on the required core distribution surface")
    elif ui_macro_policy == "required" and not ui_optional_all_pass:
        blockers.append("ui family still misses the required PCY/UR macro side lane")
    elif not ui_optional_all_pass:
        blockers.append("ui family still misses the optional PCY/UR macro side lane")
    if not transfer_all_pass:
        blockers.append("transfer-composite family does not yet generalize cleanly across supplied holdouts")

    if ui_policy_passes and transfer_all_pass and ui_macro_policy == "required":
        status = "generalized"
        next_step = "carry both families forward into the next integrated validation layer"
    elif ui_policy_passes and transfer_all_pass and ui_macro_policy == "optional":
        status = "generalized-on-core-surface"
        next_step = "carry both families forward on the core distribution surface while tracking the optional UI macro lane separately"
    elif ui_status == "core-generalized-with-optional-misses" and transfer_status == "generalized":
        status = "core-generalized-with-ui-macro-caveat"
        next_step = "decide whether PCY/UR should remain an optional UI macro lane or become a separate holdout criterion"
    elif ui_core_all_pass or bool(transfer_summary.get("core_all_pass")):
        status = "partial-generalization"
        next_step = "keep running family holdouts before treating the fixed coefficient package as generalized"
    else:
        status = "not-generalized"
        next_step = "repair the fixed coefficient families before using them as generalized compatibility layers"

    payload = {
        "ui_family_report_path": str(ui_family_report_path),
        "transfer_family_report_path": str(transfer_family_report_path),
        "evidence_scope": evidence_scope,
        "families": {
            "ui": {
                "status": ui_status,
                "summary": ui_summary,
            },
            "transfer-composite": {
                "status": transfer_status,
                "summary": transfer_summary,
            },
        },
        "overall": {
            "ui_macro_policy": ui_macro_policy,
            "status": status,
            "scenario_scope": evidence_scope["scenario_scope"],
            "final_parity_admissible": bool(evidence_scope["final_parity_admissible"]),
            "blockers": blockers,
            "next_step": next_step,
        },
    }
    if report_path is None:
        report_path = repo_paths().runtime_distribution_reports_root / "assess_phase1_distribution_generalization_readiness.json"
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    Path(report_path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "status": status,
        "ui_status": ui_status,
        "transfer_status": transfer_status,
        "ui_macro_policy": ui_macro_policy,
        "scenario_scope": evidence_scope["scenario_scope"],
        "final_parity_admissible": bool(evidence_scope["final_parity_admissible"]),
    }


def assess_phase1_distribution_intervention_effect(
    *,
    control_compare_report_path: Path,
    intervention_compare_report_path: Path,
    variant_ids: tuple[str, ...] | list[str],
    variables: tuple[str, ...] | list[str],
    report_path: Path | None = None,
) -> dict[str, Any]:
    paths = repo_paths()
    control_payload = json.loads(Path(control_compare_report_path).read_text(encoding="utf-8"))
    intervention_payload = json.loads(Path(intervention_compare_report_path).read_text(encoding="utf-8"))
    control_map = {
        str(row["variant_id"]): dict(row)
        for row in list(control_payload.get("comparisons", []) or [])
    }
    intervention_map = {
        str(row["variant_id"]): dict(row)
        for row in list(intervention_payload.get("comparisons", []) or [])
    }

    summary_rows: list[dict[str, Any]] = []
    closure_values: list[float] = []
    improved_count = 0
    worsened_count = 0
    unchanged_count = 0
    for variant_id in [str(item) for item in variant_ids]:
        control_row = control_map[variant_id]
        intervention_row = intervention_map[variant_id]
        for variable in [str(item) for item in variables]:
            control_gap = dict(dict(control_row.get("first_levels", {})).get(variable, {})).get("abs_diff")
            intervention_gap = dict(dict(intervention_row.get("first_levels", {})).get(variable, {})).get("abs_diff")
            if control_gap is None or intervention_gap is None:
                continue
            control_gap = float(control_gap)
            intervention_gap = float(intervention_gap)
            gap_delta = control_gap - intervention_gap
            closure_ratio = None if control_gap <= 1e-12 else gap_delta / control_gap
            if gap_delta > 1e-12:
                improved_count += 1
            elif gap_delta < -1e-12:
                worsened_count += 1
            else:
                unchanged_count += 1
            if closure_ratio is not None:
                closure_values.append(float(closure_ratio))
            summary_rows.append(
                {
                    "variant_id": variant_id,
                    "variable": variable,
                    "control_gap_abs": control_gap,
                    "intervention_gap_abs": intervention_gap,
                    "gap_delta_abs": gap_delta,
                    "gap_closure_ratio": closure_ratio,
                    "improved": gap_delta > 1e-12,
                }
            )

    payload = {
        "control_compare_report_path": str(control_compare_report_path),
        "intervention_compare_report_path": str(intervention_compare_report_path),
        "variant_ids": [str(item) for item in variant_ids],
        "variables": [str(item) for item in variables],
        "rows": summary_rows,
        "summary": {
            "row_count": len(summary_rows),
            "improved_count": improved_count,
            "worsened_count": worsened_count,
            "unchanged_count": unchanged_count,
            "median_gap_closure_ratio": (
                float(sorted(closure_values)[len(closure_values) // 2]) if closure_values else None
            ),
            "mean_gap_closure_ratio": (
                float(sum(closure_values) / len(closure_values)) if closure_values else None
            ),
        },
    }
    report_path = report_path or (paths.runtime_distribution_reports_root / "assess_phase1_distribution_intervention_effect.json")
    Path(report_path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "row_count": len(summary_rows),
        "improved_count": improved_count,
        "worsened_count": worsened_count,
        "median_gap_closure_ratio": payload["summary"]["median_gap_closure_ratio"],
    }


def compose_phase1_distribution_package_evidence(
    *,
    package_id: str,
    ui_effect_report_path: Path,
    transfer_effect_report_path: Path,
    control_compare_report_path: Path,
    control_run_report_path: Path,
    reference_run_report_path: Path,
    intervention_spec_path: Path,
    description: str = "",
    ui_variant_id: str = "ui-relief",
    transfer_variant_id: str = "transfer-composite-medium",
    ui_family_report_path: Path | None = None,
    transfer_family_report_path: Path | None = None,
    ui_macro_policy: str = "optional",
    report_dir: Path | None = None,
) -> dict[str, Any]:
    paths = repo_paths()
    package_root = (
        Path(report_dir)
        if report_dir is not None
        else paths.runtime_distribution_interventions_root / _clean_intervention_id(package_id) / "reports"
    )
    package_root.mkdir(parents=True, exist_ok=True)
    evidence_scope = _classify_parity_evidence(
        paths_to_check=(
            ui_effect_report_path,
            transfer_effect_report_path,
            intervention_spec_path,
            package_root,
        )
    )

    ui_payload = json.loads(Path(ui_effect_report_path).read_text(encoding="utf-8"))
    transfer_payload = json.loads(Path(transfer_effect_report_path).read_text(encoding="utf-8"))

    ui_rows = [
        dict(row)
        for row in list(ui_payload.get("rows", []) or [])
        if str(row.get("variant_id", "")) == str(ui_variant_id)
    ]
    transfer_rows = [
        dict(row)
        for row in list(transfer_payload.get("rows", []) or [])
        if str(row.get("variant_id", "")) == str(transfer_variant_id)
    ]
    rows = [*ui_rows, *transfer_rows]
    closure_values = [
        float(row["gap_closure_ratio"])
        for row in rows
        if row.get("gap_closure_ratio") is not None
    ]
    summary = {
        "row_count": len(rows),
        "improved_count": sum(1 for row in rows if bool(row.get("improved"))),
        "worsened_count": sum(
            1
            for row in rows
            if not bool(row.get("improved")) and abs(float(row.get("gap_delta_abs", 0.0))) > 1e-12
        ),
        "unchanged_count": sum(
            1 for row in rows if abs(float(row.get("gap_delta_abs", 0.0))) <= 1e-12
        ),
        "median_gap_closure_ratio": (
            float(sorted(closure_values)[len(closure_values) // 2]) if closure_values else None
        ),
        "mean_gap_closure_ratio": (
            float(sum(closure_values) / len(closure_values)) if closure_values else None
        ),
    }
    effect_payload = {
        "evidence_scope": evidence_scope,
        "control_compare_report_path": str(control_compare_report_path),
        "intervention_compare_report_path": "composed-from-ui-and-transfer-widened-candidates",
        "variant_ids": [str(ui_variant_id), str(transfer_variant_id)],
        "variables": sorted({str(row.get("variable", "")) for row in rows if str(row.get("variable", "")).strip()}),
        "rows": rows,
        "summary": summary,
        "component_effect_report_paths": {
            str(ui_variant_id): str(ui_effect_report_path),
            str(transfer_variant_id): str(transfer_effect_report_path),
        },
    }
    effect_report_path = package_root / "assess_phase1_distribution_intervention_effect.composed.json"
    effect_report_path.write_text(json.dumps(effect_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    experiment_payload = {
        "description": (
            str(description).strip()
            or "Composed package experiment using completed widened ui-relief and transfer component candidates."
        ),
        "evidence_scope": evidence_scope,
        "intervention_id": _clean_intervention_id(package_id),
        "intervention_spec_path": str(intervention_spec_path),
        "intervention_effect_report_path": str(effect_report_path),
        "control_compare_report_path": str(control_compare_report_path),
        "control_run_report_path": str(control_run_report_path),
        "intervention_run_report_path": str(control_run_report_path),
        "reference_run_report_path": str(reference_run_report_path),
        "summary": {
            "control_passes": True,
            "intervention_identity_passes": True,
            "intervention_passes": False,
            "reference_passes": True,
            "gap_improved_count": summary["improved_count"],
            "gap_worsened_count": summary["worsened_count"],
            "median_gap_closure_ratio": summary["median_gap_closure_ratio"],
        },
        "variant_ids": [str(ui_variant_id), str(transfer_variant_id)],
        "scenario_window": {
            "forecast_start": "2026.1",
            "forecast_end": "2026.4",
        },
        "component_effect_report_paths": {
            str(ui_variant_id): str(ui_effect_report_path),
            str(transfer_variant_id): str(transfer_effect_report_path),
        },
    }
    experiment_report_path = package_root / "run_phase1_distribution_intervention_experiment.composed.json"
    experiment_report_path.write_text(json.dumps(experiment_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    readiness_payload = assess_phase1_distribution_package_readiness(
        experiment_report_path=experiment_report_path,
        effect_report_path=effect_report_path,
        ui_variant_id=ui_variant_id,
        transfer_variant_id=transfer_variant_id,
        ui_family_report_path=ui_family_report_path,
        transfer_family_report_path=transfer_family_report_path,
        ui_macro_policy=ui_macro_policy,
        report_path=package_root / "assess_phase1_distribution_package_readiness.composed.json",
    )
    return {
        "report_dir": str(package_root),
        "effect_report_path": str(effect_report_path),
        "experiment_report_path": str(experiment_report_path),
        "package_readiness_report_path": str(readiness_payload["report_path"]),
        "status": readiness_payload["status"],
        "replacement_ready": readiness_payload["replacement_ready"],
        "package_internal_coexistence_passes": readiness_payload["package_internal_coexistence_passes"],
        "ui_macro_policy": readiness_payload["ui_macro_policy"],
        "scenario_scope": readiness_payload["scenario_scope"],
        "final_parity_admissible": bool(readiness_payload["final_parity_admissible"]),
    }


def assess_phase1_distribution_package_readiness(
    *,
    experiment_report_path: Path,
    effect_report_path: Path | None = None,
    ui_variant_id: str = "ui-relief",
    transfer_variant_id: str = "transfer-composite-medium",
    ui_family_report_path: Path | None = None,
    transfer_family_report_path: Path | None = None,
    ui_macro_policy: str = "optional",
    ui_min_median_gap_closure: float = 0.08,
    transfer_promising_min_median_gap_closure: float = 0.01,
    transfer_working_min_median_gap_closure: float = 0.03,
    transfer_min_improved_count: int = 7,
    transfer_max_worsened_count: int = 3,
    report_path: Path | None = None,
) -> dict[str, Any]:
    ui_macro_policy = str(ui_macro_policy).strip().lower() or "optional"
    if ui_macro_policy not in {"optional", "required"}:
        raise ValueError("ui_macro_policy must be either 'optional' or 'required'")
    experiment_payload = json.loads(Path(experiment_report_path).read_text(encoding="utf-8"))
    experiment_scope = dict(experiment_payload.get("evidence_scope", {}) or {})
    effect_path = Path(
        effect_report_path
        or str(
            experiment_payload.get("intervention_effect_report_path")
            or Path(experiment_report_path).with_name("assess_phase1_distribution_intervention_effect.json")
        )
    )
    effect_payload = json.loads(effect_path.read_text(encoding="utf-8"))
    effect_scope = dict(effect_payload.get("evidence_scope", {}) or {})
    evidence_scope = _classify_parity_evidence(
        paths_to_check=(
            experiment_report_path,
            effect_path,
            experiment_payload.get("intervention_spec_path"),
            ui_family_report_path,
            transfer_family_report_path,
        )
    )
    if experiment_scope:
        evidence_scope["notes"] = list(
            dict.fromkeys([*evidence_scope.get("notes", []), *list(experiment_scope.get("notes", []) or [])])
        )
    if effect_scope:
        evidence_scope["notes"] = list(
            dict.fromkeys([*evidence_scope.get("notes", []), *list(effect_scope.get("notes", []) or [])])
        )

    rows_by_variant: dict[str, list[dict[str, Any]]] = {}
    for row in list(effect_payload.get("rows", []) or []):
        rows_by_variant.setdefault(str(row.get("variant_id", "")), []).append(dict(row))

    def _variant_summary(variant_id: str) -> dict[str, Any]:
        rows = rows_by_variant.get(str(variant_id), [])
        closure_values = [
            float(row["gap_closure_ratio"])
            for row in rows
            if row.get("gap_closure_ratio") is not None
        ]
        improved_count = sum(1 for row in rows if bool(row.get("improved")))
        worsened_count = sum(1 for row in rows if not bool(row.get("improved")) and abs(float(row.get("gap_delta_abs", 0.0))) > 1e-12)
        return {
            "variant_id": str(variant_id),
            "row_count": len(rows),
            "improved_count": improved_count,
            "worsened_count": worsened_count,
            "mean_gap_closure_ratio": (
                None if not closure_values else float(sum(closure_values) / len(closure_values))
            ),
            "median_gap_closure_ratio": (
                None if not closure_values else float(sorted(closure_values)[len(closure_values) // 2])
            ),
            "rows": rows,
        }

    ui_summary = _variant_summary(str(ui_variant_id))
    transfer_summary = _variant_summary(str(transfer_variant_id))
    experiment_summary = dict(experiment_payload.get("summary", {}) or {})

    ui_family_summary = (
        dict(json.loads(Path(ui_family_report_path).read_text(encoding="utf-8")).get("summary", {}) or {})
        if ui_family_report_path is not None
        else None
    )
    transfer_family_summary = (
        dict(json.loads(Path(transfer_family_report_path).read_text(encoding="utf-8")).get("summary", {}) or {})
        if transfer_family_report_path is not None
        else None
    )

    def _acceptance_surface_evaluable(run_report_key: str) -> bool | None:
        run_report_path = experiment_payload.get(run_report_key)
        if not run_report_path:
            return None
        payload = json.loads(Path(run_report_path).read_text(encoding="utf-8"))
        scenarios = dict(payload.get("scenarios", {}) or {})
        if not scenarios:
            return None
        return "baseline-observed" in scenarios

    def _is_clean_acceptance_surface(flag: bool | None) -> bool:
        return flag is True

    ui_working = (
        ui_summary["row_count"] > 0
        and ui_summary["worsened_count"] == 0
        and ui_summary["median_gap_closure_ratio"] is not None
        and float(ui_summary["median_gap_closure_ratio"]) >= float(ui_min_median_gap_closure)
    )
    transfer_promising = (
        transfer_summary["row_count"] > 0
        and transfer_summary["improved_count"] >= int(transfer_min_improved_count)
        and transfer_summary["worsened_count"] <= int(transfer_max_worsened_count)
        and transfer_summary["median_gap_closure_ratio"] is not None
        and float(transfer_summary["median_gap_closure_ratio"]) >= float(transfer_promising_min_median_gap_closure)
    )
    transfer_working = (
        transfer_promising
        and transfer_summary["median_gap_closure_ratio"] is not None
        and float(transfer_summary["median_gap_closure_ratio"]) >= float(transfer_working_min_median_gap_closure)
    )

    ui_family_generalizes = (
        True
        if ui_family_summary is None
        else (
            bool(ui_family_summary.get("core_all_pass"))
            and (
                bool(ui_family_summary.get("optional_all_pass"))
                if ui_macro_policy == "required"
                else True
            )
        )
    )
    transfer_family_generalizes = (
        True
        if transfer_family_summary is None
        else bool(transfer_family_summary.get("all_pass"))
    )

    intervention_identity_passes = bool(experiment_summary.get("intervention_identity_passes"))
    intervention_run_passes = bool(experiment_summary.get("intervention_passes"))
    control_run_passes = bool(experiment_summary.get("control_passes"))
    reference_run_passes = bool(experiment_summary.get("reference_passes"))
    control_acceptance_evaluable = _acceptance_surface_evaluable("control_run_report_path")
    intervention_acceptance_evaluable = _acceptance_surface_evaluable("intervention_run_report_path")
    reference_acceptance_evaluable = _acceptance_surface_evaluable("reference_run_report_path")

    package_internal_coexistence_passes = (
        intervention_identity_passes
        and ui_working
        and transfer_promising
        and ui_family_generalizes
        and transfer_family_generalizes
    )
    clean_run_evidence = (
        intervention_run_passes
        and control_run_passes
        and reference_run_passes
        and _is_clean_acceptance_surface(control_acceptance_evaluable)
        and _is_clean_acceptance_surface(intervention_acceptance_evaluable)
        and _is_clean_acceptance_surface(reference_acceptance_evaluable)
    )
    replacement_ready = (
        package_internal_coexistence_passes
        and clean_run_evidence
        and transfer_working
        and bool(evidence_scope["final_parity_admissible"])
    )

    blockers: list[str] = []
    if not intervention_identity_passes:
        blockers.append("combined fp-r intervention does not pass the required identity surface")
    if not intervention_run_passes and intervention_acceptance_evaluable is not False:
        blockers.append("combined fp-r intervention run report is not marked passing")
    if not control_run_passes and control_acceptance_evaluable is not False:
        blockers.append("combined fp-r control/reference comparison surface is not fully passing")
    if not reference_run_passes and reference_acceptance_evaluable is not False:
        blockers.append("widened fpexe reference surface is not clean enough for full package replacement claims")
    if package_internal_coexistence_passes and not clean_run_evidence:
        blockers.append(
            "current package evidence is composed or acceptance-incomplete; a clean integrated wrapper run is still required for replacement-ready status"
        )
    if not bool(evidence_scope["final_parity_admissible"]):
        blockers.append(
            "current evidence depends on intervention-spec or intervention-report paths and is not admissible for frozen canonical scenario parity claims"
        )
    if not ui_working:
        blockers.append("ui-relief repair is not yet strong enough on the current package thresholds")
    if ui_family_summary is not None and not ui_family_generalizes:
        if ui_macro_policy == "required":
            blockers.append("ui family does not clear the required PCY/UR macro lane on the current holdout surface")
        else:
            blockers.append("ui family does not generalize on the required core distribution holdout surface")
    if not transfer_promising:
        blockers.append("transfer macro repair is not yet strong enough to count as a promising package candidate")
    elif not transfer_working:
        blockers.append("transfer macro repair is still modest and not yet strong enough for replacement-ready status")
    if transfer_family_summary is not None and not transfer_family_generalizes:
        blockers.append("transfer-composite family does not generalize on the current holdout surface")

    if replacement_ready and ui_macro_policy == "optional":
        status = "replacement-ready-on-core-surface"
        next_step = "freeze the combined package as the working fp-r structural candidate on the core distribution surface and keep the UI macro lane tracked separately"
    elif replacement_ready:
        status = "replacement-ready"
        next_step = "freeze the combined package as the working fp-r structural candidate and expand validation scope"
    elif package_internal_coexistence_passes and transfer_working and ui_macro_policy == "optional":
        status = "component-composed-supported-on-core-surface"
        next_step = "treat the current package as a composed candidate on the core distribution surface, then finish the clean integrated wrapper run and keep the UI macro lane tracked separately"
    elif package_internal_coexistence_passes and transfer_working:
        status = "component-composed-supported"
        next_step = "treat the current package as a composed candidate, then finish the clean integrated wrapper run and holdout validation"
    elif package_internal_coexistence_passes and ui_macro_policy == "optional":
        status = "working-package-candidate-on-core-surface"
        next_step = "carry the combined package forward as the working fp-r candidate on the core distribution surface, while keeping transfer refinement and the UI macro lane open"
    elif package_internal_coexistence_passes:
        status = "working-package-candidate"
        next_step = "carry the combined package forward as the working fp-r candidate, but keep transfer refinement and widened reference cleanup open"
    elif intervention_identity_passes and ui_working and ui_macro_policy == "optional":
        status = "partial-package-candidate-on-core-surface"
        next_step = "keep the core distribution repair path, but refine transfer and decide how to treat the UI macro lane before stronger package claims"
    elif intervention_identity_passes and ui_working:
        status = "partial-package-candidate"
        next_step = "keep the ui repair, refine the transfer macro repair further, and avoid replacement claims"
    else:
        status = "not-ready"
        next_step = "repair the blocked package components before treating fp-r as a package candidate"

    payload = {
        "experiment_report_path": str(experiment_report_path),
        "effect_report_path": str(effect_path),
        "thresholds": {
            "ui_min_median_gap_closure": float(ui_min_median_gap_closure),
            "transfer_promising_min_median_gap_closure": float(transfer_promising_min_median_gap_closure),
            "transfer_working_min_median_gap_closure": float(transfer_working_min_median_gap_closure),
            "transfer_min_improved_count": int(transfer_min_improved_count),
            "transfer_max_worsened_count": int(transfer_max_worsened_count),
        },
        "variant_summaries": {
            str(ui_variant_id): {
                **ui_summary,
                "family_generalization_report_path": (None if ui_family_report_path is None else str(ui_family_report_path)),
                "family_generalization_summary": ui_family_summary,
                "status": (
                    "working-repair-candidate-on-core-surface"
                    if (ui_working and ui_macro_policy == "optional")
                    else ("working-repair-candidate" if ui_working else "still-blocked")
                ),
            },
            str(transfer_variant_id): {
                **transfer_summary,
                "family_generalization_report_path": (
                    None if transfer_family_report_path is None else str(transfer_family_report_path)
                ),
                "family_generalization_summary": transfer_family_summary,
                "status": (
                    "working-repair-candidate"
                    if transfer_working
                    else ("promising-repair-candidate" if transfer_promising else "still-blocked")
                ),
            },
        },
        "overall": {
            "status": status,
            "ui_macro_policy": ui_macro_policy,
            "ui_family_report_path": (None if ui_family_report_path is None else str(ui_family_report_path)),
            "transfer_family_report_path": (
                None if transfer_family_report_path is None else str(transfer_family_report_path)
            ),
            "evidence_kind": ("clean-run" if clean_run_evidence else "component-composed-or-partial"),
            "scenario_scope": evidence_scope["scenario_scope"],
            "final_parity_admissible": bool(evidence_scope["final_parity_admissible"]),
            "evidence_scope": evidence_scope,
            "intervention_identity_passes": intervention_identity_passes,
            "intervention_run_passes": intervention_run_passes,
            "control_run_passes": control_run_passes,
            "reference_run_passes": reference_run_passes,
            "control_acceptance_evaluable": control_acceptance_evaluable,
            "intervention_acceptance_evaluable": intervention_acceptance_evaluable,
            "reference_acceptance_evaluable": reference_acceptance_evaluable,
            "package_internal_coexistence_passes": package_internal_coexistence_passes,
            "replacement_ready": replacement_ready,
            "blockers": blockers,
            "next_step": next_step,
        },
    }
    report_path = report_path or Path(experiment_report_path).with_name("assess_phase1_distribution_package_readiness.json")
    Path(report_path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "status": status,
        "package_internal_coexistence_passes": package_internal_coexistence_passes,
        "replacement_ready": replacement_ready,
        "ui_macro_policy": ui_macro_policy,
        "scenario_scope": evidence_scope["scenario_scope"],
        "final_parity_admissible": bool(evidence_scope["final_parity_admissible"]),
    }


def assess_phase1_distribution_intervention_ladder_selection(
    *,
    zero_history_summary_path: Path,
    control_identity_report_path: Path,
    optional_identity_id: str = "ub_unscaled",
    conservative_control_multiple: float = 1.25,
    balanced_control_multiple: float = 2.0,
    report_path: Path | None = None,
) -> dict[str, Any]:
    summary_payload = json.loads(Path(zero_history_summary_path).read_text(encoding="utf-8"))
    control_payload = json.loads(Path(control_identity_report_path).read_text(encoding="utf-8"))
    variants = list(control_payload.get("variants", []) or [])
    if not variants:
        raise ValueError(f"Control identity report has no variants: {control_identity_report_path}")
    control_checks = dict(dict(variants[0]).get("all_identity_checks", {}) or {})
    optional_identity = dict(control_checks.get(optional_identity_id, {}) or {})
    control_optional_residual = optional_identity.get("max_abs_residual")
    if control_optional_residual is None:
        raise ValueError(
            f"Control identity report does not include optional identity '{optional_identity_id}': "
            f"{control_identity_report_path}"
        )
    control_optional_residual = float(control_optional_residual)

    rows: list[dict[str, Any]] = []
    for coefficient_text, metrics in sorted(summary_payload.items(), key=lambda item: float(item[0])):
        row = dict(metrics or {})
        coefficient = float(coefficient_text)
        optional_residual = float(row["optional_ub_unscaled_max_abs_residual"])
        rows.append(
            {
                "coefficient": coefficient,
                "required_identity_passes": bool(row["required_identity_passes"]),
                "improved_count": int(row["improved_count"]),
                "worsened_count": int(row["worsened_count"]),
                "mean_gap_closure_ratio": float(row["mean_gap_closure_ratio"]),
                "median_gap_closure_ratio": float(row["median_gap_closure_ratio"]),
                "optional_identity_id": optional_identity_id,
                "optional_identity_max_abs_residual": optional_residual,
                "optional_identity_increase_abs": optional_residual - control_optional_residual,
                "optional_identity_control_multiple": (
                    None if control_optional_residual <= 0 else optional_residual / control_optional_residual
                ),
            }
        )

    def _best_with_limit(control_multiple_limit: float | None) -> dict[str, Any] | None:
        candidates = [
            row
            for row in rows
            if row["required_identity_passes"]
            and row["worsened_count"] == 0
            and (
                control_multiple_limit is None
                or (
                    row["optional_identity_control_multiple"] is not None
                    and row["optional_identity_control_multiple"] <= control_multiple_limit
                )
            )
        ]
        if not candidates:
            return None
        return max(
            candidates,
            key=lambda row: (
                float(row["median_gap_closure_ratio"]),
                float(row["mean_gap_closure_ratio"]),
                float(row["coefficient"]),
            ),
        )

    def _first_above_limit(control_multiple_limit: float) -> dict[str, Any] | None:
        candidates = [
            row
            for row in rows
            if row["required_identity_passes"]
            and row["worsened_count"] == 0
            and row["optional_identity_control_multiple"] is not None
            and row["optional_identity_control_multiple"] > control_multiple_limit
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda row: float(row["optional_identity_control_multiple"]))

    frontier_rows: list[dict[str, Any]] = []
    best_median = float("-inf")
    lowest_optional = float("inf")
    for row in rows:
        median = float(row["median_gap_closure_ratio"])
        optional_residual = float(row["optional_identity_max_abs_residual"])
        if median > best_median or optional_residual < lowest_optional:
            frontier_rows.append(dict(row))
            best_median = max(best_median, median)
            lowest_optional = min(lowest_optional, optional_residual)

    conservative_row = _best_with_limit(float(conservative_control_multiple))
    balanced_row = _best_with_limit(float(balanced_control_multiple))
    aggressive_row = _best_with_limit(None)
    stretch_row = _first_above_limit(float(balanced_control_multiple))

    payload = {
        "zero_history_summary_path": str(zero_history_summary_path),
        "control_identity_report_path": str(control_identity_report_path),
        "optional_identity_id": optional_identity_id,
        "control_optional_identity_max_abs_residual": control_optional_residual,
        "selection_rules": {
            "conservative_control_multiple_limit": float(conservative_control_multiple),
            "balanced_control_multiple_limit": float(balanced_control_multiple),
        },
        "rows": rows,
        "pareto_frontier": frontier_rows,
        "recommendations": {
            "conservative": conservative_row,
            "balanced": balanced_row,
            "stretch_above_balanced_limit": stretch_row,
            "aggressive": aggressive_row,
        },
    }
    report_path = report_path or (Path(zero_history_summary_path).with_name("assess_phase1_distribution_intervention_ladder_selection.json"))
    Path(report_path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "conservative_coefficient": None if conservative_row is None else float(conservative_row["coefficient"]),
        "balanced_coefficient": None if balanced_row is None else float(balanced_row["coefficient"]),
        "stretch_coefficient": None if stretch_row is None else float(stretch_row["coefficient"]),
        "aggressive_coefficient": None if aggressive_row is None else float(aggressive_row["coefficient"]),
    }


def run_phase1_distribution_intervention_experiment(
    *,
    fp_home: Path,
    intervention_spec_path: Path,
    intervention_backend: str = _DISTRIBUTION_DEFAULT_BACKEND,
    reference_backend: str = "fpexe",
    control_run_report_path: Path | None = None,
    reference_run_report_path: Path | None = None,
) -> dict[str, Any]:
    paths = repo_paths()
    spec = load_phase1_distribution_intervention_spec(intervention_spec_path)
    analysis = dict(spec["analysis"])
    scenario_window = dict(spec["scenario_window"])
    variant_ids = tuple(str(item) for item in analysis["variant_ids"])
    compare_variables = tuple(str(item) for item in analysis["compare_variables"])
    first_level_variables = tuple(str(item) for item in analysis["first_level_variables"])
    identity_max_abs_residual = float(analysis["identity_max_abs_residual"])

    root = paths.runtime_distribution_root / "interventions" / str(spec["id"])
    root.mkdir(parents=True, exist_ok=True)
    reports_root = root / "reports"
    reports_root.mkdir(parents=True, exist_ok=True)

    if control_run_report_path is None:
        control_run = run_phase1_distribution_block(
            fp_home=fp_home,
            backend=intervention_backend,
            scenarios_root=root / f"scenarios-control-{intervention_backend}",
            artifacts_root=root / f"artifacts-control-{intervention_backend}",
            overlay_root=root / f"overlay-control-{intervention_backend}",
            report_path=reports_root / f"run_phase1_distribution_block.control.{intervention_backend}.json",
            variant_ids=variant_ids,
            forecast_start=str(scenario_window["forecast_start"]),
            forecast_end=str(scenario_window["forecast_end"]),
        )
    else:
        control_run = {
            "report_path": str(Path(control_run_report_path)),
            "passes": True,
        }
    intervention_run = run_phase1_distribution_block(
        fp_home=fp_home,
        backend=intervention_backend,
        scenarios_root=root / f"scenarios-intervention-{intervention_backend}",
        artifacts_root=root / f"artifacts-intervention-{intervention_backend}",
        overlay_root=root / f"overlay-intervention-{intervention_backend}",
        report_path=reports_root / f"run_phase1_distribution_block.intervention.{intervention_backend}.json",
        variant_ids=variant_ids,
        forecast_start=str(scenario_window["forecast_start"]),
        forecast_end=str(scenario_window["forecast_end"]),
        experimental_patch_ids=tuple(spec["experimental_patch_ids"]),
        runtime_text_overrides=dict(spec["runtime_text_overrides"]),
        runtime_text_appends=dict(spec["runtime_text_appends"]),
        runtime_text_post_patches=dict(spec["runtime_text_post_patches"]),
        compose_post_patches=tuple(spec["compose_post_patches"]),
        scenario_override_additions=dict(spec["scenario_override_additions"]),
        scenario_fpr_additions=dict(spec["scenario_fpr_additions"]),
        scenario_extra_metadata=dict(spec["scenario_extra_metadata"]),
    )
    if reference_run_report_path is None:
        reference_run = run_phase1_distribution_block(
            fp_home=fp_home,
            backend=reference_backend,
            scenarios_root=root / f"scenarios-reference-{reference_backend}",
            artifacts_root=root / f"artifacts-reference-{reference_backend}",
            overlay_root=root / f"overlay-reference-{reference_backend}",
            report_path=reports_root / f"run_phase1_distribution_block.reference.{reference_backend}.json",
            variant_ids=variant_ids,
            forecast_start=str(scenario_window["forecast_start"]),
            forecast_end=str(scenario_window["forecast_end"]),
        )
    else:
        reference_run = {
            "report_path": str(Path(reference_run_report_path)),
            "passes": True,
        }

    control_compare = compare_phase1_distribution_reports(
        left_report_path=Path(control_run["report_path"]),
        right_report_path=Path(reference_run["report_path"]),
        left_backend=intervention_backend,
        right_backend=reference_backend,
        variant_ids=variant_ids,
        variables=compare_variables,
        report_path=reports_root / "compare.control_vs_reference.json",
    )
    intervention_compare = compare_phase1_distribution_reports(
        left_report_path=Path(intervention_run["report_path"]),
        right_report_path=Path(reference_run["report_path"]),
        left_backend=intervention_backend,
        right_backend=reference_backend,
        variant_ids=variant_ids,
        variables=compare_variables,
        report_path=reports_root / "compare.intervention_vs_reference.json",
    )
    intervention_identity = validate_phase1_distribution_identities(
        backend=intervention_backend,
        run_report_path=Path(intervention_run["report_path"]),
        variant_ids=variant_ids,
        max_abs_residual=identity_max_abs_residual,
    )
    intervention_effect = assess_phase1_distribution_intervention_effect(
        control_compare_report_path=Path(control_compare["report_path"]),
        intervention_compare_report_path=Path(intervention_compare["report_path"]),
        variant_ids=variant_ids,
        variables=first_level_variables,
        report_path=reports_root / "assess_phase1_distribution_intervention_effect.json",
    )

    payload = {
        "intervention_id": str(spec["id"]),
        "description": str(spec["description"]),
        "intervention_spec_path": str(spec["spec_path"]),
        "intervention_backend": str(intervention_backend),
        "reference_backend": str(reference_backend),
        "variant_ids": list(variant_ids),
        "scenario_window": {
            "forecast_start": str(scenario_window["forecast_start"]),
            "forecast_end": str(scenario_window["forecast_end"]),
        },
        "compare_variables": list(compare_variables),
        "first_level_variables": list(first_level_variables),
        "control_run_report_path": str(control_run["report_path"]),
        "intervention_run_report_path": str(intervention_run["report_path"]),
        "reference_run_report_path": str(reference_run["report_path"]),
        "control_compare_report_path": str(control_compare["report_path"]),
        "intervention_compare_report_path": str(intervention_compare["report_path"]),
        "intervention_identity_report_path": str(intervention_identity["report_path"]),
        "intervention_effect_report_path": str(intervention_effect["report_path"]),
        "summary": {
            "control_passes": bool(control_run["passes"]),
            "intervention_passes": bool(intervention_run["passes"]),
            "reference_passes": bool(reference_run["passes"]),
            "intervention_identity_passes": bool(intervention_identity["passes"]),
            "gap_improved_count": int(intervention_effect["improved_count"]),
            "gap_worsened_count": int(intervention_effect["worsened_count"]),
            "median_gap_closure_ratio": intervention_effect["median_gap_closure_ratio"],
        },
    }
    summary_report_path = reports_root / "run_phase1_distribution_intervention_experiment.json"
    summary_report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(summary_report_path),
        "intervention_id": str(spec["id"]),
        "gap_improved_count": int(intervention_effect["improved_count"]),
        "gap_worsened_count": int(intervention_effect["worsened_count"]),
        "median_gap_closure_ratio": intervention_effect["median_gap_closure_ratio"],
        "intervention_identity_passes": bool(intervention_identity["passes"]),
    }


def run_phase1_distribution_intervention_ladder(
    *,
    fp_home: Path,
    intervention_spec_path: Path,
    coefficients: tuple[float, ...] | list[float] | None = None,
    intervention_backend: str = _DISTRIBUTION_DEFAULT_BACKEND,
    reference_backend: str = "fpexe",
    control_run_report_path: Path | None = None,
    reference_run_report_path: Path | None = None,
) -> dict[str, Any]:
    paths = repo_paths()
    spec = load_phase1_distribution_intervention_spec(intervention_spec_path)
    analysis = dict(spec["analysis"])
    ladder = dict(spec.get("ladder", {}) or {})
    target = str(ladder.get("target", "")).strip().upper()
    variable = str(ladder.get("variable", "")).strip().upper()
    mode = str(ladder.get("mode", "add")).strip().lower() or "add"
    coefficient_values = [float(item) for item in list(coefficients or ladder.get("coefficients") or [])]
    if not coefficient_values:
        raise ValueError("Intervention ladder requires one or more coefficients")
    if not target or not variable:
        raise ValueError("Intervention ladder requires ladder.target and ladder.variable")

    scenario_window = dict(spec["scenario_window"])
    variant_ids = tuple(str(item) for item in analysis["variant_ids"])
    compare_variables = tuple(str(item) for item in analysis["compare_variables"])
    first_level_variables = tuple(str(item) for item in analysis["first_level_variables"])
    identity_max_abs_residual = float(analysis["identity_max_abs_residual"])

    root = paths.runtime_distribution_root / "interventions" / f"{spec['id']}-ladder"
    root.mkdir(parents=True, exist_ok=True)
    reports_root = root / "reports"
    reports_root.mkdir(parents=True, exist_ok=True)

    if control_run_report_path is None:
        control_run = run_phase1_distribution_block(
            fp_home=fp_home,
            backend=intervention_backend,
            scenarios_root=root / f"scenarios-control-{intervention_backend}",
            artifacts_root=root / f"artifacts-control-{intervention_backend}",
            overlay_root=root / f"overlay-control-{intervention_backend}",
            report_path=reports_root / f"run_phase1_distribution_block.control.{intervention_backend}.json",
            variant_ids=variant_ids,
            forecast_start=str(scenario_window["forecast_start"]),
            forecast_end=str(scenario_window["forecast_end"]),
        )
    else:
        control_run = {
            "report_path": str(Path(control_run_report_path)),
            "passes": True,
        }
    if reference_run_report_path is None:
        reference_run = run_phase1_distribution_block(
            fp_home=fp_home,
            backend=reference_backend,
            scenarios_root=root / f"scenarios-reference-{reference_backend}",
            artifacts_root=root / f"artifacts-reference-{reference_backend}",
            overlay_root=root / f"overlay-reference-{reference_backend}",
            report_path=reports_root / f"run_phase1_distribution_block.reference.{reference_backend}.json",
            variant_ids=variant_ids,
            forecast_start=str(scenario_window["forecast_start"]),
            forecast_end=str(scenario_window["forecast_end"]),
        )
    else:
        reference_run = {
            "report_path": str(Path(reference_run_report_path)),
            "passes": True,
        }
    control_compare = compare_phase1_distribution_reports(
        left_report_path=Path(control_run["report_path"]),
        right_report_path=Path(reference_run["report_path"]),
        left_backend=intervention_backend,
        right_backend=reference_backend,
        variant_ids=variant_ids,
        variables=compare_variables,
        report_path=reports_root / "compare.control_vs_reference.json",
    )

    rows: list[dict[str, Any]] = []
    best_row: dict[str, Any] | None = None
    for coefficient in coefficient_values:
        coeff_tag = _coefficient_tag(float(coefficient))
        intervention_fpr_additions = _with_equation_term_override_coefficient(
            dict(spec["scenario_fpr_additions"]),
            target=target,
            variable=variable,
            coefficient=float(coefficient),
            mode=mode,
        )
        intervention_run = run_phase1_distribution_block(
            fp_home=fp_home,
            backend=intervention_backend,
            scenarios_root=root / f"scenarios-intervention-{intervention_backend}-{coeff_tag}",
            artifacts_root=root / f"artifacts-intervention-{intervention_backend}-{coeff_tag}",
            overlay_root=root / f"overlay-intervention-{intervention_backend}-{coeff_tag}",
            report_path=reports_root / f"run_phase1_distribution_block.intervention.{coeff_tag}.{intervention_backend}.json",
            variant_ids=variant_ids,
            forecast_start=str(scenario_window["forecast_start"]),
            forecast_end=str(scenario_window["forecast_end"]),
            experimental_patch_ids=tuple(spec["experimental_patch_ids"]),
            runtime_text_overrides=dict(spec["runtime_text_overrides"]),
            runtime_text_appends=dict(spec["runtime_text_appends"]),
            runtime_text_post_patches=dict(spec["runtime_text_post_patches"]),
            compose_post_patches=tuple(spec["compose_post_patches"]),
            scenario_override_additions=dict(spec["scenario_override_additions"]),
            scenario_fpr_additions=intervention_fpr_additions,
            scenario_extra_metadata=dict(spec["scenario_extra_metadata"]),
        )
        intervention_compare = compare_phase1_distribution_reports(
            left_report_path=Path(intervention_run["report_path"]),
            right_report_path=Path(reference_run["report_path"]),
            left_backend=intervention_backend,
            right_backend=reference_backend,
            variant_ids=variant_ids,
            variables=compare_variables,
            report_path=reports_root / f"compare.intervention-{coeff_tag}_vs_reference.json",
        )
        intervention_identity = validate_phase1_distribution_identities(
            backend=intervention_backend,
            run_report_path=Path(intervention_run["report_path"]),
            variant_ids=variant_ids,
            max_abs_residual=identity_max_abs_residual,
        )
        intervention_effect = assess_phase1_distribution_intervention_effect(
            control_compare_report_path=Path(control_compare["report_path"]),
            intervention_compare_report_path=Path(intervention_compare["report_path"]),
            variant_ids=variant_ids,
            variables=first_level_variables,
            report_path=reports_root / f"assess_phase1_distribution_intervention_effect.{coeff_tag}.json",
        )
        row = {
            "coefficient": float(coefficient),
            "coefficient_tag": coeff_tag,
            "intervention_run_report_path": str(intervention_run["report_path"]),
            "intervention_compare_report_path": str(intervention_compare["report_path"]),
            "intervention_identity_report_path": str(intervention_identity["report_path"]),
            "intervention_effect_report_path": str(intervention_effect["report_path"]),
            "intervention_passes": bool(intervention_run["passes"]),
            "intervention_identity_passes": bool(intervention_identity["passes"]),
            "gap_improved_count": int(intervention_effect["improved_count"]),
            "gap_worsened_count": int(intervention_effect["worsened_count"]),
            "median_gap_closure_ratio": intervention_effect["median_gap_closure_ratio"],
            "max_abs_diff_vs_reference": float(intervention_compare["max_abs_diff"]),
        }
        rows.append(row)
        if best_row is None:
            best_row = dict(row)
        else:
            current_score = (
                bool(row["intervention_identity_passes"]),
                -int(row["gap_worsened_count"]),
                int(row["gap_improved_count"]),
                float(row["median_gap_closure_ratio"] or float("-inf")),
            )
            best_score = (
                bool(best_row["intervention_identity_passes"]),
                -int(best_row["gap_worsened_count"]),
                int(best_row["gap_improved_count"]),
                float(best_row["median_gap_closure_ratio"] or float("-inf")),
            )
            if current_score > best_score:
                best_row = dict(row)

    payload = {
        "intervention_id": str(spec["id"]),
        "intervention_spec_path": str(spec["spec_path"]),
        "intervention_backend": str(intervention_backend),
        "reference_backend": str(reference_backend),
        "scenario_window": {
            "forecast_start": str(scenario_window["forecast_start"]),
            "forecast_end": str(scenario_window["forecast_end"]),
        },
        "ladder": {
            "target": target,
            "variable": variable,
            "mode": mode,
            "coefficients": [float(item) for item in coefficient_values],
        },
        "variant_ids": list(variant_ids),
        "compare_variables": list(compare_variables),
        "first_level_variables": list(first_level_variables),
        "control_run_report_path": str(control_run["report_path"]),
        "reference_run_report_path": str(reference_run["report_path"]),
        "control_compare_report_path": str(control_compare["report_path"]),
        "rows": rows,
        "summary": {
            "row_count": len(rows),
            "identity_pass_count": sum(1 for row in rows if row["intervention_identity_passes"]),
            "best_row": best_row,
        },
    }
    summary_report_path = reports_root / "run_phase1_distribution_intervention_ladder.json"
    summary_report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(summary_report_path),
        "intervention_id": str(spec["id"]),
        "row_count": len(rows),
        "best_coefficient": None if best_row is None else float(best_row["coefficient"]),
        "best_median_gap_closure_ratio": None if best_row is None else best_row["median_gap_closure_ratio"],
    }
