from __future__ import annotations

import json
from pathlib import Path

from .paths import repo_paths
from .phase1_catalog import phase1_family_by_id, phase1_public_bundle_specs

__all__ = ["assess_phase1_contrary_channels"]


_AUDIT_VARIABLES = ("TRLOWZ", "UR", "GDPR", "YD", "RS", "IPOVALL", "IPOVCH")


def _expected_sign(variant_id: str) -> float:
    if variant_id.endswith("-shock"):
        return -1.0
    return 1.0


def _countervailing_flags(delta_map: dict[str, float | None], *, expected_sign: float) -> dict[str, bool]:
    def _value(name: str) -> float | None:
        value = delta_map.get(name)
        return None if value is None else float(value)

    ur = _value("UR")
    gdpr = _value("GDPR")
    yd = _value("YD")
    rs = _value("RS")
    return {
        "ur_countervailing": False if ur is None else (ur * -expected_sign) < 0.0,
        "gdpr_countervailing": False if gdpr is None else (gdpr * expected_sign) < 0.0,
        "yd_countervailing": False if yd is None else (yd * expected_sign) < 0.0,
        "rate_countervailing": False if rs is None else (rs * expected_sign) > 0.0,
    }


def _float_or_none(value: float | None) -> float | None:
    return None if value is None else float(value)


def _scenario_audit_entry(
    *,
    variant_id: str,
    delta_map: dict[str, float | None],
) -> dict[str, object]:
    expected_sign = _expected_sign(variant_id)
    return {
        "expected_sign": expected_sign,
        "selected_deltas": {name: _float_or_none(delta_map.get(name)) for name in _AUDIT_VARIABLES},
        "countervailing_flags": _countervailing_flags(delta_map, expected_sign=expected_sign),
    }


def _family_summary(entries: list[dict[str, object]]) -> dict[str, object]:
    count = len(entries)
    if count == 0:
        return {
            "scenario_count": 0,
            "countervailing_counts": {},
            "has_any_countervailing_signal": False,
        }
    keys = tuple(dict(entries[0]["countervailing_flags"]).keys())
    counts = {
        key: sum(1 for entry in entries if bool(dict(entry["countervailing_flags"]).get(key)))
        for key in keys
    }
    return {
        "scenario_count": count,
        "countervailing_counts": counts,
        "has_any_countervailing_signal": any(value > 0 for value in counts.values()),
    }


def _recommendation(payload: dict[str, object]) -> dict[str, str]:
    ui_summary = dict(dict(payload["families"]).get("ui", {}))
    if bool(ui_summary.get("has_any_countervailing_signal")):
        ui_recommendation = (
            "Keep the public UI family framed as demand-dominant with the existing private offset caveat. "
            "The public UI runs already show some endogenous countervailing signals, and the private matching-offset "
            "family remains the bounded way to stress-test the missing dedicated adverse channel."
        )
    else:
        ui_recommendation = (
            "UI still looks one-sided in the public runs; rely on the private matching-offset family rather than "
            "inventing additional public contrary scenarios."
        )
    return {
        "ui": ui_recommendation,
        "non_ui": (
            "Do not add synthetic contrary families for the other public transfer families yet. "
            "Use the endogenous countervailing macro moves already visible in the solved Fair runs unless a specific "
            "omitted mechanism and patch point is identified."
        ),
    }


def _markdown_summary(payload: dict[str, object]) -> str:
    lines = [
        "# Contrary-Channel Audit",
        "",
        payload["summary"],
        "",
        "## Family summaries",
        "",
    ]
    families = phase1_family_by_id()
    family_payloads = dict(payload["families"])
    seen_family_ids: set[str] = set()
    for family_id in [spec.family_id for spec in phase1_public_bundle_specs()]:
        if family_id in seen_family_ids:
            continue
        seen_family_ids.add(family_id)
        if family_id not in family_payloads:
            continue
        summary = dict(family_payloads[family_id])
        label = families[family_id].label
        counts = dict(summary["countervailing_counts"])
        rendered_counts = ", ".join(f"{key}={value}" for key, value in counts.items())
        lines.append(f"- `{family_id}` ({label}): scenarios={summary['scenario_count']}; any_countervailing={str(summary['has_any_countervailing_signal']).lower()}; {rendered_counts}")
    lines.extend(
        [
            "",
            "## Recommendations",
            "",
            f"- UI: {payload['recommendations']['ui']}",
            f"- Non-UI: {payload['recommendations']['non_ui']}",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def assess_phase1_contrary_channels(
    *,
    report_path: Path | None = None,
    ui_offset_report_path: Path | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    report_path = report_path or (paths.runtime_distribution_reports_root / "run_phase1_distribution_block.json")
    ui_offset_report_path = ui_offset_report_path or (paths.runtime_ui_offset_reports_root / "run_phase2_ui_offset.json")
    if not report_path.exists():
        raise FileNotFoundError(
            f"Distribution report missing: {report_path}. Run `fp-ineq run-phase1-distribution-block` first."
        )

    distribution_report = json.loads(report_path.read_text(encoding="utf-8"))
    comparisons = dict(dict(distribution_report["acceptance"]).get("comparisons", {}))
    families = phase1_family_by_id()
    scenario_entries: dict[str, dict[str, object]] = {}
    family_entries: dict[str, list[dict[str, object]]] = {}
    for spec in phase1_public_bundle_specs():
        delta_map = dict(comparisons.get(spec.variant_id, {}))
        entry = _scenario_audit_entry(variant_id=spec.variant_id, delta_map=delta_map)
        scenario_entries[spec.variant_id] = entry
        family_entries.setdefault(spec.family_id, []).append(entry)

    family_payloads = {
        family_id: {
            **_family_summary(entries),
            "label": families[family_id].label,
        }
        for family_id, entries in family_entries.items()
    }

    payload: dict[str, object] = {
        "distribution_report_path": str(report_path),
        "ui_offset_report_path": str(ui_offset_report_path) if ui_offset_report_path.exists() else None,
        "summary": (
            "Contrary-channel audit of the public transfer-family runs using solved Fair output comparisons. "
            "This report is descriptive: it identifies endogenous countervailing macro signals already present in "
            "the checked-in runs and keeps the dedicated omitted-channel stress test confined to the private UI offset family."
        ),
        "scenarios": scenario_entries,
        "families": family_payloads,
    }
    if ui_offset_report_path.exists():
        ui_offset = json.loads(ui_offset_report_path.read_text(encoding="utf-8"))
        payload["ui_offset_context"] = {
            "report_path": str(ui_offset_report_path),
            "target_clawback_share": float(dict(ui_offset["metrics"])["target_clawback_share"]),
            "achieved_clawback_share": float(dict(ui_offset["metrics"])["clawback_share"]),
            "trlowz_relative_gap": float(dict(ui_offset["metrics"])["trlowz_relative_gap"]),
            "offset_has_lower_gdpr": bool(dict(dict(ui_offset["acceptance"])["diagnostics"])["offset_has_lower_gdpr"]),
        }
    payload["recommendations"] = _recommendation(payload)

    json_path = paths.runtime_distribution_reports_root / "assess_phase1_contrary_channels.json"
    md_path = paths.runtime_distribution_reports_root / "assess_phase1_contrary_channels.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_markdown_summary(payload), encoding="utf-8")
    return {
        "report_path": str(json_path),
        "markdown_path": str(md_path),
        "recommendations": payload["recommendations"],
    }
