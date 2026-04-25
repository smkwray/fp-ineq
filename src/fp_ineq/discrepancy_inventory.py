from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .paths import repo_paths

__all__ = ["build_phase1_bridge_discrepancy_inventory"]

_BRIDGE_NUMERIC_COLUMNS = (
    "dose_value",
    "delta_trlowz",
    "delta_ipovall",
    "delta_ipovch",
    "delta_rydpc",
    "delta_iginihh",
    "delta_imedrinc",
)
_DIFF_TOLERANCE = 1e-8
_DEFAULT_COMPARISON_SPECS = (
    {
        "comparison_id": "tracked_default_vs_fpexe",
        "left_id": "tracked-default",
        "right_id": "fpexe",
        "left_dir": "reports/phase1_distribution_block",
        "right_dir": "reports/phase1_distribution_block_fpexe",
    },
    {
        "comparison_id": "tracked_default_vs_fprremote2",
        "left_id": "tracked-default",
        "right_id": "fprremote2",
        "left_dir": "reports/phase1_distribution_block",
        "right_dir": "reports/phase1_distribution_block_fprremote2",
    },
    {
        "comparison_id": "fprremote2_vs_fpexe",
        "left_id": "fprremote2",
        "right_id": "fpexe",
        "left_dir": "reports/phase1_distribution_block_fprremote2",
        "right_dir": "reports/phase1_distribution_block_fpexe",
    },
)


@dataclass(frozen=True)
class _BridgeComparisonSpec:
    comparison_id: str
    left_id: str
    right_id: str
    left_dir: Path
    right_dir: Path


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _float_close_zero(value: float, *, tolerance: float = 1e-12) -> bool:
    return abs(value) <= tolerance


def _sign(value: float, *, tolerance: float = 1e-12) -> int:
    if _float_close_zero(value, tolerance=tolerance):
        return 0
    return 1 if value > 0 else -1


def _read_bridge_rows(path: Path) -> tuple[list[dict[str, object]], dict[tuple[str, int], dict[str, object]]]:
    rows: list[dict[str, object]] = []
    by_key: dict[tuple[str, int], dict[str, object]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            row: dict[str, object] = dict(raw)
            row["h"] = int(str(raw["h"]))
            for name in _BRIDGE_NUMERIC_COLUMNS:
                row[name] = float(str(raw[name]))
            key = (str(row["scenario_id"]), int(row["h"]))
            rows.append(row)
            by_key[key] = row
    return rows, by_key


def _comparison_specs() -> list[_BridgeComparisonSpec]:
    paths = repo_paths()
    specs: list[_BridgeComparisonSpec] = []
    for item in _DEFAULT_COMPARISON_SPECS:
        specs.append(
            _BridgeComparisonSpec(
                comparison_id=str(item["comparison_id"]),
                left_id=str(item["left_id"]),
                right_id=str(item["right_id"]),
                left_dir=paths.repo_root / str(item["left_dir"]),
                right_dir=paths.repo_root / str(item["right_dir"]),
            )
        )
    return specs


def _bridge_surface_payload(root: Path) -> dict[str, object]:
    return {
        "root": str(root.relative_to(repo_paths().repo_root)),
        "bridge_results_path": str((root / "bridge_results.csv").relative_to(repo_paths().repo_root)),
        "bridge_metadata_path": str((root / "bridge_metadata.json").relative_to(repo_paths().repo_root)),
        "bridge_export_report_path": str((root / "bridge_export_report.json").relative_to(repo_paths().repo_root)),
        "bridge_metadata": _load_json(root / "bridge_metadata.json"),
        "bridge_export_report": _load_json(root / "bridge_export_report.json"),
    }


def _compare_bridge_surfaces(spec: _BridgeComparisonSpec) -> dict[str, object]:
    left_surface = _bridge_surface_payload(spec.left_dir)
    right_surface = _bridge_surface_payload(spec.right_dir)
    left_rows, left_by_key = _read_bridge_rows(spec.left_dir / "bridge_results.csv")
    right_rows, right_by_key = _read_bridge_rows(spec.right_dir / "bridge_results.csv")

    left_keys = set(left_by_key)
    right_keys = set(right_by_key)
    shared_keys = sorted(left_keys & right_keys)
    left_only = sorted(left_keys - right_keys)
    right_only = sorted(right_keys - left_keys)

    row_differences: list[dict[str, object]] = []
    family_summary: dict[str, dict[str, object]] = {}
    identical = True

    for key in shared_keys:
        left_row = left_by_key[key]
        right_row = right_by_key[key]
        metric_differences: list[dict[str, object]] = []
        max_abs_diff = 0.0
        for name in _BRIDGE_NUMERIC_COLUMNS:
            left_value = float(left_row[name])
            right_value = float(right_row[name])
            abs_diff = abs(left_value - right_value)
            sign_flip = _sign(left_value) != _sign(right_value) and _sign(left_value) != 0 and _sign(right_value) != 0
            if abs_diff > _DIFF_TOLERANCE:
                identical = False
                metric_differences.append(
                    {
                        "metric": name,
                        "left_value": left_value,
                        "right_value": right_value,
                        "abs_diff": abs_diff,
                        "sign_flip": sign_flip,
                    }
                )
                max_abs_diff = max(max_abs_diff, abs_diff)
        if not metric_differences:
            continue
        row_record = {
            "family": left_row["family"],
            "channel": left_row["channel"],
            "scenario_id": left_row["scenario_id"],
            "scenario_label": left_row["scenario_label"],
            "h": left_row["h"],
            "max_abs_diff": max_abs_diff,
            "metric_differences": metric_differences,
        }
        row_differences.append(row_record)
        family_id = str(left_row["family"])
        summary = family_summary.setdefault(
            family_id,
            {
                "row_difference_count": 0,
                "scenario_ids": [],
                "max_abs_diff": 0.0,
                "metrics_with_differences": [],
                "sign_flip_metrics": [],
            },
        )
        summary["row_difference_count"] = int(summary["row_difference_count"]) + 1
        if row_record["scenario_id"] not in summary["scenario_ids"]:
            summary["scenario_ids"].append(row_record["scenario_id"])
        summary["max_abs_diff"] = max(float(summary["max_abs_diff"]), max_abs_diff)
        for metric in metric_differences:
            metric_name = str(metric["metric"])
            if metric_name not in summary["metrics_with_differences"]:
                summary["metrics_with_differences"].append(metric_name)
            if metric["sign_flip"] and metric_name not in summary["sign_flip_metrics"]:
                summary["sign_flip_metrics"].append(metric_name)

    return {
        "comparison_id": spec.comparison_id,
        "left_id": spec.left_id,
        "right_id": spec.right_id,
        "left_surface": left_surface,
        "right_surface": right_surface,
        "row_count_left": len(left_rows),
        "row_count_right": len(right_rows),
        "shared_row_count": len(shared_keys),
        "left_only_rows": [{"scenario_id": key[0], "h": key[1]} for key in left_only],
        "right_only_rows": [{"scenario_id": key[0], "h": key[1]} for key in right_only],
        "identical_shared_rows": identical and not left_only and not right_only,
        "row_differences": row_differences,
        "family_summary": family_summary,
    }


def _inventory_markdown(payload: dict[str, object]) -> str:
    lines = [
        "# Phase 1 Bridge Discrepancy Inventory",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Scope: `{payload['scope']}`",
        "",
    ]
    for comparison in payload["comparisons"]:
        lines.extend(
            [
                f"## {comparison['comparison_id']}",
                "",
                f"- Left: `{comparison['left_id']}` -> `{comparison['left_surface']['bridge_results_path']}`",
                f"- Right: `{comparison['right_id']}` -> `{comparison['right_surface']['bridge_results_path']}`",
                f"- Shared rows: `{comparison['shared_row_count']}`",
                f"- Identical shared rows: `{str(comparison['identical_shared_rows']).lower()}`",
                "",
            ]
        )
        if comparison["left_only_rows"] or comparison["right_only_rows"]:
            lines.append("- Non-overlapping rows detected.")
            if comparison["left_only_rows"]:
                lines.append(f"  - Left only: `{len(comparison['left_only_rows'])}`")
            if comparison["right_only_rows"]:
                lines.append(f"  - Right only: `{len(comparison['right_only_rows'])}`")
            lines.append("")
        family_summary = comparison["family_summary"]
        if not family_summary:
            lines.append("- No differing rows.")
            lines.append("")
            continue
        lines.append("| Family | Diff rows | Max abs diff | Metrics | Sign flips |")
        lines.append("| --- | ---: | ---: | --- | --- |")
        for family_id in sorted(family_summary):
            summary = family_summary[family_id]
            lines.append(
                "| "
                + " | ".join(
                    [
                        family_id,
                        str(summary["row_difference_count"]),
                        f"{float(summary['max_abs_diff']):.6g}",
                        ", ".join(summary["metrics_with_differences"]) or "none",
                        ", ".join(summary["sign_flip_metrics"]) or "none",
                    ]
                )
                + " |"
            )
        lines.append("")
        lines.append("### First differing rows")
        lines.append("")
        for row in comparison["row_differences"][:12]:
            diffs = ", ".join(
                f"{item['metric']}={item['left_value']:.6g}->{item['right_value']:.6g}"
                for item in row["metric_differences"][:4]
            )
            lines.append(
                f"- `{row['scenario_id']}` h={row['h']} family=`{row['family']}` max_abs_diff=`{row['max_abs_diff']:.6g}` {diffs}"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_phase1_bridge_discrepancy_inventory(
    *,
    out_json_path: Path | None = None,
    out_md_path: Path | None = None,
) -> dict[str, object]:
    paths = repo_paths()
    reports_root = paths.repo_root / "reports"
    out_json_path = out_json_path or (reports_root / "phase1_bridge_discrepancy_inventory.json")
    out_md_path = out_md_path or (reports_root / "phase1_bridge_discrepancy_inventory.md")

    comparisons = [_compare_bridge_surfaces(spec) for spec in _comparison_specs()]
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "scope": "tracked phase1 bridge surfaces",
        "comparisons": comparisons,
    }
    out_json_path.parent.mkdir(parents=True, exist_ok=True)
    out_md_path.parent.mkdir(parents=True, exist_ok=True)
    out_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    out_md_path.write_text(_inventory_markdown(payload), encoding="utf-8")
    return {
        "report_path": str(out_json_path),
        "markdown_path": str(out_md_path),
        "comparison_ids": [item["comparison_id"] for item in comparisons],
        "comparisons": comparisons,
    }
