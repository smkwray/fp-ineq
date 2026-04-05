from __future__ import annotations

import json

from .paths import repo_paths

__all__ = ["assess_phase2_wealth_maturity"]


_WEALTH_MIN_EFFECTIVE_NOBS = 10.0
_WEALTH_MAX_LOO_RMSE_MEAN = 0.08
_WEALTH_MIN_SIGN_STABILITY = 8
_WEALTH_MIN_MAX_ABS_SCENARIO_DELTA = 0.01
_WEALTH_PUBLIC_SCENARIO_PROBES = (
    "ui-relief",
    "ui-shock",
    "transfer-package-relief",
    "transfer-package-shock",
    "transfer-composite-medium",
)


def _wealth_maturity_assessment(
    coefficient_report: dict[str, object],
    run_report: dict[str, object],
) -> dict[str, object]:
    wealth_eq = dict(dict(coefficient_report.get("equations", {})).get("IWGAP150", {}))
    if not wealth_eq:
        raise KeyError("IWGAP150 equation missing from distribution coefficient report")

    loo = dict(wealth_eq.get("loo", {}))
    sign_stability = {str(name): int(value) for name, value in dict(loo.get("sign_stability", {})).items()}
    min_sign_stability = min(sign_stability.values()) if sign_stability else 0
    diagnostics = {
        "effective_nobs": float(wealth_eq.get("effective_nobs", 0.0)),
        "main_rmse": float(wealth_eq.get("main_rmse", 0.0)),
        "loo_rmse_mean": float(loo.get("rmse_mean", 0.0)),
        "loo_rmse_std": float(loo.get("rmse_std", 0.0)),
        "restricted_regressor": str(dict(wealth_eq.get("benchmarks", {})).get("restricted_regressor", "")),
        "sign_stability": sign_stability,
        "min_sign_stability": int(min_sign_stability),
    }
    diagnostics_ready = (
        diagnostics["effective_nobs"] >= _WEALTH_MIN_EFFECTIVE_NOBS
        and diagnostics["loo_rmse_mean"] <= _WEALTH_MAX_LOO_RMSE_MEAN
        and diagnostics["min_sign_stability"] >= _WEALTH_MIN_SIGN_STABILITY
    )

    scenarios = dict(run_report.get("scenarios", {}))
    baseline_last = dict(dict(scenarios.get("baseline-observed", {})).get("last_levels", {}))
    baseline_value = baseline_last.get("IWGAP150")
    if baseline_value is None:
        raise KeyError("Baseline IWGAP150 missing from distribution run report")

    scenario_deltas: dict[str, float] = {}
    for variant_id in _WEALTH_PUBLIC_SCENARIO_PROBES:
        variant_last = dict(dict(scenarios.get(variant_id, {})).get("last_levels", {}))
        if variant_last.get("IWGAP150") is None:
            continue
        scenario_deltas[variant_id] = float(variant_last["IWGAP150"]) - float(baseline_value)

    max_abs_scenario_delta = max((abs(value) for value in scenario_deltas.values()), default=0.0)
    response_signal_ready = max_abs_scenario_delta >= _WEALTH_MIN_MAX_ABS_SCENARIO_DELTA

    structural_status = {
        "public_family_ready": False,
        "expert_only_candidate": diagnostics_ready and response_signal_ready,
        "public_family_blocker": (
            "Wealth/home-equity transmission family is still deferred; no dedicated public wealth-family "
            "baseline or wealth shock family has been built yet."
        ),
    }
    recommendation = (
        "candidate_for_expert_only_preset_keep_public_wealth_family_deferred"
        if structural_status["expert_only_candidate"]
        else "keep_private_shadow_until_wealth_family_is_built"
    )
    return {
        "diagnostics": diagnostics,
        "response_signal": {
            "scenario_deltas": scenario_deltas,
            "max_abs_scenario_delta": float(max_abs_scenario_delta),
            "threshold": float(_WEALTH_MIN_MAX_ABS_SCENARIO_DELTA),
            "passes": response_signal_ready,
        },
        "structural_status": structural_status,
        "recommendation": recommendation,
    }


def assess_phase2_wealth_maturity() -> dict[str, object]:
    paths = repo_paths()
    coefficient_report_path = paths.runtime_distribution_reports_root / "estimate_phase1_distribution_coefficients.json"
    run_report_path = paths.runtime_distribution_reports_root / "run_phase1_distribution_block.json"
    if not coefficient_report_path.exists():
        raise FileNotFoundError(
            f"Wealth maturity assessment requires coefficient report: {coefficient_report_path}"
        )
    if not run_report_path.exists():
        raise FileNotFoundError(
            f"Wealth maturity assessment requires distribution run report: {run_report_path}"
        )
    coefficient_report = json.loads(coefficient_report_path.read_text(encoding="utf-8"))
    run_report = json.loads(run_report_path.read_text(encoding="utf-8"))
    assessment = _wealth_maturity_assessment(coefficient_report, run_report)
    payload = {
        "coefficient_report_path": str(coefficient_report_path),
        "run_report_path": str(run_report_path),
        "assessment": assessment,
    }
    report_path = paths.runtime_distribution_reports_root / "assess_phase2_wealth_maturity.json"
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "recommendation": str(assessment["recommendation"]),
        "expert_only_candidate": bool(dict(assessment["structural_status"]).get("expert_only_candidate")),
        "public_family_ready": bool(dict(assessment["structural_status"]).get("public_family_ready")),
    }
