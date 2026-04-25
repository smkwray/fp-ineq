from __future__ import annotations

from pathlib import Path

import typer

from .bridge import locate_fp_home
from .canonical_freeze import assess_phase1_canonical_freeze
from .data_pipeline import refresh_data
from .discrepancy_inventory import build_phase1_bridge_discrepancy_inventory
from .export import export_phase1_bridge_artifacts, export_phase1_full_bundle, publish_phase1_bundle_to_docs
from .paths import repo_paths
from .phase1_contrary_audit import assess_phase1_contrary_channels
from .phase1_distribution_block import (
    analyze_phase1_distribution_canonical_blocker_traces,
    analyze_phase1_distribution_canonical_solved_path,
    assess_phase1_distribution_canonical_parity,
    assess_phase1_distribution_backend_boundary,
    analyze_phase1_distribution_driver_gap,
    analyze_phase1_distribution_first_levels,
    analyze_phase1_distribution_policy_gap,
    analyze_phase1_distribution_transfer_macro_block,
    analyze_phase1_distribution_ui_attenuation,
    _tagged_report_path,
    _tagged_runtime_dir,
    build_phase1_distribution_overlay,
    compare_phase1_distribution_backends,
    run_phase1_distribution_block,
    validate_phase1_distribution_identities,
    write_phase1_distribution_scenarios,
)
from .phase1_distribution_interventions import (
    assess_phase1_distribution_generalization_readiness,
    assess_phase1_distribution_family_generalization,
    compose_phase1_distribution_package_evidence,
    assess_phase1_distribution_package_readiness,
    assess_phase1_distribution_intervention_ladder_selection,
    run_phase1_distribution_family_holdout,
    run_phase1_distribution_intervention_experiment,
    run_phase1_distribution_intervention_ladder,
)
from .phase1_transfer_core import (
    run_phase1_transfer_composite_ladder,
    run_phase1_transfer_core,
    write_phase1_transfer_scenarios,
)
from .phase1_ui import (
    build_phase1_private_overlay,
    run_phase1_ui_ladder,
    run_phase1_ui_prototype,
    write_phase1_ui_scenarios,
)
from .phase2_credit import (
    build_phase2_credit_overlay,
    run_phase2_credit,
    run_phase2_credit_scale_sweep,
    write_phase2_credit_scenarios,
)
from .phase2_ui_offset import (
    build_phase2_ui_offset_overlay,
    run_phase2_ui_offset_envelope,
    run_phase2_ui_offset,
    write_phase2_ui_offset_scenarios,
)
from .phase2_wealth import assess_phase2_wealth_maturity

app = typer.Typer(help="Public inequality overlay toolkit for the stock Fair model.")


@app.command("refresh-data")
def refresh_data_cmd() -> None:
    payload = refresh_data()
    typer.echo(f"refreshed series={payload['series_count']} through {payload['period_end']}")


@app.command("assess-phase1-canonical-freeze")
def assess_phase1_canonical_freeze_cmd(
    remote_ref: str = typer.Option(
        "origin/main",
        "--remote-ref",
        help="Canonical public git ref to compare the local repo against",
    ),
    report_path: Path = typer.Option(
        None,
        "--report-path",
        help="Optional output path for the canonical-freeze report",
    ),
) -> None:
    payload = assess_phase1_canonical_freeze(
        remote_ref=remote_ref,
        report_path=report_path,
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"catalog_preserved={str(payload['canonical_distribution_catalog_preserved']).lower()}",
                f"transfer_core_preserved={str(payload['canonical_transfer_core_preserved']).lower()}",
                f"experimental_specs_present={str(payload['experimental_intervention_specs_present']).lower()}",
                f"experimental_artifacts_present={str(payload['experimental_intervention_artifacts_present']).lower()}",
            ]
        )
    )


@app.command("assess-phase1-distribution-canonical-parity")
def assess_phase1_distribution_canonical_parity_cmd(
    freeze_report_path: Path = typer.Option(
        None,
        "--freeze-report-path",
        help="Optional canonical-freeze report override",
    ),
    compare_report_path: Path = typer.Option(
        None,
        "--compare-report-path",
        help="Optional compare_phase1_distribution_backends report override",
    ),
    first_levels_report_path: Path = typer.Option(
        None,
        "--first-levels-report-path",
        help="Optional analyze_phase1_distribution_first_levels report override",
    ),
    backend_boundary_report_path: Path = typer.Option(
        None,
        "--backend-boundary-report-path",
        help="Optional assess_phase1_distribution_backend_boundary report override",
    ),
    fp_r_identity_report_path: Path = typer.Option(
        None,
        "--fp-r-identity-report-path",
        help="Optional validate_phase1_distribution_identities.fp-r report override",
    ),
    fpexe_identity_report_path: Path = typer.Option(
        None,
        "--fpexe-identity-report-path",
        help="Optional validate_phase1_distribution_identities.fpexe report override",
    ),
    report_path: Path = typer.Option(
        None,
        "--report-path",
        help="Optional output path for the canonical parity report",
    ),
) -> None:
    payload = assess_phase1_distribution_canonical_parity(
        freeze_report_path=freeze_report_path,
        compare_report_path=compare_report_path,
        first_levels_report_path=first_levels_report_path,
        backend_boundary_report_path=backend_boundary_report_path,
        fp_r_identity_report_path=fp_r_identity_report_path,
        fpexe_identity_report_path=fpexe_identity_report_path,
        report_path=report_path,
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"status={payload['status']}",
                f"canonical_parity_ready={str(payload['canonical_parity_ready']).lower()}",
                f"covered_variants={payload['covered_variant_count']}",
                f"missing_variants={payload['missing_variant_count']}",
            ]
        )
    )


@app.command("compose-phase1-ui")
def compose_phase1_ui_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
) -> None:
    payload = build_phase1_private_overlay(fp_home=locate_fp_home(fp_home))
    written = write_phase1_ui_scenarios(fp_home=locate_fp_home(fp_home))
    typer.echo(f"phase1 overlay -> {payload['overlay_root']} scenarios={len(written)}")


@app.command("run-phase1-ui")
def run_phase1_ui_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
) -> None:
    payload = run_phase1_ui_prototype(fp_home=locate_fp_home(fp_home))
    typer.echo(
        " ".join(
            [
                f"passes={str(payload['passes']).lower()}",
                f"report={payload['report_path']}",
                f"artifacts={payload['artifacts_dir']}",
            ]
        )
    )


@app.command("run-phase1-ui-ladder")
def run_phase1_ui_ladder_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
    prototype_report_path: Path = typer.Option(
        None,
        "--prototype-report-path",
        help="Existing run_phase1_ui.json report to calibrate the ladder from",
    ),
) -> None:
    payload = run_phase1_ui_ladder(
        fp_home=locate_fp_home(fp_home),
        prototype_report_path=prototype_report_path,
    )
    typer.echo(
        " ".join(
            [
                f"passes={str(payload['passes']).lower()}",
                f"passes_targets={str(payload['passes_targets']).lower()}",
                f"report={payload['report_path']}",
                f"artifacts={payload['artifacts_dir']}",
            ]
        )
    )


@app.command("compose-phase1-transfer-core")
def compose_phase1_transfer_core_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
) -> None:
    payload = build_phase1_private_overlay(fp_home=locate_fp_home(fp_home))
    written = write_phase1_transfer_scenarios(fp_home=locate_fp_home(fp_home))
    typer.echo(f"transfer-core overlay -> {payload['overlay_root']} scenarios={len(written)}")


@app.command("run-phase1-transfer-core")
def run_phase1_transfer_core_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
) -> None:
    payload = run_phase1_transfer_core(fp_home=locate_fp_home(fp_home))
    typer.echo(
        " ".join(
            [
                f"passes={str(payload['passes']).lower()}",
                f"report={payload['report_path']}",
                f"artifacts={payload['artifacts_dir']}",
            ]
        )
    )


@app.command("run-phase1-transfer-composite-ladder")
def run_phase1_transfer_composite_ladder_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
) -> None:
    payload = run_phase1_transfer_composite_ladder(
        fp_home=locate_fp_home(fp_home),
    )
    typer.echo(
        " ".join(
            [
                f"passes={str(payload['passes']).lower()}",
                f"report={payload['report_path']}",
                f"artifacts={payload['artifacts_dir']}",
            ]
        )
    )


@app.command("compose-phase1-distribution-block")
def compose_phase1_distribution_block_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
) -> None:
    payload = build_phase1_distribution_overlay(fp_home=locate_fp_home(fp_home))
    written = write_phase1_distribution_scenarios(fp_home=locate_fp_home(fp_home))
    typer.echo(f"distribution-block overlay -> {payload['overlay_root']} scenarios={len(written)}")


@app.command("run-phase1-distribution-block")
def run_phase1_distribution_block_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
    backend: str = typer.Option(
        "fp-r",
        "--backend",
        help="Execution backend for the 14-run distribution block (fpexe, fppy, fp-r, or both)",
    ),
    runtime_tag: str = typer.Option(
        "",
        "--runtime-tag",
        help="Optional suffix for scenarios/artifacts/report paths, e.g. 'fpr'",
    ),
    variant_id: list[str] = typer.Option(
        [],
        "--variant-id",
        help="Optional scenario variants to run instead of the full distribution block",
    ),
    fpr_timeout_seconds: int = typer.Option(
        1200,
        "--fpr-timeout-seconds",
        help="fp-r subprocess timeout in seconds for generated canonical/distribution scenario runs",
    ),
    fpr_exogenous_equation_target_policy: str = typer.Option(
        "",
        "--fpr-exogenous-equation-target-policy",
        help="Optional explicit fp-r exogenous equation target policy override for generated runs",
    ),
    fpr_setupsolve: str = typer.Option(
        "",
        "--fpr-setupsolve",
        help="Optional SETUPSOLVE statement injected into generated fp-r overlay input, e.g. 'SETUPSOLVE RHORESIDAR1=YES RHORESIDSOURCESUFFIX=_OBS TARGETLAGSUFFIX=_OBS;'",
    ),
) -> None:
    paths = repo_paths()
    tag = runtime_tag.strip() or None
    scenario_fpr_additions = None
    if fpr_exogenous_equation_target_policy.strip():
        scenario_fpr_additions = {
            "__all__": {
                "exogenous_equation_target_policy": fpr_exogenous_equation_target_policy.strip()
            }
        }
    payload = run_phase1_distribution_block(
        fp_home=locate_fp_home(fp_home),
        backend=backend,
        scenarios_root=_tagged_runtime_dir(paths.runtime_distribution_scenarios_root, tag),
        artifacts_root=_tagged_runtime_dir(paths.runtime_distribution_artifacts_root, tag),
        overlay_root=_tagged_runtime_dir(paths.runtime_distribution_overlay_root, tag),
        report_path=_tagged_report_path(
            paths.runtime_distribution_reports_root / "run_phase1_distribution_block.json",
            tag,
        ),
        variant_ids=tuple(variant_id) if variant_id else None,
        scenario_fpr_additions=scenario_fpr_additions,
        fpr_timeout_seconds=int(fpr_timeout_seconds),
        fpr_setupsolve_statement=fpr_setupsolve.strip() or None,
    )
    typer.echo(
        " ".join(
            [
                f"backend={payload['backend']}",
                f"passes={str(payload['passes']).lower()}",
                f"report={payload['report_path']}",
                f"artifacts={payload['artifacts_dir']}",
            ]
        )
    )


@app.command("compare-phase1-distribution-backends")
def compare_phase1_distribution_backends_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
    left_backend: str = typer.Option(
        "fp-r",
        "--left-backend",
        help="Primary backend to validate as the main distribution path",
    ),
    right_backend: str = typer.Option(
        "fpexe",
        "--right-backend",
        help="Reference backend for the comparison run",
    ),
    variant_id: list[str] = typer.Option(
        ["baseline-observed", "ui-relief", "transfer-composite-medium"],
        "--variant-id",
        help="Scenario variants to compare across backends",
    ),
    variable: list[str] = typer.Option(
        ["TRLOWZ", "IPOVALL", "IPOVCH", "RYDPC"],
        "--variable",
        help="Variables to compare in the backend validation report",
    ),
    left_fpr_timeout_seconds: int = typer.Option(
        1200,
        "--left-fpr-timeout-seconds",
        help="fp-r subprocess timeout in seconds for the left backend when it is fp-r",
    ),
    right_fpr_timeout_seconds: int = typer.Option(
        1200,
        "--right-fpr-timeout-seconds",
        help="fp-r subprocess timeout in seconds for the right backend when it is fp-r",
    ),
) -> None:
    payload = compare_phase1_distribution_backends(
        fp_home=locate_fp_home(fp_home),
        left_backend=left_backend,
        right_backend=right_backend,
        variant_ids=tuple(variant_id),
        variables=tuple(variable),
        left_fpr_timeout_seconds=int(left_fpr_timeout_seconds),
        right_fpr_timeout_seconds=int(right_fpr_timeout_seconds),
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"variants={payload['variant_count']}",
                f"max_abs_diff={payload['max_abs_diff']}",
            ]
        )
    )


@app.command("validate-phase1-distribution-identities")
def validate_phase1_distribution_identities_cmd(
    fp_home: Path = typer.Option(
        None,
        "--fp-home",
        help="Path to private stock FM directory; only needed if the run report must be generated",
    ),
    backend: str = typer.Option(
        "fp-r",
        "--backend",
        help="Backend whose solved outputs should be validated against the installed policy identities",
    ),
    variant_id: list[str] = typer.Option(
        [],
        "--variant-id",
        help="Optional variants to validate instead of the default UI and transfer-composite set",
    ),
    run_report_path: Path = typer.Option(
        None,
        "--run-report-path",
        help="Existing run_phase1_distribution_block report to validate",
    ),
    max_abs_residual: float = typer.Option(
        1e-6,
        "--max-abs-residual",
        help="Maximum allowed absolute residual for required policy identities",
    ),
    ub_identity_mode: str = typer.Option(
        "scaled",
        "--ub-identity-mode",
        help=(
            "Required UB identity mode: 'scaled' checks UB=EXP(LUB)*UIFAC; "
            "'unscaled' checks UB=EXP(LUB) for transformed-LHS percent-log compatibility runs."
        ),
    ),
    forecast_start: str = typer.Option(
        "",
        "--forecast-start",
        help="Optional validation-window start period override, e.g. 2026.1.",
    ),
    forecast_end: str = typer.Option(
        "",
        "--forecast-end",
        help="Optional validation-window end period override, e.g. 2026.1.",
    ),
    fpr_timeout_seconds: int = typer.Option(
        1200,
        "--fpr-timeout-seconds",
        help="fp-r subprocess timeout in seconds if this command needs to generate the run report",
    ),
) -> None:
    payload = validate_phase1_distribution_identities(
        fp_home=locate_fp_home(fp_home) if fp_home is not None else None,
        backend=backend,
        variant_ids=tuple(variant_id) if variant_id else None,
        run_report_path=run_report_path,
        max_abs_residual=max_abs_residual,
        ub_identity_mode=ub_identity_mode,
        forecast_start=forecast_start.strip() or None,
        forecast_end=forecast_end.strip() or None,
        fpr_timeout_seconds=int(fpr_timeout_seconds),
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"variants={payload['variant_count']}",
                f"passes={str(payload['passes']).lower()}",
                f"max_required_identity_residual={payload['max_required_identity_residual']}",
            ]
        )
    )


@app.command("run-phase1-distribution-intervention")
def run_phase1_distribution_intervention_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
    intervention_spec: Path = typer.Option(
        ...,
        "--intervention-spec",
        help="YAML spec describing the structural intervention to apply",
    ),
    intervention_backend: str = typer.Option(
        "fp-r",
        "--intervention-backend",
        help="Backend to run for the control and intervention arms",
    ),
    reference_backend: str = typer.Option(
        "fpexe",
        "--reference-backend",
        help="Reference backend for the intervention gap-closure comparison",
    ),
    control_run_report_path: Path = typer.Option(
        None,
        "--control-run-report-path",
        help="Existing control-arm run report to reuse instead of rerunning it",
    ),
    reference_run_report_path: Path = typer.Option(
        None,
        "--reference-run-report-path",
        help="Existing reference-arm run report to reuse instead of rerunning it",
    ),
) -> None:
    payload = run_phase1_distribution_intervention_experiment(
        fp_home=locate_fp_home(fp_home),
        intervention_spec_path=intervention_spec,
        intervention_backend=intervention_backend,
        reference_backend=reference_backend,
        control_run_report_path=control_run_report_path,
        reference_run_report_path=reference_run_report_path,
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"intervention_id={payload['intervention_id']}",
                f"gap_improved_count={payload['gap_improved_count']}",
                f"gap_worsened_count={payload['gap_worsened_count']}",
                f"identity_passes={str(payload['intervention_identity_passes']).lower()}",
            ]
        )
    )


@app.command("run-phase1-distribution-family-holdout")
def run_phase1_distribution_family_holdout_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
    intervention_spec: Path = typer.Option(
        ...,
        "--intervention-spec",
        help="Base YAML intervention spec whose fixed coefficients should be tested on holdout variants",
    ),
    family_id: list[str] = typer.Option(
        ...,
        "--family-id",
        help="Distribution family id(s) to resolve holdout variants from; repeat the flag to pass multiple families",
    ),
    exclude_variant_id: list[str] = typer.Option(
        None,
        "--exclude-variant-id",
        help="Variant id(s) to exclude from the resolved family holdout set; repeat the flag to pass multiple values",
    ),
    holdout_variant_id: list[str] = typer.Option(
        None,
        "--holdout-variant-id",
        help="Optional explicit holdout variant ids; if provided these override family resolution",
    ),
    internal_only: bool = typer.Option(
        False,
        "--internal-only",
        help="Skip the legacy reference arm and score holdouts on fp-r intervention-vs-control directionality only",
    ),
    report_tag: str = typer.Option(
        None,
        "--report-tag",
        help="Optional intervention/report tag for the derived holdout run",
    ),
    intervention_backend: str = typer.Option(
        "fp-r",
        "--intervention-backend",
        help="Backend to run for the control and intervention arms",
    ),
    reference_backend: str = typer.Option(
        "fpexe",
        "--reference-backend",
        help="Reference backend for the intervention gap-closure comparison",
    ),
    control_run_report_path: Path = typer.Option(
        None,
        "--control-run-report-path",
        help="Existing control-arm run report to reuse instead of rerunning it",
    ),
    reference_run_report_path: Path = typer.Option(
        None,
        "--reference-run-report-path",
        help="Existing reference-arm run report to reuse instead of rerunning it",
    ),
) -> None:
    payload = run_phase1_distribution_family_holdout(
        fp_home=locate_fp_home(fp_home),
        intervention_spec_path=intervention_spec,
        family_ids=tuple(str(item) for item in family_id),
        exclude_variant_ids=tuple(str(item) for item in exclude_variant_id) if exclude_variant_id else (),
        holdout_variant_ids=tuple(str(item) for item in holdout_variant_id) if holdout_variant_id else None,
        internal_only=internal_only,
        report_tag=report_tag,
        intervention_backend=intervention_backend,
        reference_backend=reference_backend,
        control_run_report_path=control_run_report_path,
        reference_run_report_path=reference_run_report_path,
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"intervention_id={payload['intervention_id']}",
                f"holdout_variants={','.join(payload['holdout_variant_ids'])}",
                (
                    f"directionality_all_pass={str(payload['directionality_all_pass']).lower()}"
                    if internal_only
                    else f"gap_improved_count={payload['gap_improved_count']}"
                ),
                (
                    f"directionality_pass_count={payload['directionality_pass_count']}"
                    if internal_only
                    else f"gap_worsened_count={payload['gap_worsened_count']}"
                ),
                (
                    "identity_passes=na"
                    if internal_only
                    else f"identity_passes={str(payload['intervention_identity_passes']).lower()}"
                ),
            ]
        )
    )


@app.command("run-phase1-distribution-intervention-ladder")
def run_phase1_distribution_intervention_ladder_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
    intervention_spec: Path = typer.Option(
        ...,
        "--intervention-spec",
        help="YAML spec describing the structural intervention ladder to apply",
    ),
    coefficient: list[float] = typer.Option(
        None,
        "--coefficient",
        help="Override ladder coefficients; repeat the flag to pass multiple values",
    ),
    intervention_backend: str = typer.Option(
        "fp-r",
        "--intervention-backend",
        help="Backend to run for the control and intervention arms",
    ),
    reference_backend: str = typer.Option(
        "fpexe",
        "--reference-backend",
        help="Reference backend for the intervention gap-closure comparison",
    ),
    control_run_report_path: Path = typer.Option(
        None,
        "--control-run-report-path",
        help="Existing control-arm run report to reuse instead of rerunning the control surface",
    ),
    reference_run_report_path: Path = typer.Option(
        None,
        "--reference-run-report-path",
        help="Existing reference-arm run report to reuse instead of rerunning the reference surface",
    ),
) -> None:
    payload = run_phase1_distribution_intervention_ladder(
        fp_home=locate_fp_home(fp_home),
        intervention_spec_path=intervention_spec,
        coefficients=tuple(float(item) for item in coefficient) if coefficient else None,
        intervention_backend=intervention_backend,
        reference_backend=reference_backend,
        control_run_report_path=control_run_report_path,
        reference_run_report_path=reference_run_report_path,
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"intervention_id={payload['intervention_id']}",
                f"row_count={payload['row_count']}",
                f"best_coefficient={payload['best_coefficient']}",
                f"best_median_gap_closure_ratio={payload['best_median_gap_closure_ratio']}",
            ]
        )
    )


@app.command("assess-phase1-distribution-intervention-ladder-selection")
def assess_phase1_distribution_intervention_ladder_selection_cmd(
    zero_history_summary_path: Path = typer.Option(
        ...,
        "--zero-history-summary-path",
        help="Summary JSON from a completed intervention ladder run",
    ),
    control_identity_report_path: Path = typer.Option(
        ...,
        "--control-identity-report-path",
        help="validate.control.json for the same ladder family",
    ),
    optional_identity_id: str = typer.Option(
        "ub_unscaled",
        "--optional-identity-id",
        help="Optional identity to use for the drift tradeoff",
    ),
    conservative_control_multiple: float = typer.Option(
        1.25,
        "--conservative-control-multiple",
        help="Maximum multiple of the control optional residual for the conservative recommendation",
    ),
    balanced_control_multiple: float = typer.Option(
        2.0,
        "--balanced-control-multiple",
        help="Maximum multiple of the control optional residual for the balanced recommendation",
    ),
    report_path: Path = typer.Option(
        None,
        "--report-path",
        help="Optional output path for the ladder-selection report",
    ),
) -> None:
    payload = assess_phase1_distribution_intervention_ladder_selection(
        zero_history_summary_path=zero_history_summary_path,
        control_identity_report_path=control_identity_report_path,
        optional_identity_id=optional_identity_id,
        conservative_control_multiple=conservative_control_multiple,
        balanced_control_multiple=balanced_control_multiple,
        report_path=report_path,
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"conservative={payload['conservative_coefficient']}",
                f"balanced={payload['balanced_coefficient']}",
                f"stretch={payload['stretch_coefficient']}",
                f"aggressive={payload['aggressive_coefficient']}",
            ]
        )
    )


@app.command("assess-phase1-distribution-package-readiness")
def assess_phase1_distribution_package_readiness_cmd(
    experiment_report_path: Path = typer.Option(
        ...,
        "--experiment-report-path",
        help="run_phase1_distribution_intervention_experiment.json for the combined package candidate",
    ),
    effect_report_path: Path = typer.Option(
        None,
        "--effect-report-path",
        help="Optional assess_phase1_distribution_intervention_effect.json override",
    ),
    ui_variant_id: str = typer.Option(
        "ui-relief",
        "--ui-variant-id",
        help="Variant id to treat as the repaired UI channel",
    ),
    transfer_variant_id: str = typer.Option(
        "transfer-composite-medium",
        "--transfer-variant-id",
        help="Variant id to treat as the repaired transfer-macro channel",
    ),
    ui_family_report_path: Path = typer.Option(
        None,
        "--ui-family-report-path",
        help="Optional family generalization report for the UI family",
    ),
    transfer_family_report_path: Path = typer.Option(
        None,
        "--transfer-family-report-path",
        help="Optional family generalization report for the transfer-composite family",
    ),
    ui_macro_policy: str = typer.Option(
        "optional",
        "--ui-macro-policy",
        help="Whether the UI PCY/UR macro lane is optional or required for package readiness framing",
    ),
) -> None:
    payload = assess_phase1_distribution_package_readiness(
        experiment_report_path=experiment_report_path,
        effect_report_path=effect_report_path,
        ui_variant_id=ui_variant_id,
        transfer_variant_id=transfer_variant_id,
        ui_family_report_path=ui_family_report_path,
        transfer_family_report_path=transfer_family_report_path,
        ui_macro_policy=ui_macro_policy,
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"status={payload['status']}",
                f"package_internal_coexistence_passes={str(payload['package_internal_coexistence_passes']).lower()}",
                f"replacement_ready={str(payload['replacement_ready']).lower()}",
                f"ui_macro_policy={payload['ui_macro_policy']}",
            ]
        )
    )


@app.command("assess-phase1-distribution-family-generalization")
def assess_phase1_distribution_family_generalization_cmd(
    family_id: str = typer.Option(..., "--family-id", help="Scenario family id to summarize"),
    holdout_report_path: list[Path] = typer.Option(
        ...,
        "--holdout-report-path",
        help="One or more run_phase1_distribution_family_holdout.internal.json reports",
    ),
    report_path: Path | None = typer.Option(
        None,
        "--report-path",
        help="Optional output path for the family generalization report",
    ),
) -> None:
    payload = assess_phase1_distribution_family_generalization(
        family_id=family_id,
        holdout_report_paths=tuple(holdout_report_path),
        report_path=report_path,
    )
    typer.echo(
        " ".join(
            [
                f"family={payload['family_id']}",
                f"status={payload['status']}",
                f"core_all_pass={str(payload['core_all_pass']).lower()}",
                f"optional_all_pass={str(payload['optional_all_pass']).lower()}",
                f"all_pass={str(payload['all_pass']).lower()}",
                f"report={payload['report_path']}",
            ]
        )
    )


@app.command("assess-phase1-distribution-generalization-readiness")
def assess_phase1_distribution_generalization_readiness_cmd(
    ui_family_report_path: Path = typer.Option(
        ...,
        "--ui-family-report-path",
        help="Family generalization report for the UI family",
    ),
    transfer_family_report_path: Path = typer.Option(
        ...,
        "--transfer-family-report-path",
        help="Family generalization report for the transfer-composite family",
    ),
    ui_macro_policy: str = typer.Option(
        "optional",
        "--ui-macro-policy",
        help="Whether the UI PCY/UR macro lane is optional or required for overall generalization",
    ),
    report_path: Path | None = typer.Option(
        None,
        "--report-path",
        help="Optional output path for the overall generalization readiness report",
    ),
) -> None:
    payload = assess_phase1_distribution_generalization_readiness(
        ui_family_report_path=ui_family_report_path,
        transfer_family_report_path=transfer_family_report_path,
        ui_macro_policy=ui_macro_policy,
        report_path=report_path,
    )
    typer.echo(
        " ".join(
            [
                f"status={payload['status']}",
                f"ui_status={payload['ui_status']}",
                f"transfer_status={payload['transfer_status']}",
                f"ui_macro_policy={payload['ui_macro_policy']}",
                f"report={payload['report_path']}",
            ]
        )
    )


@app.command("compose-phase1-distribution-package-evidence")
def compose_phase1_distribution_package_evidence_cmd(
    package_id: str = typer.Option(
        ...,
        "--package-id",
        help="Identifier for the composed package evidence bundle",
    ),
    ui_effect_report_path: Path = typer.Option(
        ...,
        "--ui-effect-report-path",
        help="Completed widened ui-relief assess_phase1_distribution_intervention_effect.json",
    ),
    transfer_effect_report_path: Path = typer.Option(
        ...,
        "--transfer-effect-report-path",
        help="Completed widened transfer assess_phase1_distribution_intervention_effect.json",
    ),
    control_compare_report_path: Path = typer.Option(
        ...,
        "--control-compare-report-path",
        help="compare.control_vs_reference.json to treat as the shared package control surface",
    ),
    control_run_report_path: Path = typer.Option(
        ...,
        "--control-run-report-path",
        help="run_phase1_distribution_block.control.fp-r.json to carry into the package evidence",
    ),
    reference_run_report_path: Path = typer.Option(
        ...,
        "--reference-run-report-path",
        help="run_phase1_distribution_block.reference.fpexe.json to carry into the package evidence",
    ),
    intervention_spec_path: Path = typer.Option(
        ...,
        "--intervention-spec-path",
        help="Package intervention spec path to record in the composed evidence",
    ),
    description: str = typer.Option(
        "",
        "--description",
        help="Optional description override for the composed package experiment record",
    ),
    ui_family_report_path: Path = typer.Option(
        None,
        "--ui-family-report-path",
        help="Optional UI family generalization report to carry into composed package readiness",
    ),
    transfer_family_report_path: Path = typer.Option(
        None,
        "--transfer-family-report-path",
        help="Optional transfer family generalization report to carry into composed package readiness",
    ),
    ui_macro_policy: str = typer.Option(
        "optional",
        "--ui-macro-policy",
        help="Whether the UI PCY/UR macro lane is optional or required for composed package readiness framing",
    ),
    report_dir: Path = typer.Option(
        None,
        "--report-dir",
        help="Optional destination directory for the composed package reports",
    ),
) -> None:
    payload = compose_phase1_distribution_package_evidence(
        package_id=package_id,
        ui_effect_report_path=ui_effect_report_path,
        transfer_effect_report_path=transfer_effect_report_path,
        control_compare_report_path=control_compare_report_path,
        control_run_report_path=control_run_report_path,
        reference_run_report_path=reference_run_report_path,
        intervention_spec_path=intervention_spec_path,
        description=description,
        ui_family_report_path=ui_family_report_path,
        transfer_family_report_path=transfer_family_report_path,
        ui_macro_policy=ui_macro_policy,
        report_dir=report_dir,
    )
    typer.echo(
        " ".join(
            [
                f"report_dir={payload['report_dir']}",
                f"status={payload['status']}",
                f"package_internal_coexistence_passes={str(payload['package_internal_coexistence_passes']).lower()}",
                f"replacement_ready={str(payload['replacement_ready']).lower()}",
                f"ui_macro_policy={payload['ui_macro_policy']}",
            ]
        )
    )


@app.command("analyze-phase1-distribution-policy-gap")
def analyze_phase1_distribution_policy_gap_cmd(
    compare_report_path: Path = typer.Option(
        None,
        "--compare-report-path",
        help="Existing compare_phase1_distribution_backends report to analyze",
    ),
    variant_id: list[str] = typer.Option(
        ["ui-relief", "transfer-composite-medium"],
        "--variant-id",
        help="Policy variants to analyze against backend-specific baselines",
    ),
    variable: list[str] = typer.Option(
        ["LUB", "UB", "TRLOWZ", "IPOVALL", "IPOVCH", "RYDPC"],
        "--variable",
        help="Variables to inspect over the forecast window",
    ),
) -> None:
    payload = analyze_phase1_distribution_policy_gap(
        compare_report_path=compare_report_path,
        variant_ids=tuple(variant_id),
        variables=tuple(variable),
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"variants={payload['variant_count']}",
                f"max_gap_ratio_abs={payload['max_gap_ratio_abs']}",
            ]
        )
    )


@app.command("analyze-phase1-distribution-first-levels")
def analyze_phase1_distribution_first_levels_cmd(
    compare_report_path: Path = typer.Option(
        None,
        "--compare-report-path",
        help="Existing compare_phase1_distribution_backends report to analyze",
    ),
    variant_id: list[str] = typer.Option(
        ["ui-relief", "transfer-composite-medium"],
        "--variant-id",
        help="Policy variants whose first forecast-quarter levels should be compared directly",
    ),
    variable: list[str] = typer.Option(
        [
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
        ],
        "--variable",
        help="Variables to inspect at the first forecast quarter",
    ),
) -> None:
    payload = analyze_phase1_distribution_first_levels(
        compare_report_path=compare_report_path,
        variant_ids=tuple(variant_id),
        variables=tuple(variable),
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"variants={payload['variant_count']}",
                f"max_level_abs_diff={payload['max_level_abs_diff']}",
                f"max_delta_abs_diff={payload['max_delta_abs_diff']}",
            ]
        )
    )


@app.command("analyze-phase1-distribution-driver-gap")
def analyze_phase1_distribution_driver_gap_cmd(
    compare_report_path: Path = typer.Option(
        None,
        "--compare-report-path",
        help="Existing compare_phase1_distribution_backends report to analyze",
    ),
    ui_experiment_series_path: Path = typer.Option(
        None,
        "--ui-experiment-series-path",
        help="Patched retained-target fp-r ui-relief fp_r_series.csv to compare against the old controlled fp-r run",
    ),
    ui_variant_id: str = typer.Option(
        "ui-relief",
        "--ui-variant-id",
        help="UI variant to compare against the retained-target experiment",
    ),
    transfer_variant_id: str = typer.Option(
        "transfer-composite-medium",
        "--transfer-variant-id",
        help="Transfer-composite variant to inspect for the RYDPC income-path gap",
    ),
) -> None:
    payload = analyze_phase1_distribution_driver_gap(
        compare_report_path=compare_report_path,
        ui_experiment_series_path=ui_experiment_series_path,
        ui_variant_id=ui_variant_id,
        transfer_variant_id=transfer_variant_id,
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"ui_retained_target_lub_moves={str(payload['ui_retained_target_lub_moves']).lower()}",
                f"transfer_rydpc_negative={str(payload['transfer_rydpc_negative']).lower()}",
            ]
        )
    )


@app.command("analyze-phase1-distribution-canonical-blocker-traces")
def analyze_phase1_distribution_canonical_blocker_traces_cmd(
    baseline_run_report_path: Path = typer.Option(
        None,
        "--baseline-run-report-path",
        help="Canonical baseline fp-r run report",
    ),
    ui_relief_run_report_path: Path = typer.Option(
        None,
        "--ui-relief-run-report-path",
        help="Canonical ui-relief fp-r run report",
    ),
    ui_shock_run_report_path: Path = typer.Option(
        None,
        "--ui-shock-run-report-path",
        help="Canonical ui-shock fp-r run report",
    ),
    transfer_medium_run_report_path: Path = typer.Option(
        None,
        "--transfer-medium-run-report-path",
        help="Canonical transfer-composite-medium fp-r run report",
    ),
    first_levels_report_path: Path = typer.Option(
        None,
        "--first-levels-report-path",
        help="Existing clean canonical first-level analysis report",
    ),
    ui_relief_exclude_run_report_path: Path = typer.Option(
        None,
        "--ui-relief-exclude-run-report-path",
        help="Canonical ui-relief fp-r exclude_from_solve run report",
    ),
    ui_shock_exclude_run_report_path: Path = typer.Option(
        None,
        "--ui-shock-exclude-run-report-path",
        help="Canonical ui-shock fp-r exclude_from_solve run report",
    ),
    transfer_medium_exclude_run_report_path: Path = typer.Option(
        None,
        "--transfer-medium-exclude-run-report-path",
        help="Canonical transfer-composite-medium fp-r exclude_from_solve run report",
    ),
    ui_relief_exclude_first_levels_report_path: Path = typer.Option(
        None,
        "--ui-relief-exclude-first-levels-report-path",
        help="Clean ui-relief exclude_from_solve first-level analysis report",
    ),
    ui_shock_exclude_first_levels_report_path: Path = typer.Option(
        None,
        "--ui-shock-exclude-first-levels-report-path",
        help="Clean ui-shock exclude_from_solve first-level analysis report",
    ),
    transfer_medium_exclude_first_levels_report_path: Path = typer.Option(
        None,
        "--transfer-medium-exclude-first-levels-report-path",
        help="Clean transfer-composite-medium exclude_from_solve first-level analysis report",
    ),
    period: str = typer.Option(
        "2026.1",
        "--period",
        help="Forecast period to trace inside the blocker artifacts",
    ),
    report_path: Path = typer.Option(
        None,
        "--report-path",
        help="Optional output path for the canonical blocker trace report",
    ),
) -> None:
    payload = analyze_phase1_distribution_canonical_blocker_traces(
        baseline_run_report_path=baseline_run_report_path,
        ui_relief_run_report_path=ui_relief_run_report_path,
        ui_shock_run_report_path=ui_shock_run_report_path,
        transfer_medium_run_report_path=transfer_medium_run_report_path,
        first_levels_report_path=first_levels_report_path,
        ui_relief_exclude_run_report_path=ui_relief_exclude_run_report_path,
        ui_shock_exclude_run_report_path=ui_shock_exclude_run_report_path,
        transfer_medium_exclude_run_report_path=transfer_medium_exclude_run_report_path,
        ui_relief_exclude_first_levels_report_path=ui_relief_exclude_first_levels_report_path,
        ui_shock_exclude_first_levels_report_path=ui_shock_exclude_first_levels_report_path,
        transfer_medium_exclude_first_levels_report_path=transfer_medium_exclude_first_levels_report_path,
        period=period,
        report_path=report_path,
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"period={payload['period']}",
                f"ui_relief_has_uifac_reference={str(payload['ui_relief_has_uifac_reference']).lower()}",
                f"ui_shock_has_uifac_reference={str(payload['ui_shock_has_uifac_reference']).lower()}",
                f"transfer_pre_solve_changed={len(payload['transfer_pre_solve_changed_variables'])}",
                f"transfer_intgz_policy_branch_identical={str(payload['transfer_intgz_policy_branch_identical']).lower()}",
                f"transfer_ljf1_policy_branch_identical={str(payload['transfer_ljf1_policy_branch_identical']).lower()}",
            ]
        )
    )


@app.command("analyze-phase1-distribution-canonical-solved-path")
def analyze_phase1_distribution_canonical_solved_path_cmd(
    baseline_run_report_path: Path = typer.Option(
        None,
        "--baseline-run-report-path",
        help="Canonical baseline fp-r run report",
    ),
    ui_relief_run_report_path: Path = typer.Option(
        None,
        "--ui-relief-run-report-path",
        help="Canonical ui-relief fp-r run report",
    ),
    ui_shock_run_report_path: Path = typer.Option(
        None,
        "--ui-shock-run-report-path",
        help="Canonical ui-shock fp-r run report",
    ),
    transfer_medium_run_report_path: Path = typer.Option(
        None,
        "--transfer-medium-run-report-path",
        help="Canonical transfer-composite-medium fp-r run report",
    ),
    first_levels_report_path: Path = typer.Option(
        None,
        "--first-levels-report-path",
        help="Existing clean canonical first-level analysis report",
    ),
    period: str = typer.Option(
        "2026.1",
        "--period",
        help="Forecast period to inspect inside the solved-path traces",
    ),
    report_path: Path = typer.Option(
        None,
        "--report-path",
        help="Optional output path for the canonical solved-path report",
    ),
) -> None:
    payload = analyze_phase1_distribution_canonical_solved_path(
        baseline_run_report_path=baseline_run_report_path,
        ui_relief_run_report_path=ui_relief_run_report_path,
        ui_shock_run_report_path=ui_shock_run_report_path,
        transfer_medium_run_report_path=transfer_medium_run_report_path,
        first_levels_report_path=first_levels_report_path,
        period=period,
        report_path=report_path,
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"period={payload['period']}",
                f"ui_relief_inside_solve={len(payload['ui_relief_inside_solve_variables'])}",
                f"ui_shock_inside_solve={len(payload['ui_shock_inside_solve_variables'])}",
                f"transfer_inside_solve={len(payload['transfer_inside_solve_variables'])}",
            ]
        )
    )


@app.command("compose-phase2-credit")
def compose_phase2_credit_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
) -> None:
    payload = build_phase2_credit_overlay(fp_home=locate_fp_home(fp_home))
    written = write_phase2_credit_scenarios(fp_home=locate_fp_home(fp_home))
    typer.echo(f"phase2 credit overlay -> {payload['overlay_root']} scenarios={len(written)}")


@app.command("run-phase2-credit")
def run_phase2_credit_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
) -> None:
    payload = run_phase2_credit(fp_home=locate_fp_home(fp_home))
    typer.echo(
        " ".join(
            [
                f"passes={str(payload['passes']).lower()}",
                f"report={payload['report_path']}",
                f"artifacts={payload['artifacts_dir']}",
            ]
        )
    )


@app.command("run-phase2-credit-scale-sweep")
def run_phase2_credit_scale_sweep_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
    magnitude: list[float] = typer.Option(
        [1.0, 5.0, 10.0],
        "--magnitude",
        help="Absolute CRWEDGE magnitudes to probe for private credit-family scaling",
    ),
) -> None:
    payload = run_phase2_credit_scale_sweep(
        fp_home=locate_fp_home(fp_home),
        magnitudes=tuple(magnitude),
    )
    typer.echo(
        " ".join(
            [
                f"publication_ready={str(payload['publication_ready']).lower()}",
                f"recommended_action={payload['recommended_action']}",
                f"report={payload['report_path']}",
            ]
        )
    )


@app.command("compose-phase2-ui-offset")
def compose_phase2_ui_offset_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
) -> None:
    payload = build_phase2_ui_offset_overlay(fp_home=locate_fp_home(fp_home))
    written = write_phase2_ui_offset_scenarios(fp_home=locate_fp_home(fp_home))
    typer.echo(f"phase2 ui-offset overlay -> {payload['overlay_root']} scenarios={len(written)}")


@app.command("run-phase2-ui-offset")
def run_phase2_ui_offset_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
    reference_report_path: Path = typer.Option(
        None,
        "--reference-report-path",
        help="Existing phase-1 distribution-block report used to validate the neutral/no-offset matches",
    ),
    target_clawback_share: float = typer.Option(
        0.25,
        "--target-clawback-share",
        help="First-year UR clawback share to target for the private UI offset run",
    ),
) -> None:
    payload = run_phase2_ui_offset(
        fp_home=locate_fp_home(fp_home),
        reference_report_path=reference_report_path,
        target_clawback_share=target_clawback_share,
    )
    typer.echo(
        " ".join(
            [
                f"passes={str(payload['passes']).lower()}",
                f"report={payload['report_path']}",
                f"artifacts={payload['artifacts_dir']}",
            ]
        )
    )


@app.command("run-phase2-ui-offset-envelope")
def run_phase2_ui_offset_envelope_cmd(
    fp_home: Path = typer.Option(..., "--fp-home", help="Path to private stock FM directory"),
    reference_report_path: Path = typer.Option(
        None,
        "--reference-report-path",
        help="Existing phase-1 distribution-block report used to validate the neutral/no-offset matches",
    ),
) -> None:
    payload = run_phase2_ui_offset_envelope(
        fp_home=locate_fp_home(fp_home),
        reference_report_path=reference_report_path,
    )
    typer.echo(
        " ".join(
            [
                f"passes={str(payload['passes']).lower()}",
                f"report={payload['report_path']}",
            ]
        )
    )


@app.command("assess-phase1-distribution-backend-boundary")
def assess_phase1_distribution_backend_boundary_cmd(
    first_levels_report_path: Path = typer.Option(
        None,
        "--first-levels-report-path",
        help="Existing analyze_phase1_distribution_first_levels report to assess",
    ),
    identity_report_path: Path = typer.Option(
        None,
        "--identity-report-path",
        help="Existing validate_phase1_distribution_identities report to assess",
    ),
    driver_gap_report_path: Path = typer.Option(
        None,
        "--driver-gap-report-path",
        help="Existing analyze_phase1_distribution_driver_gap report to assess",
    ),
) -> None:
    payload = assess_phase1_distribution_backend_boundary(
        first_levels_report_path=first_levels_report_path,
        identity_report_path=identity_report_path,
        driver_gap_report_path=driver_gap_report_path,
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"replacement_readiness={payload['replacement_readiness']}",
                f"identity_surface_passes={str(payload['identity_surface_passes']).lower()}",
            ]
        )
    )


@app.command("analyze-phase1-distribution-ui-attenuation")
def analyze_phase1_distribution_ui_attenuation_cmd(
    policy_gap_report_path: Path = typer.Option(
        None,
        "--policy-gap-report-path",
        help="Existing analyze_phase1_distribution_policy_gap report to assess",
    ),
    driver_gap_report_path: Path = typer.Option(
        None,
        "--driver-gap-report-path",
        help="Existing analyze_phase1_distribution_driver_gap report to assess",
    ),
) -> None:
    payload = analyze_phase1_distribution_ui_attenuation(
        policy_gap_report_path=policy_gap_report_path,
        driver_gap_report_path=driver_gap_report_path,
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"assessment={payload['assessment']}",
                f"core_median_gap_ratio_abs={payload['core_median_gap_ratio_abs']}",
            ]
        )
    )


@app.command("analyze-phase1-distribution-transfer-macro-block")
def analyze_phase1_distribution_transfer_macro_block_cmd(
    boundary_report_path: Path = typer.Option(
        None,
        "--boundary-report-path",
        help="Existing assess_phase1_distribution_backend_boundary report to assess",
    ),
    driver_gap_report_path: Path = typer.Option(
        None,
        "--driver-gap-report-path",
        help="Existing analyze_phase1_distribution_driver_gap report to assess",
    ),
) -> None:
    payload = analyze_phase1_distribution_transfer_macro_block(
        boundary_report_path=boundary_report_path,
        driver_gap_report_path=driver_gap_report_path,
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"assessment={payload['assessment']}",
                f"macro_sign_mismatch_count={payload['macro_sign_mismatch_count']}",
            ]
        )
    )


@app.command("assess-phase1-contrary-channels")
def assess_phase1_contrary_channels_cmd(
    report_path: Path = typer.Option(
        None,
        "--report-path",
        help="Existing phase-1 distribution-block report to audit",
    ),
    ui_offset_report_path: Path = typer.Option(
        None,
        "--ui-offset-report-path",
        help="Optional private UI-offset report used as context in the contrary-channel audit",
    ),
) -> None:
    payload = assess_phase1_contrary_channels(
        report_path=report_path,
        ui_offset_report_path=ui_offset_report_path,
    )
    typer.echo(f"phase1 contrary-channel audit -> {payload['report_path']}")


@app.command("assess-phase2-wealth-maturity")
def assess_phase2_wealth_maturity_cmd() -> None:
    payload = assess_phase2_wealth_maturity()
    typer.echo(
        " ".join(
            [
                f"public_family_ready={str(payload['public_family_ready']).lower()}",
                f"expert_only_candidate={str(payload['expert_only_candidate']).lower()}",
                f"recommendation={payload['recommendation']}",
                f"report={payload['report_path']}",
            ]
        )
    )


@app.command("export-phase1-full")
def export_phase1_full_cmd(
    report_path: Path = typer.Option(
        None,
        "--report-path",
        help="Accepted phase-1 distribution-block report to export from",
    ),
    out_dir: Path = typer.Option(
        None,
        "--out-dir",
        help="Output directory for the broad solved-output bundle",
    ),
    family_maturity: list[str] = typer.Option(
        ["public"],
        "--family-maturity",
        help="Include only families with these maturity tags",
    ),
    family_id: list[str] = typer.Option(
        [],
        "--family-id",
        help="Optional family ids to export; defaults to all families in the selected maturity tier(s)",
    ),
) -> None:
    payload = export_phase1_full_bundle(
        report_path=report_path,
        out_dir=out_dir,
        family_maturities=tuple(family_maturity),
        family_ids=tuple(family_id) if family_id else None,
    )
    typer.echo(f"phase1 full bundle -> {payload['out_dir']}")


@app.command("export-phase1-bridge")
def export_phase1_bridge_cmd(
    report_path: Path = typer.Option(
        None,
        "--report-path",
        help="Accepted phase-1 distribution-block report to export bridge rows from",
    ),
    out_dir: Path = typer.Option(
        None,
        "--out-dir",
        help="Output directory for tracked bridge artifacts",
    ),
    family_maturity: list[str] = typer.Option(
        ["public"],
        "--family-maturity",
        help="Include only families with these maturity tags",
    ),
    family_id: list[str] = typer.Option(
        [],
        "--family-id",
        help="Optional family ids to export; defaults to all families in the selected maturity tier(s)",
    ),
) -> None:
    payload = export_phase1_bridge_artifacts(
        report_path=report_path,
        out_dir=out_dir,
        family_maturities=tuple(family_maturity),
        family_ids=tuple(family_id) if family_id else None,
    )
    typer.echo(
        " ".join(
            [
                f"bridge={payload['bridge_results_path']}",
                f"metadata={payload['bridge_metadata_path']}",
                f"rows={payload['bridge_row_count']}",
            ]
        )
    )


@app.command("publish-phase1-full")
def publish_phase1_full_cmd(
    source_dir: Path = typer.Option(
        None,
        "--source-dir",
        help="Solved phase-1 bundle directory to publish from",
    ),
    docs_dir: Path = typer.Option(
        None,
        "--docs-dir",
        help="Docs directory to publish into",
    ),
) -> None:
    payload = publish_phase1_bundle_to_docs(
        source_dir=source_dir,
        docs_dir=docs_dir,
    )
    typer.echo(
        " ".join(
            [
                f"docs={payload['docs_dir']}",
                f"runs={payload['run_count']}",
                f"variables={payload['variable_count']}",
            ]
        )
    )


@app.command("inventory-phase1-bridge-discrepancy")
def inventory_phase1_bridge_discrepancy_cmd(
    out_json_path: Path = typer.Option(
        None,
        "--out-json-path",
        help="Optional JSON report path for the tracked bridge discrepancy inventory",
    ),
    out_md_path: Path = typer.Option(
        None,
        "--out-md-path",
        help="Optional Markdown report path for the tracked bridge discrepancy inventory",
    ),
) -> None:
    payload = build_phase1_bridge_discrepancy_inventory(
        out_json_path=out_json_path,
        out_md_path=out_md_path,
    )
    typer.echo(
        " ".join(
            [
                f"report={payload['report_path']}",
                f"markdown={payload['markdown_path']}",
                f"comparisons={len(payload['comparison_ids'])}",
            ]
        )
    )


def main() -> None:
    app()


if __name__ == "__main__":
    main()
