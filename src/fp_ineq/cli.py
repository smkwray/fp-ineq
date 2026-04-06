from __future__ import annotations

from pathlib import Path

import typer

from .bridge import locate_fp_home
from .data_pipeline import refresh_data
from .export import export_phase1_full_bundle, publish_phase1_bundle_to_docs
from .phase1_contrary_audit import assess_phase1_contrary_channels
from .phase1_distribution_block import (
    build_phase1_distribution_overlay,
    run_phase1_distribution_block,
    write_phase1_distribution_scenarios,
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
) -> None:
    payload = run_phase1_distribution_block(fp_home=locate_fp_home(fp_home))
    typer.echo(
        " ".join(
            [
                f"passes={str(payload['passes']).lower()}",
                f"report={payload['report_path']}",
                f"artifacts={payload['artifacts_dir']}",
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
def main() -> None:
    app()


if __name__ == "__main__":
    main()
