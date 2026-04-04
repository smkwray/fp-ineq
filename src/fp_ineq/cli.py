from __future__ import annotations

from pathlib import Path

import typer

from .bridge import locate_fp_home
from .data_pipeline import refresh_data
from .export import export_phase1_full_bundle, publish_phase1_bundle_to_docs
from .phase1_distribution_block import (
    build_phase1_distribution_overlay,
    run_phase1_distribution_block,
    write_phase1_distribution_scenarios,
)
from .phase1_transfer_core import run_phase1_transfer_core, write_phase1_transfer_scenarios
from .phase1_ui import build_phase1_private_overlay, run_phase1_ui_prototype, write_phase1_ui_scenarios

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
) -> None:
    payload = export_phase1_full_bundle(
        report_path=report_path,
        out_dir=out_dir,
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
