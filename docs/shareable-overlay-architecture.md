# Shareable Overlay Architecture

`fp-ineq` publishes a wrapper layer around the stock Fair model rather than the stock model itself.

## Packaging pattern

The shareable part of the project consists of:

- overlay include files
- patch manifests
- scenario definitions
- estimation code
- methodology and source notes

The private part remains local to the operator:

- the stock Fair deck
- stock `fmdata.txt`, `fmage.txt`, and `fmexog.txt`
- any fully composed deck produced by applying the patch manifest
- raw runtime artifacts

The stock deck and the published overlay are composed locally before running `fp.exe`.

## Why this works

The runner machinery in `fp-wraptr` supports exactly this split:

- `fp_wraptr.scenarios.config.ScenarioConfig` supports `input_overlay_dir` and `input_patches`
- `fp_wraptr.scenarios.runner.run_scenario()` stages a work directory, copies overlay files with overlay precedence, applies `input_patches`, and runs `fp.exe`
- `fp_wraptr.scenarios.input_tree.prepare_work_dir_for_fp_run()` copies transitive `INPUT FILE=...;` dependencies into the working directory
- `fp_wraptr.scenarios.authoring.compiler` stages authored overlay trees and generated include files into a compile directory

That means the published unit does not need to be a fully rewritten `fminput.txt`. It can be:

- a patch manifest that describes how to modify the stock deck
- include files referenced by those patches
- public scenario metadata
- public data and methodology

## Published overlay form

The repository publishes:

- `overlay/stock_fm/ipolicy_base.txt`
- `overlay/stock_fm/idist_identities.txt`
- `overlay/stock_fm/stock_patch_manifest.phase1.yaml`
- scenario definitions
- public target and policy series
- methodology and source notes

## Baseline rule for scenario comparisons

A shared baseline is valid only when the baseline already contains the same installed mechanisms as the scenario runs.

For the published transfer family, that means:

- `UIFAC`, `SNAPDELTAQ`, and `SSFAC` are present in the baseline with neutral settings
- the same stock patch manifest is applied in the baseline and in every shocked run
- the same distribution identity block is installed in the baseline and in every shocked run

Under that design, the UI, SNAP-style, Social Security, and transfer-package runs are comparable to one shared baseline because they differ only by neutral-versus-shocked lever settings.

If a future family adds a new structural patch, new identity block, different closure, or different coefficient path that the baseline does not also contain neutrally, that family should either:

1. install that mechanism neutrally in the shared baseline, or
2. use its own family-specific baseline

## Practical implication

The project can share its modifications to the stock Fair model without sharing the stock model itself. Those modifications are distributed as overlays and patch manifests that a private local composer applies before solve.
