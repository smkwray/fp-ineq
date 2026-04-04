# Methodology

`fp-ineq` is implemented as a stock-Fair overlay, not as a redistribution of the stock model itself.

The repository publishes:

- overlay source files
- checked-in `data/series/` snapshots and `data/reports/` provenance
- scenario definitions
- export tooling

The private runtime flow is:

1. read a user-local stock `FM/fminput.txt`
2. apply the phase-1 stock patch manifest that installs the shareable policy layer on top of stock Fair transfer channels
3. inject the phase-1 distribution identity block and the private coefficient include on the distribution path
4. write the composed deck only into ignored local runtime paths
5. run scenarios through `fp-wraptr`

## Phase-1 model design

The phase-1 system has three main components:

- a stock-Fair policy patch layer for unemployment insurance, SNAP-style transfers, and Social Security
- a solved distribution identity block for overall poverty, child poverty, the household Gini coefficient, and a median-style income proxy
- a one-way macro-to-distribution wrapper in which Fair macro states drive distribution identities without feeding back into the stock macro block

## Published scenario scope

The published solved bundle contains 9 runs:

- `baseline-observed`
- `ui-relief`
- `ui-shock`
- `snap-relief`
- `snap-shock`
- `social-security-relief`
- `social-security-shock`
- `transfer-package-relief`
- `transfer-package-shock`

These runs belong to one model system and are intended to be compared to a shared baseline.

## Baseline comparability rule

A shared baseline is valid only when all compared runs use:

- the same composed stock deck
- the same installed overlay files and identity blocks
- the same closure and endogeneity structure
- the same coefficient-delivery path

The only allowed difference between baseline and a non-baseline run is the level of an already-installed scenario lever.

For the phase-1 transfer family, that condition holds:

- the shared policy layer is installed in every run with neutral settings `UIFAC=1`, `SNAPDELTAQ=0`, and `SSFAC=1`
- the same stock-deck patch manifest is applied in every run
- the same phase-1 distribution block and coefficient include are installed in every run
- the non-baseline runs differ only through scenario-level replacements of those neutral settings

The family interpretations are therefore:

- UI: move `UIFAC` away from `1`
- SNAP-style transfers: move `SNAPDELTAQ` away from `0`
- Social Security: move `SSFAC` away from `1`
- transfer package: move several already-installed levers at once

## When a family needs its own baseline

A family needs its own baseline if it changes model structure rather than only shock level. Examples include:

- adding a new patch group that is not also installed neutrally in the shared baseline
- adding or removing an equation block
- changing closure or endogeneity
- switching to a different coefficient or calibration-delivery path

If a future family does any of those things, it must either:

1. install the new mechanism neutrally in the shared baseline too, or
2. use a family-specific baseline

## Interpretation limits

The phase-1 shock sizes are channel probes, not calibrated policy packages with matched fiscal scale:

- UI runs move `UIFAC` to `1.02` or `0.98`
- SNAP runs move `SNAPDELTAQ` to `+2` or `-2`
- Social Security runs move `SSFAC` to `1.02` or `0.99`
- transfer-package runs move several already-installed levers together

These settings are intended to test whether the transmission channels move the stock macro block and the distribution identities in coherent directions.

`IGINIHH` and `IMEDRINC` are solved outputs in all 9 runs, but they remain provisional diagnostics rather than headline measures.

- `IGINIHH` is a reduced-form identity driven by `UR` and `TRLOWZ`
- `IMEDRINC` is a reduced-form identity driven by `LRYDPC` and `UR`
- `RYDPC` remains the stronger household-resource headline

The strongest phase-1 outputs are:

- baseline comparability across the 9-run family
- coherent macro and transfer-channel movement
- directional poverty movement in response to transfer-side relief and shock scenarios

The weaker phase-1 outputs are:

- headline interpretation of `IGINIHH`
- headline interpretation of `IMEDRINC`
- final policy calibration of shock magnitudes

## Calibration window

The checked-in output target snapshots do not provide a true `1990` to `2025` historical panel.

- the checked-in annual anchors for `IPOVALL`, `IPOVCH`, `IGINIHH`, and `IMEDRINC` begin in `2015`
- `refresh-data` writes only the observed quarterly span implied by those anchors
- the phase-1 distribution coefficient fit therefore uses the actual observed target/regressor overlap, which is `2015` to `2025` in the checked-in data surface

If longer historical target coverage is added later, the coefficient report can widen the main sample accordingly.
