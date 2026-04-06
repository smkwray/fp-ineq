# Methodology

`fp-ineq` is implemented as a stock-Fair overlay, not as a redistribution of the stock model itself.

The repository publishes:

- overlay source files
- checked-in `data/series/` snapshots and `data/reports/` provenance
- scenario definitions
- export tooling

The private runtime flow is:

1. read a user-local stock `FM/fminput.txt`
2. apply the stock patch manifest that installs the shareable policy layer on top of stock Fair transfer channels
3. inject the distribution identity block and the private coefficient include on the distribution path
4. write the composed deck only into ignored local runtime paths
5. run scenarios through `fp-wraptr`

## Model design

The model has three main components:

- a stock-Fair policy patch layer for unemployment insurance, broad federal household transfers, and state/local household transfers
- a solved distribution identity block for overall poverty, child poverty, the household Gini coefficient, and a median-style income proxy
- a one-way macro-to-distribution wrapper in which Fair macro states drive distribution identities without feeding back into the stock macro block

## Published scenario scope

The published explorer exposes one public results family built around a shared baseline:

- `baseline-observed`
- `transfer-composite-small`
- `transfer-composite-medium`
- `transfer-composite-large`

The published manifest is now family-aware:

- each run is tagged with a `family_id` and `family_maturity`
- the top-level manifest groups the published runs into explicit family records
- private families such as the credit wedge probes and the shadow wealth block remain outside the published manifest

## Baseline comparability rule

A shared baseline is valid only when all compared runs use:

- the same composed stock deck
- the same installed overlay files and identity blocks
- the same closure and endogeneity structure
- the same coefficient-delivery path

The only allowed difference between baseline and a non-baseline run is the level of an already-installed scenario lever.

For the published transfer family, that condition holds:

- the shared policy layer is installed in every run with neutral settings `UIFAC=1`, `SNAPDELTAQ=0`, and `SSFAC=1`
- the same stock-deck patch manifest is applied in every run
- the same distribution block and coefficient include are installed in every run
- the non-baseline runs differ only through scenario-level replacements of those neutral settings

The family interpretations are therefore:

- UI: move `UIFAC` away from `1`
- UI ladder: move `UIFAC` to matched first-year `ΔTRLOWZ` bins
- federal household transfers: move `SNAPDELTAQ` away from `0`
- state/local household transfers: move `SSFAC` away from `1`
- transfer package: move several already-installed levers at once
- transfer-composite ladder: move several already-installed levers together to matched first-year `ΔTRLOWZ` bins

## When a family needs its own baseline

A family needs its own baseline if it changes model structure rather than only shock level. Examples include:

- adding a new patch group that is not also installed neutrally in the shared baseline
- adding or removing an equation block
- changing closure or endogeneity
- switching to a different coefficient or calibration-delivery path

If a future family does any of those things, it must either:

1. install the new mechanism neutrally in the shared baseline too, or
2. use a family-specific baseline

Current credit-family decision:

- the `credit_effective_rates` patch group is installed neutrally in its own private family baseline
- a private `CRWEDGE` scale sweep over magnitudes `1`, `5`, and `10` does not produce a strong enough demand signal for publication
- the best observed absolute demand move is about `4.01e-05` at `|CRWEDGE|=10`, below the current `1e-4` adequacy threshold
- the credit family therefore remains private and does not yet justify a published ladder

Current wealth-family decision:

- the shadow `IWGAP150` block passes the current diagnostic gates well enough to stay as a serious expert-only candidate
- the current integrated transfer scenarios produce nontrivial `IWGAP150` movement, with a best observed absolute delta of about `0.0997`
- the block is not yet promoted to a public wealth family because there is still no dedicated public wealth-family baseline or wealth shock family
- the current recommendation is `candidate_for_expert_only_preset_keep_public_wealth_family_deferred`
- current reassessment call: keep `IWGAP150` fully private for now and defer any expert-only governance/export work

## Interpretation limits

The shock sizes are channel probes, not calibrated policy packages with matched fiscal scale:

- UI runs move `UIFAC` to `1.02` or `0.98`
- UI ladder runs move `UIFAC` to calibrated matched-bin levels (`1.0141888330491307`, `1.02`, `1.023625843049206`)
- federal-transfer runs move `SNAPDELTAQ` to `+2` or `-2`
- state/local-transfer runs move `SSFAC` to `1.02` or `0.99`
- transfer-package runs move several already-installed levers together
- transfer-composite ladder runs move all three installed transfer levers together to matched-bin levels recorded in the private ladder calibration report

These settings are intended to test whether the transmission channels move the stock macro block and the distribution identities in coherent directions.

The matched-ladder normalization convention is the mean first-year `ΔTRLOWZ` over `2026.1` to `2026.4`.

The published transfer results still do **not** include a dedicated labor-supply, labor-force-participation, or matching-offset block. A private `ui-matching-offset` stress family exists as a bounded sensitivity envelope: one run claws back about 25% of the medium UI rung's first-year `ΔUR` improvement and another claws back about 50%, while both keep first-year `ΔTRLOWZ` nearly unchanged. Neither private stress lowers final `GDPR`, so those runs remain interpretation checks rather than grounds for a new public family. The published transfer results should therefore still be read as demand-dominant probes with a small private sensitivity envelope, not as a fully balanced policy package.

The new contrary-channel audit is descriptive rather than structural. It reads the public baseline plus repaired transfer-composite ladder and flags endogenous countervailing movement already present in solved Fair outputs for `UR`, `GDPR`, `YD`, and rates. In the current release, the consistent countervailing signal is higher rates across the repaired composite ladder, while the dedicated omitted-channel stress remains confined to the private UI offset family. That is why the project does not add extra synthetic contrary public scenarios at this stage.

The current reassessment decision is to keep that wording in place rather than promote the private offset family into a public scenario family or stronger public interpretation.

The repaired transfer-composite ladder is a financed package path rather than a free-standing one-channel shock. Internal package QA checks gross package size, financing flows, package balance, and acceptable net package behavior before the ladder is treated as publishable, but those detailed package artifacts remain private.

`IGINIHH` and `IMEDRINC` are solved outputs in the published bundle, but they remain provisional diagnostics rather than headline measures.

- `IPOVALL` and `IPOVCH` keep the validated aggregate transfer bridge on `TRLOWZ` and add two shrunken internal transfer-mix deviation terms (`UIDEV`, `GHSHDV`) built from standardized `UB`, `TRGH`, and `TRSH`
- `IGINIHH` is a reduced-form identity driven by `UR` and `TRLOWZ`
- `IMEDRINC` is a reduced-form identity driven by `LRYDPC` and `UR`
- `RYDPC` remains the stronger household-resource headline

The strongest outputs are:

- baseline comparability across the published 4-run family
- coherent macro and transfer-channel movement
- directional poverty movement in response to the repaired matched-ladder scenarios

The weaker outputs are:

- headline interpretation of `IGINIHH`
- headline interpretation of `IMEDRINC`
- final policy calibration of shock magnitudes
- any public claim that the checked-in transfer family already includes a dedicated adverse labor-market channel

## Calibration window

The checked-in output target snapshots do not provide a true `1990` to `2025` historical panel.

- the checked-in annual anchors for `IPOVALL`, `IPOVCH`, `IGINIHH`, and `IMEDRINC` begin in `2015`
- `refresh-data` writes only the observed quarterly span implied by those anchors
- the distribution coefficient fit therefore uses the actual observed target/regressor overlap, which is `2015` to `2025` in the checked-in data surface
- the poverty add-on deviation terms are fit with residual ridge shrinkage over that same `2015` to `2025` overlap, while preserving the base `UR` + `TRLOWZ` fit

If longer historical target coverage is added later, the coefficient report can widen the main sample accordingly.
