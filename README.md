# fp-ineq

**[Explore the published results](https://smkwray.github.io/fp-ineq/)**

`fp-ineq` adds distributional analysis to the stock [Fair US quarterly macroeconomic model](https://fairmodel.econ.yale.edu/). It runs transfer-policy scenarios through the Fair model, then produces poverty, inequality, and household-resource outputs from the solved macro path.

The overlay is built using the [fp-wraptr](https://github.com/smkwray/fp-wraptr) scenario tooling.

The repository is designed to be shareable without redistributing the stock Fair model itself. It publishes:

- the shareable overlay source that modifies a user-local stock Fair checkout
- checked-in public data snapshots and provenance reports under `data/`
- the scenario definitions and solve/export code under `src/fp_ineq/`
- the static results explorer bundle under `docs/`

It does not publish stock Fair files or fully composed private decks.

## What This Project Does

The project has two jobs:

1. modify the stock Fair model in a shareable way, without redistributing the stock model itself
2. publish a browsable results site showing how transfer-policy scenarios affect macro outcomes, poverty, and household resources

## Architecture

The model is a one-way macro-to-distribution wrapper around stock Fair:

- stock Fair provides the macro backbone
- policy scenarios enter the solve through real stock Fair transfer channels
- a separate solved identity block produces distribution outputs inside the solve
- those distribution outputs do not feed back into the stock macro block
- the published site is built from solved `LOADFORMAT.DAT` output, not reconstructed after the fact

## Public / Private Boundary

Public and intended to be shareable:

- `overlay/stock_fm/`
- `data/series/`
- `data/reports/`
- `reference/`
- `src/fp_ineq/`
- `tests/`
- `docs/`

Private and intentionally not included:

- stock Fair files such as `FM/fminput.txt`, `fmdata.txt`, `fmage.txt`, and `fmexog.txt`
- any fully composed deck built from stock Fair plus local overlays
- any local run artifacts generated under ignored runtime paths

## Repo Layout

```text
fp-ineq/
├── data/                   # checked-in public snapshots and provenance
├── docs/                   # published static site bundle
├── overlay/stock_fm/       # shareable overlay source files
├── reference/              # methodology and source notes
├── scripts/                # convenience wrappers
├── src/fp_ineq/            # compose / run / export code
└── tests/                  # regression tests for the published model path
```

When the commands are run locally, ignored runtime directories are generated for private composed decks and run artifacts.

## Results Site

The checked-in `docs/` bundle contains the published results explorer:

- 14 solved runs
- 445 available solved series
- 633 variable dictionary records
- 655 equation records
- default preset: `headline-poverty-resources`
- manifest-level family metadata with maturity tags for the published run families

The explorer is intentionally broader than the 14 public run files:

- every variable in the bundle has a definition if one exists in the stock dictionary, the model-runs dictionary, or the local inequality overlay dictionary
- every variable links to its relevant equations in the Equation Explorer
- equation and variable records are built for the bundle so they point to the matching run family

<details>
<summary>Developer commands used to rebuild the data and site</summary>

## Build and Publish Workflow

```text
1. fp-ineq refresh-data
2. fp-ineq compose-phase1-transfer-core --fp-home /path/to/FM
3. fp-ineq run-phase1-transfer-core --fp-home /path/to/FM
4. fp-ineq compose-phase1-distribution-block --fp-home /path/to/FM
5. fp-ineq run-phase1-distribution-block --fp-home /path/to/FM
6. fp-ineq export-phase1-full
7. fp-ineq publish-phase1-full
```

`export-phase1-full` now accepts optional family filters:

```text
fp-ineq export-phase1-full --family-maturity public
fp-ineq export-phase1-full --family-id ui --family-id transfer-composite
```

For the private UI ladder calibration workflow:

```text
fp-ineq run-phase1-ui-ladder --fp-home /path/to/FM
```

For the private matched transfer-composite ladder calibration workflow:

```text
fp-ineq run-phase1-transfer-composite-ladder --fp-home /path/to/FM
```

For the private credit-family scale decision workflow:

```text
fp-ineq run-phase2-credit-scale-sweep --fp-home /path/to/FM
```

For the private UI matching-offset stress workflow:

```text
fp-ineq run-phase2-ui-offset --fp-home /path/to/FM
```

Environment notes:

- stock Fair is supplied through `--fp-home` or the `FP_HOME` environment variable
- `fp-wraptr` is discovered from a sibling checkout or the `FP_WRAPTR_ROOT` environment variable
- the runtime backend is `fpexe`
- `compose-phase1-ui` and `run-phase1-ui` exist as narrower smoke-test commands, not as the main publication path

</details>

## Scope

The model centers on transfer-side scenarios built around channels already present in stock Fair:

- unemployment insurance, through the stock unemployment benefits variable (`UB`)
- broad federal household transfers, through the stock government-to-household transfer variable (`TRGH`)
- state/local household transfers, through the stock `TRSH` transfer variable
- a combined transfer package that moves all three levers together

The headline output variables are:

- overall poverty rate (`IPOVALL`)
- child poverty rate (`IPOVCH`)
- real disposable income per capita (`RYDPC`)

The following outputs are also produced but are treated as provisional diagnostics rather than headline results:

- household Gini coefficient (`IGINIHH`)
- median real household income proxy (`IMEDRINC`)

## Stock-Model Modifications

All modifications to the stock Fair model are documented in the stock patch manifest under `overlay/stock_fm/`.

At a high level, the composer does four things:

1. installs the neutral policy constants
2. installs the shareable identity include hook
3. patches three stock Fair transfer transmission points
4. keeps experimental credit and private UI-offset families out of the published family set and keeps the shadow wealth block suppressed from the public bundle even though the private distribution solve installs it neutrally

<details>
<summary>Exact stock-deck modifications</summary>

The shareable patch manifest makes these replacements:

1. Include the shareable policy and identity layer immediately after `CREATE C=1;`

```text
CREATE C=1;
INPUT FILE=ipolicy_base.txt;
INPUT FILE=idist_identities.txt;
```

2. Multiply stock unemployment benefits by the UI scaling factor (`UIFAC`):

```text
LHS UB=EXP(LUB);
```

becomes:

```text
LHS UB=EXP(LUB)*UIFAC;
```

3. Add the federal household-transfer quarterly increment (`SNAPDELTAQ`) into stock household transfers:

```text
GENR TRGH=TRGHQ*GDPD;
```

becomes:

```text
GENR TRGH=(TRGHQ+SNAPDELTAQ)*GDPD;
```

4. Multiply stock `TRSH` transfers by the `SSFAC` scaling factor:

```text
GENR TRSH=TRSHQ*GDPD;
```

becomes:

```text
GENR TRSH=(TRSHQ*SSFAC)*GDPD;
```

The neutral policy constants published in `overlay/stock_fm/ipolicy_base.txt` are:

```text
CREATE UIFAC=1;
CREATE SNAPDELTAQ=0;
CREATE SSFAC=1;
CREATE CRWEDGE=0;
CREATE UIMATCH=0;
CREATE HPEQW=0;
```

Only `UIFAC`, `SNAPDELTAQ`, and `SSFAC` are used in the published scenario family. `CRWEDGE`, `UIMATCH`, and `HPEQW` are neutral placeholders for private or deferred paths.

Current private credit-family status:

- `run-phase2-credit-scale-sweep` probes `CRWEDGE` magnitudes `1`, `5`, and `10`
- even at `|CRWEDGE|=10`, the best observed demand move is only about `4.01e-05`
- the current private recommendation is `keep_private_and_do_not_build_credit_ladder_yet`

Current private wealth-family status:

- `fp-ineq assess-phase2-wealth-maturity` evaluates the shadow `IWGAP150` block using the current coefficient diagnostics and integrated public-scenario responses
- the current recommendation is `candidate_for_expert_only_preset_keep_public_wealth_family_deferred`
- the wealth block is strong enough to remain a serious expert-only candidate, but there is still no dedicated public wealth-family baseline or wealth shock family
- current reassessment call: keep `IWGAP150` fully private for now rather than adding expert-only export governance in this tranche

Current private UI-offset status:

- `fp-ineq run-phase2-ui-offset` installs a private matching-offset patch on `JF` under its own neutral family baseline
- the latest private run calibrates `UIMATCH` to about `0.000065965955`
- that offset claws back about 25% of the medium UI rung's first-year `ΔUR` improvement while keeping first-year `ΔTRLOWZ` within about `0.006%` of the no-offset UI case
- the same stress run lowers `YD`, but it does not lower final `GDPR`, so it is currently best read as a private sensitivity check rather than a replacement public UI interpretation
- current reassessment call: keep the public UI family wording at the existing demand-dominant caveat rather than promoting the offset family into the public release story

</details>

## Scenarios

The published bundle contains exactly 14 runs:

| Run ID | UI factor (`UIFAC`) | Federal-transfer increment (`SNAPDELTAQ`) | `TRSH` factor (`SSFAC`) | Interpretation |
| --- | ---: | ---: | ---: | --- |
| `baseline-observed` | `1.00` | `0.0` | `1.00` | Shared neutral baseline with the same installed mechanisms as every other run. |
| `ui-relief` | `1.02` | `0.0` | `1.00` | Published medium UI ladder rung; same policy level as the original UI relief probe. |
| `ui-shock` | `0.98` | `0.0` | `1.00` | Lower unemployment insurance generosity through the stock unemployment benefits channel. |
| `ui-small` | `1.0141888330491307` | `0.0` | `1.00` | Small matched UI ladder rung normalized to the shared first-year `ΔTRLOWZ` bin. |
| `ui-large` | `1.023625843049206` | `0.0` | `1.00` | Large matched UI ladder rung normalized to the shared first-year `ΔTRLOWZ` bin. |
| `federal-transfer-relief` | `1.00` | `2.0` | `1.00` | Higher federal household transfers through the stock government-to-household transfer channel. |
| `federal-transfer-shock` | `1.00` | `-2.0` | `1.00` | Lower federal household transfers through the stock government-to-household transfer channel. |
| `state-local-transfer-relief` | `1.00` | `0.0` | `1.02` | Higher state/local household transfers through the stock `TRSH` transfer channel. |
| `state-local-transfer-shock` | `1.00` | `0.0` | `0.99` | Lower state/local household transfers through the stock `TRSH` transfer channel. |
| `transfer-package-relief` | `1.02` | `2.0` | `1.02` | Combined transfer-channel relief across unemployment benefits, federal household transfers, and state/local household transfers. |
| `transfer-package-shock` | `0.98` | `-2.0` | `0.99` | Combined transfer-channel shock across unemployment benefits, federal household transfers, and state/local household transfers. |
| `transfer-composite-small` | `1.0125839776982168` | `1.2583977698216835` | `1.0125839776982168` | Small matched transfer-composite ladder rung normalized to the shared first-year `ΔTRLOWZ` bin. |
| `transfer-composite-medium` | `1.018637285379202` | `1.8637285379202133` | `1.018637285379202` | Medium matched transfer-composite ladder rung normalized to the shared first-year `ΔTRLOWZ` bin. |
| `transfer-composite-large` | `1.0225154841560722` | `2.2515484156072145` | `1.0225154841560722` | Large matched transfer-composite ladder rung normalized to the shared first-year `ΔTRLOWZ` bin. |

Interpretation notes:

- These are channel probes, not calibrated policy packages with matched fiscal scale.
- `TRGH` is interpreted publicly as a broad federal household-transfer channel, not as SNAP specifically.
- `TRSH` is interpreted publicly as a state/local household-transfer channel. In the checked stock construction it is not treated as a clean Social Security-only series.
- The matched ladders normalize on the mean first-year `ΔTRLOWZ` over `2026.1` to `2026.4`.
- The federal-transfer scenarios should be read as broad federal household-transfer probes through `TRGH`.
- The state/local-transfer scenarios should be read as broad state/local household-transfer probes through `TRSH`.
- The transfer-package scenarios should be read as combined transfer probes, not as final package designs.
- The current shared bins were verified privately against the stock FM path and are anchored to the observed `ui-relief` response: `0.5x`, `1.0x`, and `1.5x` of that first-year `ΔTRLOWZ`.
- `ui-relief` is the published medium UI ladder rung, and the fresh integrated private distribution solve now uses that rung directly rather than carrying a separate `ui-medium` alias.
- The private calibration reports live under `runtime/phase1_ui/ladder/reports/` and `runtime/phase1_transfer_core/ladder/reports/`.
- The public 14-run UI family still does not include a dedicated labor-supply, labor-force-participation, or matching-offset block.
- A private `ui-matching-offset` stress family exists as a bounded two-point sensitivity envelope: one run claws back about 25% of the medium UI rung's first-year `ΔUR` improvement and another claws back about 50%, while both keep first-year `ΔTRLOWZ` nearly unchanged. Neither private stress lowers final `GDPR`, so those runs remain interpretation checks rather than grounds for a new public UI family. The public UI results should therefore still be read as demand-dominant probes with a small private sensitivity envelope, not as a fully balanced policy package.
- The descriptive contrary-channel audit shows endogenous rate counter-moves across all current public transfer families, so no additional synthetic contrary public families are added at this stage.

<details>
<summary>Why all 14 published scenarios are compared to one baseline</summary>

All 14 published runs share one baseline. The shared baseline is valid because baseline and scenario runs all use:

- the same composed stock deck
- the same installed overlay files
- the same distribution block
- the same coefficient-delivery path
- the same closure and endogeneity structure

The only thing that changes across the scenario family is the level of an already-installed scenario lever.

If a future family changes structure rather than just shock level, it must either:

1. install the new mechanism neutrally in the shared baseline too, or
2. get a family-specific baseline

</details>

## Distribution Block

The distribution path adds a separate solved identity block on top of the stock Fair macro solve. The identity file is `overlay/stock_fm/idist_phase1_block.txt`.

<details>
<summary>Exact identities used for the distribution block</summary>

```text
GENR LGDPR=LOG(GDPR);
GENR TRLOWZ=(UB+TRGH+TRSH)/(POP*PH);
GENR LRYDPC=LOG(RYDPC);
GENR UBZ=(UB-UBBAR)/UBSTD;
GENR TRGHZ=(TRGH-TRGHBAR)/TRGHSTD;
GENR TRSHZ=(TRSH-TRSHBAR)/TRSHSTD;
GENR UIDEV=UBZ-0.5*(TRGHZ+TRSHZ);
GENR GHSHDV=TRGHZ-TRSHZ;

IDENT LPOVALL=PV0+PVU*UR+PVT*TRLOWZ+PVUI*UIDEV+PVGH*GHSHDV;
IDENT LPOVCHGAP=CG0+CGU*UR+CGT*TRLOWZ+CGUI*UIDEV+CGGH*GHSHDV;
IDENT LGINIHH=GN0+GNU*UR+GNT*TRLOWZ;
IDENT LMEDINC=MD0+MDR*LRYDPC+MDU*UR;

IDENT IPOVALL=EXP(LPOVALL)/(1+EXP(LPOVALL));
IDENT IPOVCH=EXP(LPOVALL+LPOVCHGAP)/(1+EXP(LPOVALL+LPOVCHGAP));
IDENT IGINIHH=EXP(LGINIHH)/(1+EXP(LGINIHH));
IDENT IMEDRINC=EXP(LMEDINC);
```

</details>

Interpretation:

- The overall poverty rate (`IPOVALL`) and child poverty rate (`IPOVCH`) are the strongest distribution outputs. Both are logit-transformed identities anchored on the unemployment rate (`UR`) and the low-income transfer bridge (`TRLOWZ`), with an additional internal deviation basis that separates UI-heavy support from federal-versus-state/local transfer mix shifts.
- The low-income transfer bridge (`TRLOWZ`) aggregates stock transfer flows — unemployment benefits, federal household transfers, and state/local household transfers — scaled by population and prices.
- Real disposable income per capita (`RYDPC`) is the preferred household-resource headline.
- The household Gini coefficient (`IGINIHH`) and median real income proxy (`IMEDRINC`) are reduced-form diagnostics, not headline measures.

<details>
<summary>How the poverty and income measures are fit to data</summary>

The coefficient delivery is explicit and limited:

- Coefficients are estimated offline and staged privately through `idcoef.txt`.
- The fit uses annual observations.
- The checked-in target history begins in 2015.
- The coefficient report uses the observed overlap window from 2015 to 2025, giving an effective sample size of 11 annual observations.
- For `IPOVALL` and `IPOVCH`, the validated base fit on `UR` and `TRLOWZ` is preserved and only the transfer-mix deviation terms are added with ridge shrinkage.

The coefficients are model-conditional:

- Regressors come from the transfer-core baseline solve.
- Targets come from the checked-in public snapshots in `data/series/`.

</details>

## Data Provenance

`fp-ineq refresh-data` writes two checked-in public data surfaces:

- `data/series/*.csv`
- `data/reports/*.json`

Each provenance report records:

- source name
- source URL
- units
- coverage start and end
- observed start and end
- transformation note
- refresh timestamp

### Distribution calibration targets

These series are used directly in the distribution identity calibration:

| Variable | Role | Source | Notes |
| --- | --- | --- | --- |
| Overall poverty rate (`IPOVALL`) | output target | [U.S. Census Bureau — Poverty](https://www.census.gov/topics/income-poverty/poverty.html) | Poverty anchor for the overall poverty identity. |
| Child poverty rate (`IPOVCH`) | output target | [U.S. Census Bureau — Child Poverty](https://www.census.gov/topics/income-poverty/poverty/about/child-poverty.html) | Poverty anchor for the child-poverty gap identity. |
| Household Gini coefficient (`IGINIHH`) | output target | [U.S. Census Bureau — Income Inequality](https://www.census.gov/topics/income-poverty/income-inequality.html) | Provisional inequality target. |
| Median real household income proxy (`IMEDRINC`) | output target | [U.S. Census Bureau — Income & Poverty Report](https://www.census.gov/library/publications/2024/demo/p60-282.html) | Provisional median-style resource target. |

### Staged public input series

These series are refreshed into `data/` for staging, reference, or future use:

| Variable | Role | Source | Notes |
| --- | --- | --- | --- |
| UI benefits index (`IUIBEN`) | staged input | [U.S. Department of Labor — OUI Data Dashboard](https://oui.doleta.gov/unemploy/DataDashboard.asp) | Public unemployment insurance benefits snapshot. The model scenarios enter through the stock unemployment benefits channel rather than this series directly. |
| Social Security benefits index (`ISSBEN`) | staged input | [Social Security Administration — Benefit Tables](https://www.ssa.gov/oact/cola/Benefits.html) | Public Social Security benefit-level snapshot. |
| SNAP participation index (`ISNAP`) | staged input | [USDA — SNAP Data](https://www.fns.usda.gov/pd/supplemental-nutrition-assistance-program-snap) | Public SNAP participation snapshot. |
| Household net worth index (`IHHNW`) | staged input | [Federal Reserve — Financial Accounts (Z.1)](https://www.federalreserve.gov/releases/z1/) | Household net worth series retained for deferred wealth work. |
| Home equity index (`IHOMEQ`) | staged input | [Federal Reserve — Financial Accounts (Z.1)](https://www.federalreserve.gov/releases/z1/) | Home equity series retained for deferred housing/wealth work. |
| Federal funds rate (`IFFUNDS`) | staged input | [FRED — Federal Funds Rate](https://fred.stlouisfed.org/series/FEDFUNDS) | Federal funds rate series retained for deferred credit work. |
| Transfer composite index (`ITRCOMP`) | derived input | Derived from UI benefits, Social Security benefits, and SNAP participation | Legacy transfer composite, not part of the published scenario bundle. |
| Credit composite index (`ICRDCMP`) | derived input | Derived from home equity and inverse federal funds rate | Legacy credit composite, not part of the published scenario bundle. |
| Top-10 vs. bottom-50 wealth-share gap (`IWGAP1050`) | staged output target | [Federal Reserve — Distributional Financial Accounts](https://www.federalreserve.gov/releases/z1/dataviz/dfa/distribute/chart/) | Retained for deferred wealth-distribution work. |
| Top-1 vs. bottom-50 wealth-share gap (`IWGAP150`) | staged output target | [Federal Reserve — Distributional Financial Accounts](https://www.federalreserve.gov/releases/z1/dataviz/dfa/distribute/chart/) | Retained for deferred wealth-distribution work. |

### Normalization rules

`refresh-data` applies deterministic annual-to-quarterly normalization:

- Output targets are written only across their observed annual-anchor span.
- Input staging series are expanded across the longer project window.
- The checked-in output targets do not publish synthetic pre-observation history.

For more detail, see [reference/data-sources.md](reference/data-sources.md).

<details>
<summary>Checks used to confirm the scenarios behave as intended</summary>

### Transfer-core validation

For each non-baseline run:

- The relevant transfer channel variable must move.
- The relevant transfer channel variable must move in the expected direction.
- Disposable income (`YD`) must move in the expected direction.
- Real GDP (`GDPR`) must move in the expected direction.
- At least one of the unemployment rate (`UR`) or per-capita income (`PCY`) must move.
- Relief runs require the unemployment rate down or per-capita income up.
- Shock runs require the unemployment rate up or per-capita income down.

Channel mapping:

- UI runs must move unemployment benefits (`UB`).
- Federal-transfer runs must move household transfers (`TRGH`).
- State/local-transfer runs must move `TRSH`.
- Transfer-package runs must move unemployment benefits (`UB`), federal household transfers (`TRGH`), and state/local household transfers (`TRSH`).

### Distribution-block validation

For each non-baseline run:

- The overall poverty rate (`IPOVALL`) and child poverty rate (`IPOVCH`) must move.
- Relief runs require the overall poverty rate down and the child poverty rate down.
- Shock runs require the overall poverty rate up and the child poverty rate up.
- The low-income transfer bridge (`TRLOWZ`), real disposable income per capita (`RYDPC`), disposable income (`YD`), and real GDP (`GDPR`) must move in the expected direction.
- The relevant transfer channel must move in the expected direction.
- At least one of the unemployment rate (`UR`) or per-capita income (`PCY`) must confirm direction.

The household Gini coefficient (`IGINIHH`) and median real income proxy (`IMEDRINC`) are tracked but excluded from hard validation gates.

</details>

## How To Read The Published Measures

| Variable | Status | Reason |
| --- | --- | --- |
| Overall poverty rate (`IPOVALL`) | headline | Strongest overall poverty output. |
| Child poverty rate (`IPOVCH`) | headline | Strongest child-poverty output. |
| Real disposable income per capita (`RYDPC`) | headline | Strongest household-resource signal. |
| Low-income transfer bridge (`TRLOWZ`) | supporting | Transfer-resource bridge for interpreting the distribution block. |
| Household Gini coefficient (`IGINIHH`) | provisional diagnostic | Useful but too reduced-form to carry headline welfare claims. |
| Median real income proxy (`IMEDRINC`) | provisional diagnostic | Median-style household resource proxy, not a validated median-income forecast. |

## What This Repository Does Not Include

The following areas are not part of the published model scope:

- credit-condition scenario families
- housing, home-equity, or wealth scenario families
- legacy `ITRCOMP` composite-index publication as a separate public scenario family
- two-way feedback from the distribution block into the stock macro block
- publication-grade interpretation of the household Gini coefficient (`IGINIHH`)
- publication-grade interpretation of the median real income proxy (`IMEDRINC`)
- matched-scale policy calibration for the scenario shock sizes

Some older overlay files are present as legacy references but are not used in the published path.

## Tests

The main regression test coverage is in:

- `tests/test_data_pipeline.py`
- `tests/test_phase1_transfer_core.py`
- `tests/test_phase1_distribution_block.py`
- `tests/test_export_phase1_solved.py`

The published site reflects the run bundle and dictionary:

- Definitions cover every bundle variable that can be resolved from the merged stock and overlay dictionary sources.
- The Equation Explorer shows bundle-level equation links.
- The site can be checked locally by serving `docs/` and opening `docs/index.html`.

## Related Documentation

- [reference/methodology.md](reference/methodology.md)
- [reference/data-sources.md](reference/data-sources.md)
- [docs/shareable-overlay-architecture.md](docs/shareable-overlay-architecture.md)

## In Short

- The published bundle contains 14 public transfer-family runs backed by one shared baseline.
- The main published results are overall poverty, child poverty, and real disposable income per person.
- The site also includes equation links, variable definitions, and supporting distribution measures for readers who want the technical detail.
- Provisional diagnostics: household Gini coefficient, median real income proxy
- Distribution outputs are one-way (macro drives distribution; no feedback into the macro block)
- The published bundle is derived from solved model output only
- Credit and the private `ui-matching-offset` family are outside the published model scope; the shadow wealth block is installed privately but suppressed from the public bundle
