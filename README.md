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

## Published Bundle

The checked-in `docs/` bundle contains the published results explorer:

- 9 solved runs
- 450 available solved series
- 633 variable dictionary records
- 279 equation records
- default preset: `headline-poverty-resources`

The explorer is intentionally broader than the nine scenario runs alone:

- every variable in the bundle has a definition if one exists in the stock dictionary, the model-runs dictionary, or the local inequality overlay dictionary
- every variable links to its relevant equations in the Equation Explorer
- equation and variable records are built for the bundle so they point to the matching run family

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

Environment notes:

- stock Fair is supplied through `--fp-home` or the `FP_HOME` environment variable
- `fp-wraptr` is discovered from a sibling checkout or the `FP_WRAPTR_ROOT` environment variable
- the runtime backend is `fpexe`
- `compose-phase1-ui` and `run-phase1-ui` exist as narrower smoke-test commands, not as the main publication path

## Scope

The model centers on transfer-side scenarios built around channels already present in stock Fair:

- unemployment insurance, through the stock unemployment benefits variable (`UB`)
- SNAP-style household transfers, through the stock government-to-household transfer variable (`TRGH`)
- Social Security, through the stock Social Security transfer variable (`TRSH`)
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
4. leaves experimental credit and wealth paths out of the published solve

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

3. Add the SNAP-style quarterly transfer increment (`SNAPDELTAQ`) into stock household transfers:

```text
GENR TRGH=TRGHQ*GDPD;
```

becomes:

```text
GENR TRGH=(TRGHQ+SNAPDELTAQ)*GDPD;
```

4. Multiply stock Social Security transfers by the Social Security scaling factor (`SSFAC`):

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
CREATE HPEQW=0;
```

Only `UIFAC`, `SNAPDELTAQ`, and `SSFAC` are used in the published scenario family. `CRWEDGE` and `HPEQW` are neutral placeholders for experimental or deferred paths.

</details>

## Scenarios

The published bundle contains exactly 9 runs:

| Run ID | UI factor (`UIFAC`) | SNAP increment (`SNAPDELTAQ`) | SS factor (`SSFAC`) | Interpretation |
| --- | ---: | ---: | ---: | --- |
| `baseline-observed` | `1.00` | `0.0` | `1.00` | Shared neutral baseline with the same installed mechanisms as every other run. |
| `ui-relief` | `1.02` | `0.0` | `1.00` | Higher unemployment insurance generosity through the stock unemployment benefits channel. |
| `ui-shock` | `0.98` | `0.0` | `1.00` | Lower unemployment insurance generosity through the stock unemployment benefits channel. |
| `snap-relief` | `1.00` | `2.0` | `1.00` | Higher SNAP-style household transfers through the stock government-to-household transfer channel. |
| `snap-shock` | `1.00` | `-2.0` | `1.00` | Lower SNAP-style household transfers through the stock government-to-household transfer channel. |
| `social-security-relief` | `1.00` | `0.0` | `1.02` | Higher Social Security benefits through the stock Social Security transfer channel. |
| `social-security-shock` | `1.00` | `0.0` | `0.99` | Lower Social Security benefits through the stock Social Security transfer channel. |
| `transfer-package-relief` | `1.02` | `2.0` | `1.02` | Combined transfer-channel relief across unemployment benefits, household transfers, and Social Security. |
| `transfer-package-shock` | `0.98` | `-2.0` | `0.99` | Combined transfer-channel shock across unemployment benefits, household transfers, and Social Security. |

Interpretation notes:

- These are channel probes, not calibrated policy packages with matched fiscal scale.
- The SNAP scenarios should be read as SNAP-style transfer probes through the broader household transfer channel (`TRGH`).
- The transfer-package scenarios should be read as combined transfer probes, not as final package designs.

## Baseline Comparability Rule

All 9 runs share one baseline. The shared baseline is valid because baseline and scenario runs all use:

- the same composed stock deck
- the same installed overlay files
- the same distribution block
- the same coefficient-delivery path
- the same closure and endogeneity structure

The only thing that changes across the scenario family is the level of an already-installed scenario lever.

If a future family changes structure rather than just shock level, it must either:

1. install the new mechanism neutrally in the shared baseline too, or
2. get a family-specific baseline

## Distribution Block

The distribution path adds a separate solved identity block on top of the stock Fair macro solve. The identity file is `overlay/stock_fm/idist_phase1_block.txt`.

<details>
<summary>Exact identities used for the distribution block</summary>

```text
GENR LGDPR=LOG(GDPR);
GENR TRLOWZ=(UB+TRGH+TRSH)/(POP*PH);
GENR LRYDPC=LOG(RYDPC);

IDENT LPOVALL=PV0+PVU*UR+PVT*TRLOWZ;
IDENT LPOVCHGAP=CG0+CGU*UR+CGT*TRLOWZ;
IDENT LGINIHH=GN0+GNU*UR+GNT*TRLOWZ;
IDENT LMEDINC=MD0+MDR*LRYDPC+MDU*UR;

IDENT IPOVALL=EXP(LPOVALL)/(1+EXP(LPOVALL));
IDENT IPOVCH=EXP(LPOVALL+LPOVCHGAP)/(1+EXP(LPOVALL+LPOVCHGAP));
IDENT IGINIHH=EXP(LGINIHH)/(1+EXP(LGINIHH));
IDENT IMEDRINC=EXP(LMEDINC);
```

</details>

Interpretation:

- The overall poverty rate (`IPOVALL`) and child poverty rate (`IPOVCH`) are the strongest distribution outputs. Both are logit-transformed identities driven by the unemployment rate (`UR`) and the low-income transfer bridge (`TRLOWZ`).
- The low-income transfer bridge (`TRLOWZ`) aggregates stock transfer flows — unemployment benefits, household transfers, and Social Security — scaled by population and prices.
- Real disposable income per capita (`RYDPC`) is the preferred household-resource headline.
- The household Gini coefficient (`IGINIHH`) and median real income proxy (`IMEDRINC`) are reduced-form diagnostics, not headline measures.

## Coefficient Protocol

The coefficient delivery is explicit and limited:

- Coefficients are estimated offline and staged privately through `idcoef.txt`.
- The fit uses annual observations.
- The checked-in target history begins in 2015.
- The coefficient report uses the observed overlap window from 2015 to 2025, giving an effective sample size of 11 annual observations.

The coefficients are model-conditional:

- Regressors come from the transfer-core baseline solve.
- Targets come from the checked-in public snapshots in `data/series/`.

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

## Validation Rules

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
- SNAP runs must move household transfers (`TRGH`).
- Social Security runs must move Social Security transfers (`TRSH`).
- Transfer-package runs must move unemployment benefits (`UB`), household transfers (`TRGH`), and Social Security transfers (`TRSH`).

### Distribution-block validation

For each non-baseline run:

- The overall poverty rate (`IPOVALL`) and child poverty rate (`IPOVCH`) must move.
- Relief runs require the overall poverty rate down and the child poverty rate down.
- Shock runs require the overall poverty rate up and the child poverty rate up.
- The low-income transfer bridge (`TRLOWZ`), real disposable income per capita (`RYDPC`), disposable income (`YD`), and real GDP (`GDPR`) must move in the expected direction.
- The relevant transfer channel must move in the expected direction.
- At least one of the unemployment rate (`UR`) or per-capita income (`PCY`) must confirm direction.

The household Gini coefficient (`IGINIHH`) and median real income proxy (`IMEDRINC`) are tracked but excluded from hard validation gates.

## Variable Status

| Variable | Status | Reason |
| --- | --- | --- |
| Overall poverty rate (`IPOVALL`) | headline | Strongest overall poverty output. |
| Child poverty rate (`IPOVCH`) | headline | Strongest child-poverty output. |
| Real disposable income per capita (`RYDPC`) | headline | Strongest household-resource signal. |
| Low-income transfer bridge (`TRLOWZ`) | supporting | Transfer-resource bridge for interpreting the distribution block. |
| Household Gini coefficient (`IGINIHH`) | provisional diagnostic | Useful but too reduced-form to carry headline welfare claims. |
| Median real income proxy (`IMEDRINC`) | provisional diagnostic | Median-style household resource proxy, not a validated median-income forecast. |

## Out of Scope

The following areas are not part of the published model scope:

- credit-condition scenario families
- housing, home-equity, or wealth scenario families
- revived transfer-composite publication
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

## Summary

- 9 solved scenario runs probing transfer-side policy channels in the stock Fair model
- Headline outputs: overall poverty rate, child poverty rate, real disposable income per capita
- Provisional diagnostics: household Gini coefficient, median real income proxy
- Distribution outputs are one-way (macro drives distribution; no feedback into the macro block)
- The published bundle is derived from solved model output only
- Credit, wealth, and experimental paths are not part of the published model scope
