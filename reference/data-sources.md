# Data Sources

`fp-ineq` stores normalized quarterly inputs, calibration targets, and provenance metadata in the repository.

The checked-in public data surface has two parts:

- `data/series/` contains CSV snapshots used by the overlay
- `data/reports/` contains provenance JSON for those snapshots

The project uses checked-in public data so the overlay can be shared and rerun without a separate private data-preparation repository. Runtime `*.DAT` files are derived from those checked-in CSVs.

## Coverage and normalization

The published output targets use annual anchor values that begin in `2015`.

`refresh-data` writes those output targets only across their observed quarterly span. It does not publish synthetic pre-2015 target history.

The staged series used by the broader overlay project are:

- outputs: `IPOVALL`, `IPOVCH`, `IGINIHH`, `IMEDRINC`, `IWGAP1050`, `IWGAP150`
- inputs: `ITRCOMP`, `IUIBEN`, `ISSBEN`, `ISNAP`, `ICRDCMP`, `IHHNW`, `IHOMEQ`, `IFFUNDS`

The public results explorer is narrower than the full staged data surface. It excludes dormant composite, wealth, and neutral credit-shadow series that are not part of the published run bundle.

## What each provenance report includes

Each report JSON records:

- source name
- source URL
- units
- first and last quarter in the checked-in file
- transformation notes
- refresh timestamp
