#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FP_WRAPTR_ROOT="${FP_WRAPTR_ROOT:-$(cd "${REPO_ROOT}/../fp-wraptr" 2>/dev/null && pwd || true)}"
PYTHON_BIN="${PYTHON_BIN:-python}"

if [[ -z "${FP_WRAPTR_ROOT}" || ! -d "${FP_WRAPTR_ROOT}" ]]; then
  echo "FP_WRAPTR_ROOT must point to a valid fp-wraptr checkout." >&2
  exit 1
fi

PYTHONPATH="${REPO_ROOT}/src:${FP_WRAPTR_ROOT}/src" \
FP_WRAPTR_ROOT="${FP_WRAPTR_ROOT}" \
"${PYTHON_BIN}" - <<'PY'
from fp_ineq.export import export_phase1_full_bundle, publish_phase1_bundle_to_docs

bundle = export_phase1_full_bundle()
published = publish_phase1_bundle_to_docs()
print(bundle["out_dir"])
print(published["docs_dir"])
print(published["run_count"], published["variable_count"])
PY
