from __future__ import annotations

import yaml

from fp_ineq.export import _phase1_solved_runs
from fp_ineq.paths import repo_paths


def test_public_phase1_strings_avoid_forbidden_fair_language() -> None:
    semantics_path = repo_paths().reference_root / "fair_variable_semantics.yaml"
    semantics = yaml.safe_load(semantics_path.read_text(encoding="utf-8"))

    public_strings = []
    for run_spec in _phase1_solved_runs():
        public_strings.extend(
            [
                str(run_spec["label"]),
                str(run_spec["summary"]),
                str(run_spec["group"]),
            ]
        )
    public_blob = "\n".join(public_strings).lower()

    for variable_name, entry in dict(semantics).items():
        for forbidden_phrase in list(dict(entry).get("avoid_public", [])):
            phrase = str(forbidden_phrase).strip().lower()
            assert phrase not in public_blob, f"{variable_name} forbidden public phrase leaked: {forbidden_phrase!r}"
