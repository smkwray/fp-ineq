from __future__ import annotations

import copy
import shutil
from pathlib import Path
from typing import Any

import yaml

from .paths import repo_paths

__all__ = [
    "DEFAULT_RUNTIME_INCLUDE_NAMES",
    "_apply_manifest_patches",
    "_load_phase1_manifest",
    "_replace_once",
    "_sanitize_runtime_include",
    "compose_phase1_overlay",
]


DEFAULT_RUNTIME_INCLUDE_NAMES = {
    "ipolicy_base.txt": "ipolbase.txt",
    "idist_identities.txt": "idistid.txt",
}


def _load_phase1_manifest() -> dict[str, Any]:
    paths = repo_paths()
    manifest_path = paths.overlay_source_root / "stock_patch_manifest.phase1.yaml"
    return yaml.safe_load(manifest_path.read_text(encoding="utf-8"))


def _replace_once(text: str, search: str, replace: str) -> str:
    count = text.count(search)
    if count != 1:
        raise ValueError(f"Expected exactly one match for patch anchor: {search!r}; found {count}")
    return text.replace(search, replace, 1)


def _apply_manifest_patches(
    stock_text: str,
    manifest: dict[str, Any],
    *,
    experimental_patch_ids: list[str] | tuple[str, ...] = (),
) -> str:
    text = stock_text
    for patch in manifest.get("patches", []):
        kind = str(patch.get("kind", "")).strip()
        if kind != "literal_replace":
            raise ValueError(f"Unsupported phase1 patch kind: {kind}")
        text = _replace_once(text, str(patch["search"]), str(patch["replace"]))
    if experimental_patch_ids:
        experimental_patches = {
            str(patch.get("id", "")).strip(): patch for patch in manifest.get("experimental_patches", [])
        }
        missing = [patch_id for patch_id in experimental_patch_ids if patch_id not in experimental_patches]
        if missing:
            raise ValueError(f"Unknown experimental phase1 patch ids: {', '.join(missing)}")
        for patch_id in experimental_patch_ids:
            patch = experimental_patches[patch_id]
            kind = str(patch.get("kind", "")).strip()
            if kind == "literal_replace_group":
                for replacement in patch.get("replacements", []):
                    text = _replace_once(text, str(replacement["search"]), str(replacement["replace"]))
                continue
            if kind == "literal_replace":
                text = _replace_once(text, str(patch["search"]), str(patch["replace"]))
                continue
            raise ValueError(f"Unsupported experimental phase1 patch kind: {kind}")
    return text


def _sanitize_runtime_include(text: str) -> str:
    lines = [line for line in text.splitlines() if not line.lstrip().startswith("@")]
    body = "\n".join(lines).strip()
    return body + "\n"


def _clean_overlay_root(overlay_root: Path) -> None:
    if overlay_root.exists():
        shutil.rmtree(overlay_root)
    overlay_root.mkdir(parents=True, exist_ok=True)


def compose_phase1_overlay(
    *,
    fp_home: Path,
    overlay_root: Path,
    extra_overlay_files: list[str] | tuple[str, ...] = (),
    runtime_name_overrides: dict[str, str] | None = None,
    runtime_text_files: dict[str, str] | None = None,
    post_patches: list[dict[str, str]] | tuple[dict[str, str], ...] = (),
    experimental_patch_ids: list[str] | tuple[str, ...] = (),
) -> dict[str, object]:
    paths = repo_paths()
    manifest = copy.deepcopy(_load_phase1_manifest())
    runtime_name_overrides = {**DEFAULT_RUNTIME_INCLUDE_NAMES, **(runtime_name_overrides or {})}
    runtime_text_files = runtime_text_files or {}

    _clean_overlay_root(overlay_root)

    staged_files = [str(name) for name in manifest.get("entry_overlay_files", [])]
    staged_files.extend(str(name) for name in extra_overlay_files)
    source_to_runtime: dict[str, str] = {}
    staged_entries: list[dict[str, str]] = []

    for file_name in staged_files:
        source = paths.overlay_source_root / file_name
        runtime_name = runtime_name_overrides.get(file_name, file_name)
        target = overlay_root / runtime_name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            _sanitize_runtime_include(source.read_text(encoding="utf-8", errors="replace")),
            encoding="utf-8",
        )
        source_to_runtime[file_name] = runtime_name
        staged_entries.append({"source_name": file_name, "runtime_name": runtime_name})

    for runtime_name, text in runtime_text_files.items():
        target = overlay_root / runtime_name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

    stock_input = (fp_home / "fminput.txt").read_text(encoding="utf-8", errors="replace")
    composed = _apply_manifest_patches(
        stock_input,
        manifest,
        experimental_patch_ids=experimental_patch_ids,
    )
    for source_name, runtime_name in source_to_runtime.items():
        composed = composed.replace(source_name, runtime_name)
    for patch in post_patches:
        composed = _replace_once(composed, str(patch["search"]), str(patch["replace"]))
    (overlay_root / "fminput.txt").write_text(composed, encoding="utf-8")

    return {
        "overlay_root": str(overlay_root),
        "entry_files": staged_entries,
        "patch_ids": [
            *[str(item.get("id", "")) for item in manifest.get("patches", [])],
            *[str(item) for item in experimental_patch_ids],
        ],
        "runtime_text_files": sorted(runtime_text_files),
    }
