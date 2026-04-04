from __future__ import annotations

import os
import sys
from pathlib import Path

from .paths import repo_paths

__all__ = [
    "ensure_fp_wraptr_importable",
    "locate_fp_home",
    "locate_fp_wraptr_root",
]


def locate_fp_wraptr_root(explicit: Path | str | None = None) -> Path:
    candidates: list[Path] = []
    if explicit is not None:
        candidates.append(Path(explicit).expanduser().resolve())
    env_root = os.environ.get("FP_WRAPTR_ROOT", "").strip()
    if env_root:
        candidates.append(Path(env_root).expanduser().resolve())
    paths = repo_paths()
    candidates.append((paths.repo_root.parent / "fp-wraptr").resolve())

    for candidate in candidates:
        if (candidate / "src" / "fp_wraptr").exists():
            return candidate
    raise FileNotFoundError(
        "Unable to locate fp-wraptr. Set FP_WRAPTR_ROOT or place fp-wraptr next to fp-ineq."
    )


def ensure_fp_wraptr_importable(explicit: Path | str | None = None) -> Path:
    root = locate_fp_wraptr_root(explicit)
    src = root / "src"
    token = str(src)
    if token not in sys.path:
        sys.path.insert(0, token)
    return root


def locate_fp_home(explicit: Path | str | None = None) -> Path:
    candidates: list[Path] = []
    if explicit is not None:
        candidates.append(Path(explicit).expanduser().resolve())
    env_home = os.environ.get("FP_HOME", "").strip()
    if env_home:
        candidates.append(Path(env_home).expanduser().resolve())
    for candidate in candidates:
        if (candidate / "fminput.txt").exists():
            return candidate
    raise FileNotFoundError(
        "Unable to locate stock Fair fp_home. Pass --fp-home or set FP_HOME."
    )

