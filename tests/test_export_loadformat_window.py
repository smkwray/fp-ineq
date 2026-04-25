from __future__ import annotations

import csv
import sys
import types
from pathlib import Path

import pytest

from fp_ineq.export import _loadformat_window


def test_loadformat_window_replaces_sentinel_trlowz_and_derives_rydpc(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loadformat_path = tmp_path / "LOADFORMAT.DAT"
    loadformat_path.write_text("placeholder\n", encoding="utf-8")
    fp_r_series_path = tmp_path / "fp_r_series.csv"
    with fp_r_series_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["period", "YD", "POP", "PH", "UB", "TRGH", "TRSH"],
        )
        writer.writeheader()
        writer.writerow({"period": "2026.1", "YD": "60", "POP": "100", "PH": "0.2", "UB": "1", "TRGH": "2", "TRSH": "3"})
        writer.writerow({"period": "2026.2", "YD": "66", "POP": "110", "PH": "0.2", "UB": "2", "TRGH": "3", "TRSH": "4"})

    fake_loadformat = types.ModuleType("fp_wraptr.io.loadformat")

    def read_loadformat(_path: Path) -> tuple[list[str], dict[str, list[float]]]:
        return ["2026.1", "2026.2"], {
            "TRLOWZ": [-99.0, -99.0],
            "RYDPC": [-99.0, -99.0],
            "YD": [60.0, 66.0],
            "POP": [100.0, 110.0],
            "PH": [0.2, 0.2],
            "UB": [1.0, 2.0],
            "TRGH": [2.0, 3.0],
            "TRSH": [3.0, 4.0],
        }

    def add_derived_series(series: dict[str, list[float]]) -> dict[str, list[float]]:
        return series

    fake_loadformat.read_loadformat = read_loadformat
    fake_loadformat.add_derived_series = add_derived_series

    monkeypatch.setattr("fp_ineq.export.ensure_fp_wraptr_importable", lambda: None)
    monkeypatch.setitem(sys.modules, "fp_wraptr", types.ModuleType("fp_wraptr"))
    monkeypatch.setitem(sys.modules, "fp_wraptr.io", types.ModuleType("fp_wraptr.io"))
    monkeypatch.setitem(sys.modules, "fp_wraptr.io.loadformat", fake_loadformat)

    periods, series = _loadformat_window(
        loadformat_path,
        variables=["TRLOWZ", "RYDPC"],
        forecast_start="2026.1",
        forecast_end="2026.2",
    )

    assert periods == ["2026.1", "2026.2"]
    assert series["TRLOWZ"] == pytest.approx([0.3, 0.4090909091])
    assert series["RYDPC"] == pytest.approx([3.0, 3.0])
