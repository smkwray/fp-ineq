from __future__ import annotations

from pathlib import Path

from fp_ineq.data_pipeline import refresh_data
from fp_ineq.paths import repo_paths


def test_refresh_data_writes_public_series_and_runtime_dat() -> None:
    payload = refresh_data()
    paths = repo_paths()
    assert payload["series_count"] == 14
    assert (paths.data_series_root / "poverty_all_qtr.csv").exists()
    assert (paths.data_reports_root / "poverty_all_qtr.json").exists()
    assert (paths.runtime_overlay_root / "IPOVALL.DAT").exists()
    assert (paths.runtime_overlay_root / "ITRCOMP_R.DAT").exists()
    assert (paths.runtime_overlay_root / "IWG1050.DAT").exists()
    sample = (paths.runtime_overlay_root / "IPOVALL.DAT").read_text(encoding="utf-8")
    assert "LOAD IPOVALL" in sample
    assert "2025.4" in sample
    assert "2015.1" in sample
    assert "END;" in sample
    poverty_csv = (paths.data_series_root / "poverty_all_qtr.csv").read_text(encoding="utf-8")
    assert "1990.1" not in poverty_csv
    assert "2015.1" in poverty_csv
    poverty_report = (paths.data_reports_root / "poverty_all_qtr.json").read_text(encoding="utf-8")
    assert '"observed_start": "2015.1"' in poverty_report
    assert '"observed_end": "2025.4"' in poverty_report
    wealth_gap_sample = (paths.runtime_overlay_root / "IWG1050.DAT").read_text(encoding="utf-8")
    assert "LOAD IWG1050" in wealth_gap_sample
