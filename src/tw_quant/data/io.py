"""File-based data path helpers for the scaffold."""

from __future__ import annotations

from tw_quant.core.models import DataPaths


def prepare_data_paths(data_paths: DataPaths) -> DataPaths:
    """Ensure local directories exist for data and generated artifacts."""

    data_paths.raw_dir.mkdir(parents=True, exist_ok=True)
    data_paths.processed_dir.mkdir(parents=True, exist_ok=True)
    data_paths.reports_dir.mkdir(parents=True, exist_ok=True)
    return data_paths
