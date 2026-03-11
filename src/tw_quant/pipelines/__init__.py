"""Workflow orchestration entrypoints."""

from tw_quant.pipelines.backtest import execute_backtest
from tw_quant.pipelines.diagnostics import execute_diagnostics
from tw_quant.pipelines.ingest import execute_ingest
from tw_quant.pipelines.signals import execute_signals
from tw_quant.pipelines.walkforward import execute_walkforward

__all__ = [
    "execute_backtest",
    "execute_diagnostics",
    "execute_ingest",
    "execute_signals",
    "execute_walkforward",
]
