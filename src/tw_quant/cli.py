"""Command-line entrypoints for tw_quant."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from tw_quant.pipelines.backtest import execute_backtest
from tw_quant.pipelines.ingest import execute_ingest
from tw_quant.pipelines.signals import execute_signals

try:
    import typer
except ModuleNotFoundError:  # pragma: no cover - exercised in local stdlib-only environments
    typer = None


def _print_backtest_summary(config_path: Path) -> int:
    result = execute_backtest(config_path)
    print(result.summary_text_zh())
    return 0


def _print_ingest_summary(config_path: Path, refresh: bool = False) -> int:
    result = execute_ingest(config_path, force_refresh=refresh)
    print(result.summary_text_zh())
    return 0


def _print_signal_summary(config_path: Path) -> int:
    result = execute_signals(config_path)
    print(result.summary_text_zh())
    return 0


if typer is not None:
    app = typer.Typer(
        help="Taiwan equities quantitative research system.",
        no_args_is_help=True,
        add_completion=False,
    )

    @app.command("backtest")
    def backtest_command(
        config: Path = typer.Option(
            ...,
            "--config",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Path to a TOML settings file.",
        ),
    ) -> None:
        """Run the local-data backtest pipeline."""
        result = execute_backtest(config)
        typer.echo(result.summary_text_zh())

    @app.command("ingest")
    def ingest_command(
        config: Path = typer.Option(
            ...,
            "--config",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Path to a TOML settings file.",
        ),
        refresh: bool = typer.Option(
            False,
            "--refresh",
            help="Ignore cached files and refetch data.",
        ),
    ) -> None:
        """Run the market data ingestion pipeline."""
        result = execute_ingest(config, force_refresh=refresh)
        typer.echo(result.summary_text_zh())

    @app.command("signals")
    def signals_command(
        config: Path = typer.Option(
            ...,
            "--config",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Path to a TOML settings file.",
        ),
    ) -> None:
        """Load normalized bars, compute signals, and write the signal panel."""
        result = execute_signals(config)
        typer.echo(result.summary_text_zh())


def _build_argparse_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="twq",
        description="Taiwan equities quantitative research system.",
    )
    subparsers = parser.add_subparsers(dest="command")

    backtest_parser = subparsers.add_parser(
        "backtest",
        help="Run the local-data backtest pipeline.",
    )
    backtest_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a TOML settings file.",
    )
    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Fetch, normalize, and cache market data.",
    )
    ingest_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a TOML settings file.",
    )
    ingest_parser.add_argument(
        "--refresh",
        action="store_true",
        help="Ignore cached files and refetch data.",
    )
    signals_parser = subparsers.add_parser(
        "signals",
        help="Load normalized data and generate signal outputs.",
    )
    signals_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a TOML settings file.",
    )
    return parser


def _run_argparse(argv: Sequence[str] | None = None) -> int:
    parser = _build_argparse_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "backtest":
        return _print_backtest_summary(args.config)
    if args.command == "ingest":
        return _print_ingest_summary(args.config, refresh=bool(args.refresh))
    if args.command == "signals":
        return _print_signal_summary(args.config)

    parser.print_help()
    return 0


def run(argv: Sequence[str] | None = None) -> int:
    """Run the CLI.

    Typer is used when available through installed project dependencies.
    A small argparse fallback keeps the CLI runnable in stdlib-only environments.
    """

    if typer is None:
        return _run_argparse(argv)

    if argv is None:
        app()
        return 0

    app(args=list(argv), prog_name="twq")
    return 0
