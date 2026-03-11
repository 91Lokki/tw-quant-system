"""Streamlit demo app for local tw_quant artifacts."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import streamlit as st

from app.demo_data import (
    build_artifact_status_rows,
    build_latest_portfolio_snapshot,
    build_project_paths,
    discover_projects,
    get_project_root,
    list_directory_files,
    load_markdown,
    load_nav_frame,
    load_summary_metrics,
    load_svg,
    load_weights_frame,
    summarize_artifact_status_rows,
)

LANGUAGE_OPTIONS = {
    "繁體中文": "zh-TW",
    "English": "en",
}

TRANSLATIONS: dict[str, dict[str, object]] = {
    "zh-TW": {
        "page_title": "tw_quant 互動展示",
        "sidebar_title": "展示控制",
        "project_label": "專案",
        "recent_rows_label": "最近顯示筆數",
        "refresh_button": "重新整理畫面",
        "sidebar_caption": "此展示只讀取本地 artifacts，不會重新執行量化引擎。",
        "hero_badge": "台股量化研究作品集展示",
        "hero_title": "tw_quant 本地互動式 Demo",
        "hero_subtitle": (
            "這個頁面以既有本地 artifacts 為基礎，幫助教授、面試官或 reviewer 快速理解"
            "資料流程、回測結果與目前的投組狀態。"
        ),
        "selected_project": "目前專案",
        "latest_update": "最近更新",
        "demo_focus": "展示定位",
        "stable_commands": "穩定啟動指令",
        "kpi_title": "核心績效摘要",
        "kpi_subtitle": "以下數值來自最新的本地回測報表。",
        "project_metric": "專案",
        "run_period": "回測期間",
        "tradable_symbols": "可交易標的",
        "benchmark": "基準指標",
        "final_nav": "最終 NAV",
        "cumulative_return": "累積報酬",
        "annualized_return": "年化報酬",
        "annualized_volatility": "年化波動",
        "sharpe_ratio": "Sharpe 比率",
        "max_drawdown": "最大回撤",
        "turnover": "累積換手",
        "benchmark_final_nav": "基準最終 NAV",
        "rebalance_frequency": "再平衡頻率",
        "transaction_costs": "交易成本設定",
        "pipeline_title": "Pipeline 概觀",
        "pipeline_subtitle": "這個 demo 顯示目前研究系統如何由資料一路走到報表。",
        "ready": "已就緒",
        "missing": "缺少 artifact",
        "artifacts_title": "Artifact 狀態與新鮮度",
        "artifacts_subtitle": "用本地檔案存在狀態與修改時間快速檢查目前 pipeline 是否完整。",
        "present_artifacts": "已存在 artifacts",
        "missing_artifacts": "缺失 artifacts",
        "artifact_total": "總 artifact 數",
        "latest_artifact_update": "最新 artifact 更新",
        "artifact_column": "Artifact",
        "status_column": "狀態",
        "path_column": "路徑",
        "modified_column": "最後更新",
        "size_column": "大小 (KB)",
        "symbol_column": "標的",
        "weight_column": "權重",
        "signal_score_column": "Signal Score",
        "daily_return_column": "單日報酬",
        "gross_return_column": "資產報酬",
        "benchmark_return_column": "基準報酬",
        "benchmark_nav_column": "基準 NAV",
        "transaction_cost_column": "交易成本",
        "normalized_files": "正規化市場資料",
        "signal_files": "訊號輸出",
        "report_files": "回測與報表 artifacts",
        "no_normalized_files": "目前找不到正規化市場資料檔案。",
        "no_signal_files": "目前找不到訊號輸出檔案。",
        "no_report_files": "目前找不到所選專案的報表 artifacts。",
        "charts_title": "圖表與時序檢視",
        "charts_subtitle": "先看互動式 NAV，再看既有回測圖表 artifacts。",
        "interactive_nav_chart": "互動式 NAV / Benchmark 折線圖",
        "equity_curve": "權益曲線圖",
        "drawdown": "回撤圖",
        "missing_equity_curve": "目前找不到權益曲線圖 artifact。",
        "missing_drawdown": "目前找不到回撤圖 artifact。",
        "snapshot_title": "最新投組快照",
        "snapshot_subtitle": "把最新一個交易日的持倉、權重、signal score 與現金部位整理成易讀摘要。",
        "latest_date": "最新日期",
        "active_positions": "持有部位數",
        "gross_exposure": "總曝險",
        "cash_weight": "現金權重",
        "held_symbols": "目前持有標的",
        "held_positions_table": "目前持有部位",
        "latest_weights_table": "最新日期完整權重",
        "no_active_positions": "最新日期沒有持有部位，投組目前應為現金或無有效曝險。",
        "no_weights": "目前找不到 daily_weights artifact。",
        "tables_title": "最近資料表",
        "tables_subtitle": "保留原始輸出表格，方便快速檢查最近的 NAV 與權重變化。",
        "recent_nav_rows": "最近 NAV 資料",
        "recent_weight_rows": "最近權重資料",
        "no_nav": "目前找不到 daily_nav artifact。",
        "notes_title": "展示重點與限制",
        "notes_subtitle": "這個 app 的目的是幫助 reviewer 快速理解專案，而不是重建量化引擎。",
        "report_preview_title": "Markdown 報表預覽",
        "report_preview_open": "展開報表摘要",
        "no_report": "目前找不到 markdown 回測報表。",
        "status_present": "存在",
        "status_missing": "缺失",
        "not_available": "未提供",
        "project_description": "本地 artifact 驅動的台股量化研究展示介面",
        "pipeline_stages": (
            "資料擷取",
            "正規化資料",
            "訊號",
            "投組",
            "回測",
            "報表",
        ),
        "limitations": (
            "此 app 只展示既有本地 artifacts，不會重新執行資料擷取、訊號或回測。",
            "目前策略邏輯仍是刻意保持簡單的 long-only、equal-weight v1 研究流程。",
            "Benchmark 使用的是目前已正規化的 TAIEX proxy 資料，而不是完整 benchmark OHLCV 系列。",
        ),
    },
    "en": {
        "page_title": "tw_quant Interactive Demo",
        "sidebar_title": "Demo Controls",
        "project_label": "Project",
        "recent_rows_label": "Recent rows",
        "refresh_button": "Refresh view",
        "sidebar_caption": "This demo reads local artifacts only. It does not rerun the quant engine.",
        "hero_badge": "Taiwan equities quant portfolio demo",
        "hero_title": "tw_quant Local Interactive Demo",
        "hero_subtitle": (
            "This page uses existing local artifacts to help a professor, interviewer, or reviewer "
            "quickly understand the pipeline, the backtest results, and the latest portfolio state."
        ),
        "selected_project": "Selected project",
        "latest_update": "Latest update",
        "demo_focus": "Demo focus",
        "stable_commands": "Stable launch commands",
        "kpi_title": "Top-Level KPI Summary",
        "kpi_subtitle": "These values are read from the latest local backtest report.",
        "project_metric": "Project",
        "run_period": "Run Period",
        "tradable_symbols": "Tradable Symbols",
        "benchmark": "Benchmark",
        "final_nav": "Final NAV",
        "cumulative_return": "Cumulative Return",
        "annualized_return": "Annualized Return",
        "annualized_volatility": "Annualized Volatility",
        "sharpe_ratio": "Sharpe Ratio",
        "max_drawdown": "Max Drawdown",
        "turnover": "Turnover",
        "benchmark_final_nav": "Benchmark Final NAV",
        "rebalance_frequency": "Rebalance Frequency",
        "transaction_costs": "Transaction Costs",
        "pipeline_title": "Pipeline Overview",
        "pipeline_subtitle": "This demo shows how the current research system moves from data to reports.",
        "ready": "Ready",
        "missing": "Missing",
        "artifacts_title": "Artifact Status and Freshness",
        "artifacts_subtitle": "Use local file presence and timestamps to quickly understand pipeline readiness.",
        "present_artifacts": "Present artifacts",
        "missing_artifacts": "Missing artifacts",
        "artifact_total": "Total artifacts",
        "latest_artifact_update": "Latest artifact update",
        "artifact_column": "Artifact",
        "status_column": "Status",
        "path_column": "Path",
        "modified_column": "Last modified",
        "size_column": "Size (KB)",
        "symbol_column": "Symbol",
        "weight_column": "Weight",
        "signal_score_column": "Signal Score",
        "daily_return_column": "Daily Return",
        "gross_return_column": "Gross Return",
        "benchmark_return_column": "Benchmark Return",
        "benchmark_nav_column": "Benchmark NAV",
        "transaction_cost_column": "Transaction Cost",
        "normalized_files": "Normalized market data",
        "signal_files": "Signal outputs",
        "report_files": "Backtest and report artifacts",
        "no_normalized_files": "No normalized market data files were found.",
        "no_signal_files": "No signal output files were found.",
        "no_report_files": "No report artifacts were found for the selected project.",
        "charts_title": "Charts and Time Series",
        "charts_subtitle": "Start with the interactive NAV chart, then inspect the persisted report visuals.",
        "interactive_nav_chart": "Interactive NAV / Benchmark chart",
        "equity_curve": "Equity Curve",
        "drawdown": "Drawdown",
        "missing_equity_curve": "No equity curve artifact was found.",
        "missing_drawdown": "No drawdown artifact was found.",
        "snapshot_title": "Latest Portfolio Snapshot",
        "snapshot_subtitle": "Summarize the most recent portfolio date, positions, weights, signal scores, and cash weight.",
        "latest_date": "Latest date",
        "active_positions": "Active positions",
        "gross_exposure": "Gross exposure",
        "cash_weight": "Cash weight",
        "held_symbols": "Currently held symbols",
        "held_positions_table": "Held positions",
        "latest_weights_table": "Full latest-day weights",
        "no_active_positions": "There are no active holdings on the latest date; the portfolio is effectively in cash or inactive.",
        "no_weights": "No daily_weights artifact was found.",
        "tables_title": "Recent Tables",
        "tables_subtitle": "Keep the raw output tables visible for quick inspection of recent NAV and weight changes.",
        "recent_nav_rows": "Recent NAV rows",
        "recent_weight_rows": "Recent weight rows",
        "no_nav": "No daily_nav artifact was found.",
        "notes_title": "Demo Notes and Limitations",
        "notes_subtitle": "This app is meant to explain the project clearly, not to rebuild the quant engine.",
        "report_preview_title": "Markdown Report Preview",
        "report_preview_open": "Open markdown summary",
        "no_report": "No markdown backtest report was found.",
        "status_present": "Present",
        "status_missing": "Missing",
        "not_available": "Not available",
        "project_description": "A local artifact-driven demo interface for Taiwan equities quant research",
        "pipeline_stages": (
            "Ingest",
            "Normalized Bars",
            "Signals",
            "Portfolio",
            "Backtest",
            "Reports",
        ),
        "limitations": (
            "This app only reads existing local artifacts and does not rerun ingestion, signals, or backtesting.",
            "The current strategy logic is intentionally simple: a long-only, equal-weight v1 research workflow.",
            "The benchmark view relies on the normalized TAIEX proxy series rather than a full benchmark OHLCV history.",
        ),
    },
}

PIPELINE_COMMANDS = """uv run python -m tw_quant ingest --config configs/settings.example.toml
uv run python -m tw_quant signals --config configs/settings.example.toml
uv run python -m tw_quant backtest --config configs/settings.example.toml
uv run python -m streamlit run app/streamlit_app.py"""


def main() -> None:
    st.set_page_config(
        page_title="tw_quant Demo",
        page_icon="TW",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _inject_styles()

    project_root = get_project_root()
    project_names = discover_projects(project_root)
    default_project = project_names[0] if project_names else "tw_quant_v1"

    with st.sidebar:
        language_label = st.selectbox(
            "Language / 語言",
            options=list(LANGUAGE_OPTIONS.keys()),
            index=0,
        )
        language = LANGUAGE_OPTIONS[language_label]
        t = _translator(language)

        st.title(str(t("sidebar_title")))
        selected_project = st.selectbox(
            str(t("project_label")),
            options=list(project_names) if project_names else [default_project],
            index=0,
        )
        recent_rows = st.slider(str(t("recent_rows_label")), min_value=5, max_value=100, value=20, step=5)
        if st.button(str(t("refresh_button")), use_container_width=True):
            st.rerun()
        st.caption(str(t("sidebar_caption")))

    paths = build_project_paths(project_root, selected_project)
    nav_frame = load_nav_frame(paths["nav"])
    weights_frame = load_weights_frame(paths["weights"])
    summary_metrics = load_summary_metrics(paths["report"])
    status_rows = build_artifact_status_rows(project_root, selected_project)
    status_summary = summarize_artifact_status_rows(status_rows)

    normalized_files = list_directory_files(paths["normalized_dir"], project_root, suffix=".csv")
    signal_files = list_directory_files(paths["signal_panel"].parent, project_root, suffix=".csv")
    report_files = list_directory_files(paths["report_dir"], project_root)

    snapshot = build_latest_portfolio_snapshot(
        weight_rows=weights_frame.to_dict("records") if weights_frame is not None else [],
        nav_rows=nav_frame.to_dict("records") if nav_frame is not None else None,
    )

    _render_hero(language, selected_project, status_summary)
    _render_kpi_section(language, selected_project, summary_metrics)
    _render_pipeline_section(language, normalized_files, paths)
    _render_artifact_section(language, status_rows, status_summary, normalized_files, signal_files, report_files)
    _render_chart_section(language, paths, nav_frame)
    _render_snapshot_section(language, snapshot)
    _render_tables_section(language, nav_frame, weights_frame, recent_rows)
    _render_notes_section(language)
    _render_report_preview(language, paths["report"])


def _render_hero(language: str, selected_project: str, status_summary: dict[str, str | int]) -> None:
    t = _translator(language)
    latest_update = status_summary["latest_modified_at"]
    latest_update_text = latest_update if latest_update != "-" else str(t("not_available"))

    left_col, right_col = st.columns([1.35, 1.0], gap="large")
    with left_col:
        st.markdown(
            (
                '<div class="hero-card">'
                f'<div class="hero-badge">{t("hero_badge")}</div>'
                f'<div class="hero-title">{t("hero_title")}</div>'
                f'<div class="hero-subtitle">{t("hero_subtitle")}</div>'
                '<div class="hero-meta-wrap">'
                f'<div class="hero-meta"><span class="hero-meta-label">{t("selected_project")}</span>'
                f'<span class="hero-meta-value">{selected_project}</span></div>'
                f'<div class="hero-meta"><span class="hero-meta-label">{t("latest_update")}</span>'
                f'<span class="hero-meta-value">{latest_update_text}</span></div>'
                f'<div class="hero-meta"><span class="hero-meta-label">{t("demo_focus")}</span>'
                f'<span class="hero-meta-value">{t("project_description")}</span></div>'
                "</div></div>"
            ),
            unsafe_allow_html=True,
        )

    with right_col:
        st.markdown(f"**{t('stable_commands')}**")
        st.code(PIPELINE_COMMANDS, language="bash")


def _render_kpi_section(language: str, selected_project: str, summary_metrics: dict[str, str]) -> None:
    t = _translator(language)
    st.subheader(str(t("kpi_title")))
    st.caption(str(t("kpi_subtitle")))

    if not summary_metrics:
        st.warning(str(t("no_report")))
        return

    top_cols = st.columns(4)
    top_cols[0].metric(str(t("project_metric")), selected_project)
    top_cols[1].metric(str(t("run_period")), summary_metrics.get("Run Period", "-"))
    top_cols[2].metric(str(t("tradable_symbols")), summary_metrics.get("Tradable Symbols", "-"))
    top_cols[3].metric(str(t("benchmark")), summary_metrics.get("Benchmark", "-"))

    kpi_rows = [
        ("final_nav", "Final NAV"),
        ("cumulative_return", "Cumulative Return"),
        ("annualized_return", "Annualized Return"),
        ("annualized_volatility", "Annualized Volatility"),
        ("sharpe_ratio", "Sharpe Ratio"),
        ("max_drawdown", "Max Drawdown"),
        ("turnover", "Turnover"),
        ("benchmark_final_nav", "Benchmark Final NAV"),
    ]
    metric_cols = st.columns(4)
    for index, (label_key, report_key) in enumerate(kpi_rows):
        metric_cols[index % 4].metric(str(t(label_key)), summary_metrics.get(report_key, "-"))
        if index % 4 == 3 and index < len(kpi_rows) - 1:
            metric_cols = st.columns(4)

    detail_col_1, detail_col_2 = st.columns(2)
    detail_col_1.caption(f"{t('rebalance_frequency')}: {summary_metrics.get('Rebalance Frequency', '-')}")
    detail_col_2.caption(f"{t('transaction_costs')}: {summary_metrics.get('Transaction Costs', '-')}")


def _render_pipeline_section(language: str, normalized_files: pd.DataFrame, paths: dict[str, Path]) -> None:
    t = _translator(language)
    st.subheader(str(t("pipeline_title")))
    st.caption(str(t("pipeline_subtitle")))

    stage_states = (
        normalized_files.shape[0] > 0,
        normalized_files.shape[0] > 0,
        paths["signal_panel"].exists(),
        paths["weights"].exists(),
        paths["nav"].exists(),
        paths["report"].exists() and paths["equity_curve"].exists() and paths["drawdown"].exists(),
    )
    stage_cards: list[str] = []
    for stage_name, is_ready in zip(t("pipeline_stages"), stage_states, strict=True):
        status_text = t("ready") if is_ready else t("missing")
        status_class = "stage-ready" if is_ready else "stage-missing"
        stage_cards.append(
            (
                '<div class="stage-card">'
                f'<div class="stage-name">{stage_name}</div>'
                f'<div class="stage-status {status_class}">{status_text}</div>'
                "</div>"
            )
        )

    st.markdown(f'<div class="stage-grid">{"".join(stage_cards)}</div>', unsafe_allow_html=True)


def _render_artifact_section(
    language: str,
    status_rows: list[dict[str, str]],
    status_summary: dict[str, str | int],
    normalized_files: pd.DataFrame,
    signal_files: pd.DataFrame,
    report_files: pd.DataFrame,
) -> None:
    t = _translator(language)
    st.subheader(str(t("artifacts_title")))
    st.caption(str(t("artifacts_subtitle")))

    freshness_cols = st.columns(4)
    freshness_cols[0].metric(str(t("present_artifacts")), f"{status_summary['present_count']}")
    freshness_cols[1].metric(str(t("missing_artifacts")), f"{status_summary['missing_count']}")
    freshness_cols[2].metric(str(t("artifact_total")), f"{status_summary['total_count']}")
    freshness_cols[3].metric(
        str(t("latest_artifact_update")),
        (
            str(status_summary["latest_modified_at"])
            if status_summary["latest_modified_at"] != "-"
            else str(t("not_available"))
        ),
    )

    status_frame = pd.DataFrame(status_rows)
    if not status_frame.empty:
        status_frame["status"] = status_frame["status"].map(
            {"Present": str(t("status_present")), "Missing": str(t("status_missing"))}
        )
        status_frame.rename(
            columns={
                "artifact": str(t("artifact_column")),
                "status": str(t("status_column")),
                "path": str(t("path_column")),
                "modified_at": str(t("modified_column")),
                "size_kb": str(t("size_column")),
            },
            inplace=True,
        )
    st.dataframe(status_frame, use_container_width=True, hide_index=True)

    file_col_1, file_col_2, file_col_3 = st.columns(3, gap="large")
    with file_col_1:
        st.markdown(f"**{t('normalized_files')}**")
        if normalized_files.empty:
            st.warning(str(t("no_normalized_files")))
        else:
            st.dataframe(_localize_file_frame(normalized_files, language), use_container_width=True, hide_index=True)

    with file_col_2:
        st.markdown(f"**{t('signal_files')}**")
        if signal_files.empty:
            st.warning(str(t("no_signal_files")))
        else:
            st.dataframe(_localize_file_frame(signal_files, language), use_container_width=True, hide_index=True)

    with file_col_3:
        st.markdown(f"**{t('report_files')}**")
        if report_files.empty:
            st.warning(str(t("no_report_files")))
        else:
            st.dataframe(_localize_file_frame(report_files, language), use_container_width=True, hide_index=True)


def _render_chart_section(language: str, paths: dict[str, Path], nav_frame: pd.DataFrame | None) -> None:
    t = _translator(language)
    st.subheader(str(t("charts_title")))
    st.caption(str(t("charts_subtitle")))

    if nav_frame is None or nav_frame.empty:
        st.warning(str(t("no_nav")))
    else:
        chart_frame = nav_frame.set_index("date")[["nav", "benchmark_nav"]]
        st.markdown(f"**{t('interactive_nav_chart')}**")
        st.line_chart(chart_frame, use_container_width=True)

    left_col, right_col = st.columns(2, gap="large")
    with left_col:
        st.markdown(f"**{t('equity_curve')}**")
        _render_svg(paths["equity_curve"], str(t("missing_equity_curve")))

    with right_col:
        st.markdown(f"**{t('drawdown')}**")
        _render_svg(paths["drawdown"], str(t("missing_drawdown")))


def _render_snapshot_section(language: str, snapshot: dict[str, object]) -> None:
    t = _translator(language)
    st.subheader(str(t("snapshot_title")))
    st.caption(str(t("snapshot_subtitle")))

    latest_rows = list(snapshot["latest_rows"])
    if not latest_rows:
        st.warning(str(t("no_weights")))
        return

    cash_weight = snapshot["cash_weight"]
    cash_value = str(t("not_available")) if cash_weight is None else f"{float(cash_weight):.1%}"
    held_symbols = ", ".join(snapshot["held_symbols"]) if snapshot["held_symbols"] else str(t("not_available"))

    overview_cols = st.columns(4)
    overview_cols[0].metric(str(t("latest_date")), str(snapshot["latest_date"]))
    overview_cols[1].metric(str(t("active_positions")), str(snapshot["held_count"]))
    overview_cols[2].metric(str(t("gross_exposure")), f"{float(snapshot['gross_exposure']):.1%}")
    overview_cols[3].metric(str(t("cash_weight")), cash_value)

    st.markdown(f"**{t('held_symbols')}:** {held_symbols}")

    held_rows = list(snapshot["held_rows"])
    if held_rows:
        st.markdown(f"**{t('held_positions_table')}**")
        held_frame = pd.DataFrame(held_rows)
        st.dataframe(_localize_snapshot_frame(held_frame, language), use_container_width=True, hide_index=True)
    else:
        st.info(str(t("no_active_positions")))

    st.markdown(f"**{t('latest_weights_table')}**")
    latest_frame = pd.DataFrame(latest_rows)
    st.dataframe(_localize_snapshot_frame(latest_frame, language), use_container_width=True, hide_index=True)


def _render_tables_section(
    language: str,
    nav_frame: pd.DataFrame | None,
    weights_frame: pd.DataFrame | None,
    recent_rows: int,
) -> None:
    t = _translator(language)
    st.subheader(str(t("tables_title")))
    st.caption(str(t("tables_subtitle")))

    nav_col, weights_col = st.columns(2, gap="large")
    with nav_col:
        st.markdown(f"**{t('recent_nav_rows')}**")
        if nav_frame is None or nav_frame.empty:
            st.warning(str(t("no_nav")))
        else:
            recent_nav = nav_frame.sort_values("date", ascending=False).head(recent_rows).copy()
            recent_nav["date"] = recent_nav["date"].dt.strftime("%Y-%m-%d")
            st.dataframe(_localize_nav_frame(recent_nav, language), use_container_width=True, hide_index=True)

    with weights_col:
        st.markdown(f"**{t('recent_weight_rows')}**")
        if weights_frame is None or weights_frame.empty:
            st.warning(str(t("no_weights")))
        else:
            recent_weights = weights_frame.sort_values(["date", "symbol"], ascending=[False, True]).head(recent_rows).copy()
            recent_weights["date"] = recent_weights["date"].dt.strftime("%Y-%m-%d")
            st.dataframe(_localize_weight_frame(recent_weights, language), use_container_width=True, hide_index=True)


def _render_notes_section(language: str) -> None:
    t = _translator(language)
    st.subheader(str(t("notes_title")))
    st.caption(str(t("notes_subtitle")))
    for note in t("limitations"):
        st.markdown(f"- {note}")


def _render_report_preview(language: str, report_path: Path) -> None:
    t = _translator(language)
    st.subheader(str(t("report_preview_title")))
    report_text = load_markdown(report_path)
    if report_text is None:
        st.warning(str(t("no_report")))
        return

    with st.expander(str(t("report_preview_open")), expanded=False):
        st.markdown(report_text)


def _render_svg(svg_path: Path, missing_message: str) -> None:
    svg_text = load_svg(svg_path)
    if svg_text is None:
        st.warning(missing_message)
        return

    st.markdown(
        (
            '<div style="background:#ffffff;border:1px solid #e2e8f0;'
            'border-radius:16px;padding:14px;overflow-x:auto;">'
            f"{svg_text}</div>"
        ),
        unsafe_allow_html=True,
    )


def _localize_file_frame(frame: pd.DataFrame, language: str) -> pd.DataFrame:
    t = _translator(language)
    display = frame.copy()
    display.rename(
        columns={
            "name": str(t("artifact_column")),
            "path": str(t("path_column")),
            "modified_at": str(t("modified_column")),
            "size_kb": str(t("size_column")),
        },
        inplace=True,
    )
    return display


def _localize_snapshot_frame(frame: pd.DataFrame, language: str) -> pd.DataFrame:
    t = _translator(language)
    display = frame.copy()
    if "weight" in display:
        display["weight"] = display["weight"].map(lambda value: f"{float(value):.1%}")
    if "signal_score" in display:
        display["signal_score"] = display["signal_score"].map(
            lambda value: "" if pd.isna(value) else f"{float(value):.2f}"
        )
    display.rename(
        columns={
            "date": str(t("latest_date")),
            "symbol": str(t("symbol_column")),
            "weight": str(t("weight_column")),
            "signal_score": str(t("signal_score_column")),
        },
        inplace=True,
    )
    return display


def _localize_nav_frame(frame: pd.DataFrame, language: str) -> pd.DataFrame:
    t = _translator(language)
    display = frame.copy()
    percent_columns = {
        "daily_return",
        "gross_return",
        "benchmark_return",
        "cash_weight",
    }
    numeric_columns = {
        "nav",
        "benchmark_nav",
        "turnover",
        "transaction_cost",
    }
    for column in percent_columns.intersection(display.columns):
        display[column] = display[column].map(lambda value: f"{float(value):.2%}")
    for column in numeric_columns.intersection(display.columns):
        display[column] = display[column].map(lambda value: f"{float(value):.6f}")

    display.rename(
        columns={
            "date": str(t("latest_date")),
            "nav": str(t("final_nav")),
            "daily_return": str(t("daily_return_column")),
            "gross_return": str(t("gross_return_column")),
            "benchmark_nav": str(t("benchmark_nav_column")),
            "benchmark_return": str(t("benchmark_return_column")),
            "turnover": str(t("turnover")),
            "transaction_cost": str(t("transaction_cost_column")),
            "cash_weight": str(t("cash_weight")),
        },
        inplace=True,
    )
    return display


def _localize_weight_frame(frame: pd.DataFrame, language: str) -> pd.DataFrame:
    t = _translator(language)
    display = frame.copy()
    if "weight" in display:
        display["weight"] = display["weight"].map(lambda value: f"{float(value):.1%}")
    if "signal_score" in display:
        display["signal_score"] = display["signal_score"].map(
            lambda value: "" if pd.isna(value) else f"{float(value):.2f}"
        )
    display.rename(
        columns={
            "date": str(t("latest_date")),
            "symbol": str(t("symbol_column")),
            "weight": str(t("weight_column")),
            "signal_score": str(t("signal_score_column")),
        },
        inplace=True,
    )
    return display


def _translator(language: str):
    def _t(key: str) -> object:
        return TRANSLATIONS[language][key]

    return _t


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 2.0rem;
        }
        .hero-card {
            border: 1px solid #dbeafe;
            border-radius: 20px;
            padding: 1.4rem 1.5rem;
            background: linear-gradient(135deg, #f8fafc 0%, #eff6ff 100%);
            box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05);
        }
        .hero-badge {
            display: inline-block;
            margin-bottom: 0.6rem;
            padding: 0.25rem 0.65rem;
            border-radius: 999px;
            background: #dbeafe;
            color: #1d4ed8;
            font-size: 0.84rem;
            font-weight: 600;
        }
        .hero-title {
            font-size: 2rem;
            font-weight: 700;
            color: #0f172a;
            margin-bottom: 0.45rem;
            line-height: 1.2;
        }
        .hero-subtitle {
            color: #475569;
            font-size: 1rem;
            line-height: 1.6;
            margin-bottom: 1rem;
        }
        .hero-meta-wrap {
            display: flex;
            flex-wrap: wrap;
            gap: 0.75rem;
        }
        .hero-meta {
            min-width: 180px;
            border: 1px solid #e2e8f0;
            border-radius: 14px;
            padding: 0.75rem 0.9rem;
            background: rgba(255, 255, 255, 0.8);
        }
        .hero-meta-label {
            display: block;
            font-size: 0.78rem;
            color: #64748b;
            margin-bottom: 0.2rem;
        }
        .hero-meta-value {
            display: block;
            font-size: 0.95rem;
            color: #0f172a;
            font-weight: 600;
        }
        .stage-grid {
            display: grid;
            grid-template-columns: repeat(6, minmax(0, 1fr));
            gap: 0.7rem;
            margin: 0.5rem 0 0.75rem 0;
        }
        .stage-card {
            border: 1px solid #e2e8f0;
            border-radius: 16px;
            padding: 0.85rem 0.9rem;
            background: #ffffff;
            min-height: 92px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }
        .stage-name {
            color: #0f172a;
            font-size: 0.95rem;
            font-weight: 600;
            line-height: 1.35;
        }
        .stage-status {
            font-size: 0.82rem;
            font-weight: 600;
        }
        .stage-ready {
            color: #047857;
        }
        .stage-missing {
            color: #b91c1c;
        }
        @media (max-width: 1200px) {
            .stage-grid {
                grid-template-columns: repeat(3, minmax(0, 1fr));
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
