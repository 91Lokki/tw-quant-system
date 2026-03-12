"""Data providers for Taiwan daily market data."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser
import io
import json
import os
from calendar import monthrange
import re
import time
from typing import Any, Protocol
import requests
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen


FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"
TWSE_MARKET_JSON_URLS = (
    "https://www.twse.com.tw/rwd/en/afterTrading/MI_INDEX",
    "https://www.twse.com.tw/exchangeReport/MI_INDEX",
)
TWSE_MARKET_CSV_URLS = (
    "https://www.twse.com.tw/exchangeReport/MI_INDEX",
    "https://www.twse.com.tw/rwd/en/afterTrading/MI_INDEX",
)
TWSE_TAIEX_HTML_REPORT_URLS = (
    "https://www.twse.com.tw/indicesReport/MI_5MINS_HIST",
    "https://www.twse.com.tw/rwd/en/indicesReport/MI_5MINS_HIST",
    "https://www.twse.com.tw/rwd/zh/indicesReport/MI_5MINS_HIST",
)
TWSE_TAIEX_HTML_PAGE_URLS = (
    "https://www.twse.com.tw/en/indices/taiex/mi-5min-hist.html",
    "https://www.twse.com.tw/indices/taiex/mi-5min-hist.html",
    "https://wwwc.twse.com.tw/en/indices/taiex/mi-5min-hist.html",
)
_NON_NUMERIC_RE = re.compile(r"[^0-9.\-]")
_TWSE_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9,zh-TW;q=0.8",
    "Referer": "https://www.twse.com.tw/",
    "Cache-Control": "no-cache",
}
_TWSE_HTML_RETRY_ATTEMPTS = 3
_TWSE_HTML_RETRY_BACKOFF_SECONDS = 0.5


class ProviderError(RuntimeError):
    """Raised when a market data provider cannot return valid data."""


@dataclass(slots=True)
class ProviderPayload:
    dataset: str
    symbol: str
    rows: list[dict[str, Any]]
    raw_payload: dict[str, Any]


class DailyDataProvider(Protocol):
    """Minimal provider contract used by the ingestion pipeline."""

    name: str

    def fetch_security_daily(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> ProviderPayload: ...

    def fetch_benchmark_daily(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> ProviderPayload: ...

    def fetch_stock_info(self) -> ProviderPayload: ...

    def fetch_market_snapshot(self, trading_date: date) -> ProviderPayload: ...

    def fetch_benchmark_month(self, symbol: str, month_anchor: date) -> ProviderPayload: ...


@dataclass(slots=True)
class FinMindProvider:
    """Fetch Taiwan market data from the FinMind data API."""

    token: str | None = None
    timeout_seconds: int = 30
    name: str = "finmind"

    @classmethod
    def from_env(cls, token_env_var: str | None) -> "FinMindProvider":
        token = os.environ.get(token_env_var) if token_env_var else None
        return cls(token=token)

    def fetch_security_daily(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> ProviderPayload:
        payload = self._request(
            dataset="TaiwanStockPrice",
            data_id=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        return ProviderPayload(
            dataset="TaiwanStockPrice",
            symbol=symbol,
            rows=self._extract_rows(payload, symbol),
            raw_payload=payload,
        )

    def fetch_benchmark_daily(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> ProviderPayload:
        payload = self._request(
            dataset="TaiwanStockTotalReturnIndex",
            data_id=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        return ProviderPayload(
            dataset="TaiwanStockTotalReturnIndex",
            symbol=symbol,
            rows=self._extract_rows(payload, symbol),
            raw_payload=payload,
        )

    def fetch_stock_info(self) -> ProviderPayload:
        payload = self._request(dataset="TaiwanStockInfo")
        return ProviderPayload(
            dataset="TaiwanStockInfo",
            symbol="TWSE_INFO",
            rows=self._extract_rows(payload, "TWSE_INFO"),
            raw_payload=payload,
        )

    def fetch_market_snapshot(self, trading_date: date) -> ProviderPayload:
        raise ProviderError("FinMindProvider does not support TWSE daily market snapshots.")

    def fetch_benchmark_month(self, symbol: str, month_anchor: date) -> ProviderPayload:
        raise ProviderError("FinMindProvider does not support TWSE monthly benchmark history.")

    def _request(
        self,
        dataset: str,
        data_id: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        query_params: dict[str, str] = {"dataset": dataset}
        if data_id is not None:
            query_params["data_id"] = data_id
        if start_date is not None:
            query_params["start_date"] = start_date.isoformat()
        if end_date is not None:
            query_params["end_date"] = end_date.isoformat()
        query = urlencode(query_params)
        request = Request(f"{FINMIND_API_URL}?{query}")
        if self.token:
            request.add_header("Authorization", f"Bearer {self.token}")

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as error:  # pragma: no cover - exercised with real network calls
            raise ProviderError(
                f"FinMind request failed with HTTP {error.code} for dataset {dataset} and symbol {data_id}."
            ) from error
        except URLError as error:  # pragma: no cover - exercised with real network calls
            raise ProviderError(f"FinMind request failed for symbol {data_id}: {error.reason}") from error

    def _extract_rows(self, payload: dict[str, Any], symbol: str) -> list[dict[str, Any]]:
        status = payload.get("status")
        if status not in (None, 200):
            message = payload.get("msg", "Unknown FinMind error")
            raise ProviderError(f"FinMind returned status {status} for {symbol}: {message}")
        rows = payload.get("data")
        if not isinstance(rows, list):
            raise ProviderError(f"FinMind returned an invalid payload for symbol {symbol}.")
        return [row for row in rows if isinstance(row, dict)]


@dataclass(slots=True)
class TwseOfficialProvider:
    """Fetch TWSE official daily market and benchmark history for the cross-sectional branch."""

    timeout_seconds: int = 30
    name: str = "twse"

    def fetch_security_daily(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> ProviderPayload:
        raise ProviderError(
            "TwseOfficialProvider does not support per-symbol historical fetches. "
            "Use daily market snapshots for the cross-sectional branch."
        )

    def fetch_benchmark_daily(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> ProviderPayload:
        raise ProviderError(
            "TwseOfficialProvider does not support single-range benchmark fetches. "
            "Use monthly TWSE TAIEX history for the cross-sectional branch."
        )

    def fetch_stock_info(self) -> ProviderPayload:
        raise ProviderError(
            "TwseOfficialProvider does not expose a separate stock-info endpoint in v1. "
            "The cross-sectional branch derives a practical symbol master from official daily market snapshots."
        )

    def fetch_market_snapshot(self, trading_date: date) -> ProviderPayload:
        last_error: Exception | None = None
        for market_type in ("ALLBUT0999", "ALL"):
            for request_payload in (
                lambda: self._request_market_snapshot_csv(trading_date, market_type),
                lambda: self._request_market_snapshot_json(trading_date, market_type),
            ):
                try:
                    payload = request_payload()
                except ProviderError as error:
                    last_error = error
                    continue
                try:
                    rows = self._extract_market_rows(payload, trading_date)
                except ProviderError as error:
                    last_error = error
                    continue
                raw_payload = dict(payload)
                raw_payload["_requested_type"] = market_type
                return ProviderPayload(
                    dataset="TWSE_MI_INDEX",
                    symbol=trading_date.isoformat(),
                    rows=rows,
                    raw_payload=raw_payload,
                )

        raise ProviderError(
            f"TWSE daily market snapshot did not contain a usable securities table for {trading_date.isoformat()}."
        ) from last_error

    def fetch_benchmark_month(self, symbol: str, month_anchor: date) -> ProviderPayload:
        payload = self._request_taiex_monthly_payload(month_anchor)
        return ProviderPayload(
            dataset="TWSE_TAIEX_HISTORY_CSV" if payload.get("format") == "csv" else "TWSE_TAIEX_HISTORY_HTML",
            symbol=symbol,
            rows=self._extract_taiex_rows(payload, symbol),
            raw_payload=payload,
        )

    def _request_taiex_monthly_payload(self, month_anchor: date) -> dict[str, Any]:
        month_urls = _build_taiex_month_urls(month_anchor)
        last_error: Exception | None = None
        for payload_format, url in month_urls:
            try:
                text = self._request_text_url(url, redirect_limit=5)
                if payload_format == "csv":
                    if not _has_expected_taiex_csv_table(text):
                        last_error = ProviderError(
                            f"TWSE benchmark CSV response did not include the expected index table for {url}."
                        )
                        continue
                    return {
                        "source": url,
                        "format": "csv",
                        "text": text,
                    }
                if not _has_expected_taiex_table(text):
                    last_error = ProviderError(
                        f"TWSE benchmark HTML response did not include the expected index table for {url}."
                    )
                    continue
                return {
                    "source": url,
                    "format": "html",
                    "html": text,
                }
            except (HTTPError, URLError, ProviderError) as error:  # pragma: no cover - network only
                last_error = error
                continue
        raise ProviderError(
            f"TWSE request failed for TWSE TAIEX monthly history for {month_anchor.strftime('%Y-%m')}."
        ) from last_error

    def _request_market_snapshot_csv(self, trading_date: date, market_type: str) -> dict[str, Any]:
        query = urlencode(
            {
                "response": "csv",
                "date": trading_date.strftime("%Y%m%d"),
                "type": market_type,
            }
        )
        last_error: Exception | None = None
        for base_url in TWSE_MARKET_CSV_URLS:
            url = f"{base_url}?{query}"
            try:
                return {
                    "format": "csv",
                    "source": url,
                    "text": self._request_text_url(url, redirect_limit=5),
                }
            except ProviderError as error:  # pragma: no cover - network only
                last_error = error
                continue
        raise ProviderError(
            f"TWSE request failed for TWSE daily market snapshot CSV for {trading_date.isoformat()} "
            f"with type={market_type}."
        ) from last_error

    def _request_market_snapshot_json(self, trading_date: date, market_type: str) -> dict[str, Any]:
        return self._request_json(
            urls=TWSE_MARKET_JSON_URLS,
            query_params={
                "response": "json",
                "date": trading_date.strftime("%Y%m%d"),
                "type": market_type,
            },
            context=(
                f"TWSE daily market snapshot JSON for {trading_date.isoformat()} "
                f"with type={market_type}"
            ),
        )

    def _request_json(
        self,
        urls: tuple[str, ...],
        query_params: dict[str, str],
        context: str,
    ) -> dict[str, Any]:
        query = urlencode(query_params)
        last_error: Exception | None = None

        for base_url in urls:
            try:
                return self._request_json_url(
                    f"{base_url}?{query}",
                    redirect_limit=5,
                )
            except (HTTPError, URLError, json.JSONDecodeError) as error:  # pragma: no cover - network only
                last_error = error
                continue

        raise ProviderError(f"TWSE request failed for {context}.") from last_error

    def _request_json_url(self, url: str, redirect_limit: int) -> dict[str, Any]:
        request = Request(url, headers=_TWSE_DEFAULT_HEADERS)
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as error:  # pragma: no cover - network only
            if error.code in {301, 302, 303, 307, 308}:
                if redirect_limit <= 0:
                    raise ProviderError(f"TWSE request exceeded redirect limit for {url}.") from error
                location = error.headers.get("Location")
                if not location:
                    raise
                redirected_url = urljoin(url, location)
                return self._request_json_url(redirected_url, redirect_limit - 1)
            raise

    def _request_text_url(self, url: str, redirect_limit: int) -> str:
        headers = dict(_TWSE_DEFAULT_HEADERS)
        headers["Connection"] = "close"
        last_error: Exception | None = None

        for attempt in range(_TWSE_HTML_RETRY_ATTEMPTS):
            try:
                with requests.Session() as session:
                    session.headers.update(headers)
                    response = session.get(
                        url,
                        timeout=self.timeout_seconds,
                        allow_redirects=True,
                    )
                    if len(response.history) > redirect_limit:
                        raise ProviderError(f"TWSE request exceeded redirect limit for {url}.")
                    response.raise_for_status()
                    return response.text
            except requests.TooManyRedirects as error:  # pragma: no cover - network only
                raise ProviderError(f"TWSE request exceeded redirect limit for {url}.") from error
            except (requests.ConnectionError, requests.Timeout) as error:  # pragma: no cover - network only
                last_error = error
            except requests.RequestException as error:  # pragma: no cover - network only
                last_error = error
                break

            if attempt < _TWSE_HTML_RETRY_ATTEMPTS - 1:
                time.sleep(_TWSE_HTML_RETRY_BACKOFF_SECONDS * (attempt + 1))

        raise ProviderError(f"TWSE text request failed for {url}.") from last_error

    def _extract_market_rows(
        self,
        payload: dict[str, Any],
        trading_date: date,
    ) -> list[dict[str, Any]]:
        if payload.get("format") == "csv":
            text = payload.get("text")
            if not isinstance(text, str) or not text.strip():
                raise ProviderError("TWSE market CSV payload is empty.")
            csv_rows = _find_market_csv_rows(text)
            return _build_market_rows_from_csv_rows(csv_rows, trading_date)

        table = _find_table(
            payload,
            required_aliases=(
                ("Security Code", "Code", "證券代號", "代號"),
                ("Security Name", "Name", "證券名稱", "名稱"),
                ("Trade Volume (Shares)", "Trade Volume", "成交股數", "成交股數(股)"),
                ("Trade Value (NT$)", "Trade Value", "成交金額", "成交金額(元)"),
                ("Opening Price", "Open", "開盤價"),
                ("Highest Price", "High", "最高價"),
                ("Lowest Price", "Low", "最低價"),
                ("Closing Price", "Close", "收盤價"),
            ),
        )
        field_lookup = _resolve_required_fields(
            table["fields"],
            {
                "code": ("Security Code", "Code", "證券代號", "代號"),
                "name": ("Security Name", "Name", "證券名稱", "名稱"),
                "volume": ("Trade Volume (Shares)", "Trade Volume", "成交股數", "成交股數(股)"),
                "traded_value": ("Trade Value (NT$)", "Trade Value", "成交金額", "成交金額(元)"),
                "open": ("Opening Price", "Open", "開盤價"),
                "high": ("Highest Price", "High", "最高價"),
                "low": ("Lowest Price", "Low", "最低價"),
                "close": ("Closing Price", "Close", "收盤價"),
            },
        )
        rows: list[dict[str, Any]] = []
        for raw_row in table["data"]:
            if not isinstance(raw_row, list):
                continue
            stock_id = str(raw_row[field_lookup["code"]]).strip()
            stock_name = str(raw_row[field_lookup["name"]]).strip()
            open_value = _parse_twse_number(raw_row[field_lookup["open"]])
            high_value = _parse_twse_number(raw_row[field_lookup["high"]])
            low_value = _parse_twse_number(raw_row[field_lookup["low"]])
            close_value = _parse_twse_number(raw_row[field_lookup["close"]])
            volume_value = _parse_twse_number(raw_row[field_lookup["volume"]])
            traded_value = _parse_twse_number(raw_row[field_lookup["traded_value"]])
            if (
                open_value is None
                or high_value is None
                or low_value is None
                or close_value is None
                or volume_value is None
            ):
                continue
            rows.append(
                {
                    "date": trading_date.isoformat(),
                    "stock_id": stock_id,
                    "stock_name": stock_name,
                    "open": open_value,
                    "max": high_value,
                    "min": low_value,
                    "close": close_value,
                    "Trading_Volume": int(volume_value),
                    "Trading_money": traded_value,
                }
            )
        if not rows:
            raise ProviderError(
                f"TWSE daily market snapshot returned no usable rows for {trading_date.isoformat()}."
            )
        return rows

    def _extract_taiex_rows(self, payload: dict[str, Any], symbol: str) -> list[dict[str, Any]]:
        payload_format = str(payload.get("format", "html"))
        if payload_format == "csv":
            text = payload.get("text")
            if not isinstance(text, str) or not text.strip():
                raise ProviderError("TWSE benchmark CSV payload is empty.")
            data_rows = _find_taiex_csv_rows(text)
            return _build_taiex_rows_from_tabular_rows(data_rows, symbol)

        html = payload.get("html")
        if not isinstance(html, str) or not html.strip():
            raise ProviderError("TWSE benchmark HTML payload is empty.")
        tables = _parse_html_tables(html)
        if not tables:
            raise ProviderError("TWSE benchmark HTML payload did not include any table.")
        field_lookup: dict[str, int] | None = None
        data_rows: list[list[str]] | None = None
        for table in tables:
            if not table["headers"] or not table["rows"]:
                continue
            try:
                resolved = _resolve_required_fields(
                    table["headers"],
                    {
                        "date": ("Date", "日期"),
                        "open": ("Opening Index", "Open", "開盤指數", "開盤"),
                        "high": ("Highest Index", "High", "最高指數", "最高"),
                        "low": ("Lowest Index", "Low", "最低指數", "最低"),
                        "close": ("Closing Index", "Close", "收盤指數", "收盤"),
                    },
                )
            except ProviderError:
                continue
            field_lookup = resolved
            data_rows = table["rows"]
            break

        if field_lookup is None or data_rows is None:
            raise ProviderError("TWSE benchmark HTML did not contain the expected monthly index table.")

        return _build_taiex_rows_from_tabular_rows(data_rows, symbol)


def build_provider(provider_name: str, token_env_var: str | None) -> DailyDataProvider:
    """Build the configured daily data provider."""

    if provider_name == "finmind":
        return FinMindProvider.from_env(token_env_var)
    if provider_name == "twse":
        return TwseOfficialProvider()
    raise ValueError(f"Unsupported provider: {provider_name}")


def _find_table(
    payload: dict[str, Any],
    required_aliases: tuple[tuple[str, ...], ...],
) -> dict[str, list[Any]]:
    tables = _extract_tables(payload)
    if not tables:
        raise ProviderError("TWSE payload did not contain any tabular data.")

    for table in tables:
        lookup = _build_field_lookup(table["fields"])
        if all(_resolve_alias_index(lookup, aliases) is not None for aliases in required_aliases):
            return table

    raise ProviderError("TWSE payload did not contain the expected table schema.")


def _build_taiex_month_urls(month_anchor: date) -> tuple[tuple[str, str], ...]:
    month_dates = tuple(
        candidate.strftime("%Y%m%d") for candidate in _build_taiex_query_dates(month_anchor)
    )
    csv_report_urls = tuple(
        ("csv", f"{base_url}?response=csv&date={month_date}")
        for month_date in month_dates
        for base_url in TWSE_TAIEX_HTML_REPORT_URLS
    )
    html_report_urls = tuple(
        ("html", f"{base_url}?response=html&date={month_date}")
        for month_date in month_dates
        for base_url in TWSE_TAIEX_HTML_REPORT_URLS
    )
    page_urls = tuple(
        ("html", f"{base_url}?myear={month_anchor.year - 1911}&mmon={month_anchor.month:02d}")
        for base_url in TWSE_TAIEX_HTML_PAGE_URLS
    )
    return csv_report_urls + html_report_urls + page_urls


def _build_taiex_query_dates(month_anchor: date) -> tuple[date, ...]:
    last_day = monthrange(month_anchor.year, month_anchor.month)[1]
    candidate_days = (1, min(15, last_day), min(28, last_day))
    deduped_days = tuple(dict.fromkeys(candidate_days))
    return tuple(month_anchor.replace(day=day) for day in deduped_days)


def _has_expected_taiex_table(html: str) -> bool:
    for table in _parse_html_tables(html):
        headers = table.get("headers")
        rows = table.get("rows")
        if not headers or not rows:
            continue
        try:
            _resolve_required_fields(
                headers,
                {
                    "date": ("Date", "日期"),
                    "open": ("Opening Index", "Open", "開盤指數", "開盤"),
                    "high": ("Highest Index", "High", "最高指數", "最高"),
                    "low": ("Lowest Index", "Low", "最低指數", "最低"),
                    "close": ("Closing Index", "Close", "收盤指數", "收盤"),
                },
            )
        except ProviderError:
            continue
        return True
    return False


def _has_expected_taiex_csv_table(text: str) -> bool:
    try:
        _find_taiex_csv_rows(text)
    except ProviderError:
        return False
    return True


def _find_market_csv_rows(text: str) -> list[list[str]]:
    reader = csv.reader(io.StringIO(text))
    field_lookup: dict[str, int] | None = None
    data_rows: list[list[str]] = []
    for raw_row in reader:
        row = [_clean_twse_text(cell) for cell in raw_row]
        if not any(row):
            if field_lookup is not None and data_rows:
                break
            continue
        if field_lookup is None:
            try:
                field_lookup = _resolve_required_fields(
                    row,
                    {
                        "code": ("Security Code", "Code", "證券代號", "代號"),
                        "name": ("Security Name", "Name", "證券名稱", "名稱"),
                        "volume": ("Trade Volume (Shares)", "Trade Volume", "成交股數", "成交股數(股)"),
                        "traded_value": ("Trade Value (NT$)", "Trade Value", "成交金額", "成交金額(元)"),
                        "open": ("Opening Price", "Open", "開盤價"),
                        "high": ("Highest Price", "High", "最高價"),
                        "low": ("Lowest Price", "Low", "最低價"),
                        "close": ("Closing Price", "Close", "收盤價"),
                    },
                )
            except ProviderError:
                continue
            continue
        if len(row) <= max(field_lookup.values()):
            if data_rows:
                break
            continue
        if not row[field_lookup["code"]]:
            if data_rows:
                break
            continue
        data_rows.append(
            [
                row[field_lookup["code"]],
                row[field_lookup["name"]],
                row[field_lookup["volume"]],
                row[field_lookup["traded_value"]],
                row[field_lookup["open"]],
                row[field_lookup["high"]],
                row[field_lookup["low"]],
                row[field_lookup["close"]],
            ]
        )

    if not data_rows:
        raise ProviderError("TWSE market CSV did not contain the expected securities table.")
    return data_rows


def _find_taiex_csv_rows(text: str) -> list[list[str]]:
    reader = csv.reader(io.StringIO(text))
    header_index: dict[str, int] | None = None
    data_rows: list[list[str]] = []
    for raw_row in reader:
        row = [cell.strip() for cell in raw_row]
        if not any(row):
            if header_index is not None and data_rows:
                break
            continue
        if header_index is None:
            try:
                header_index = _resolve_required_fields(
                    row,
                    {
                        "date": ("Date", "日期"),
                        "open": ("Opening Index", "Open", "開盤指數", "開盤"),
                        "high": ("Highest Index", "High", "最高指數", "最高"),
                        "low": ("Lowest Index", "Low", "最低指數", "最低"),
                        "close": ("Closing Index", "Close", "收盤指數", "收盤"),
                    },
                )
            except ProviderError:
                continue
            continue
        if len(row) <= max(header_index.values()):
            if data_rows:
                break
            continue
        if not row[header_index["date"]]:
            if data_rows:
                break
            continue
        data_rows.append(
            [
                row[header_index["date"]],
                row[header_index["open"]],
                row[header_index["high"]],
                row[header_index["low"]],
                row[header_index["close"]],
            ]
        )

    if not data_rows:
        raise ProviderError("TWSE benchmark CSV did not contain the expected monthly index table.")
    return data_rows


def _build_taiex_rows_from_tabular_rows(data_rows: list[list[str]], symbol: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw_row in data_rows:
        if not isinstance(raw_row, list) or len(raw_row) < 5:
            continue
        trading_date = _parse_twse_date(raw_row[0])
        open_value = _parse_twse_number(raw_row[1])
        high_value = _parse_twse_number(raw_row[2])
        low_value = _parse_twse_number(raw_row[3])
        close_value = _parse_twse_number(raw_row[4])
        if (
            close_value is None
            or open_value is None
            or high_value is None
            or low_value is None
        ):
            continue
        rows.append(
            {
                "date": trading_date.isoformat(),
                "stock_id": symbol,
                "open": open_value,
                "max": high_value,
                "min": low_value,
                "close": close_value,
                "price": close_value,
            }
        )
    if not rows:
        raise ProviderError("TWSE benchmark table returned no usable rows.")
    return rows


def _build_market_rows_from_csv_rows(data_rows: list[list[str]], trading_date: date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw_row in data_rows:
        if len(raw_row) < 8:
            continue
        stock_id = _clean_twse_text(raw_row[0])
        stock_name = _clean_twse_text(raw_row[1])
        volume_value = _parse_twse_number(raw_row[2])
        traded_value = _parse_twse_number(raw_row[3])
        open_value = _parse_twse_number(raw_row[4])
        high_value = _parse_twse_number(raw_row[5])
        low_value = _parse_twse_number(raw_row[6])
        close_value = _parse_twse_number(raw_row[7])
        if (
            not stock_id
            or open_value is None
            or high_value is None
            or low_value is None
            or close_value is None
            or volume_value is None
        ):
            continue
        rows.append(
            {
                "date": trading_date.isoformat(),
                "stock_id": stock_id,
                "stock_name": stock_name,
                "open": open_value,
                "max": high_value,
                "min": low_value,
                "close": close_value,
                "Trading_Volume": int(volume_value),
                "Trading_money": traded_value,
            }
        )
    if not rows:
        raise ProviderError("TWSE market CSV securities table returned no usable rows.")
    return rows


def _extract_tables(payload: dict[str, Any]) -> list[dict[str, list[Any]]]:
    tables: list[dict[str, list[Any]]] = []
    raw_tables = payload.get("tables")
    if isinstance(raw_tables, list):
        for table in raw_tables:
            if not isinstance(table, dict):
                continue
            fields = table.get("fields")
            data = table.get("data")
            if isinstance(fields, list) and isinstance(data, list):
                tables.append({"fields": fields, "data": data})

    for key, value in payload.items():
        if not key.startswith("fields") or not isinstance(value, list):
            continue
        suffix = key.removeprefix("fields")
        data = payload.get(f"data{suffix}")
        if isinstance(data, list):
            tables.append({"fields": value, "data": data})
    return tables


class _HtmlTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: list[dict[str, list[list[str]]]] = []
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._cell_text: list[str] = []
        self._current_row: list[str] = []
        self._current_row_has_th = False
        self._current_headers: list[str] = []
        self._current_rows: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lowered = tag.lower()
        if lowered == "table":
            self._in_table = True
            self._current_headers = []
            self._current_rows = []
            return
        if not self._in_table:
            return
        if lowered == "tr":
            self._in_row = True
            self._current_row = []
            self._current_row_has_th = False
            return
        if lowered in {"th", "td"} and self._in_row:
            self._in_cell = True
            self._cell_text = []
            if lowered == "th":
                self._current_row_has_th = True

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in {"th", "td"} and self._in_cell:
            text = "".join(self._cell_text).strip()
            self._current_row.append(text)
            self._cell_text = []
            self._in_cell = False
            return
        if lowered == "tr" and self._in_row:
            if self._current_row:
                if self._current_row_has_th and not self._current_headers:
                    self._current_headers = list(self._current_row)
                else:
                    self._current_rows.append(list(self._current_row))
            self._current_row = []
            self._in_row = False
            self._current_row_has_th = False
            return
        if lowered == "table" and self._in_table:
            if self._current_headers or self._current_rows:
                self.tables.append(
                    {
                        "headers": list(self._current_headers),
                        "rows": list(self._current_rows),
                    }
                )
            self._in_table = False
            self._current_headers = []
            self._current_rows = []


def _parse_html_tables(html: str) -> list[dict[str, list[list[str]]]]:
    parser = _HtmlTableParser()
    parser.feed(html)
    return parser.tables


def _build_field_lookup(fields: list[Any]) -> dict[str, int]:
    lookup: dict[str, int] = {}
    for index, field in enumerate(fields):
        lookup[str(field).strip().lower()] = index
    return lookup


def _resolve_alias_index(lookup: dict[str, int], aliases: tuple[str, ...]) -> int | None:
    for alias in aliases:
        index = lookup.get(alias.strip().lower())
        if index is not None:
            return index
    return None


def _resolve_required_fields(
    fields: list[Any],
    aliases_by_name: dict[str, tuple[str, ...]],
) -> dict[str, int]:
    lookup = _build_field_lookup(fields)
    resolved: dict[str, int] = {}
    for name, aliases in aliases_by_name.items():
        index = _resolve_alias_index(lookup, aliases)
        if index is None:
            raise ProviderError(f"TWSE payload did not contain a required column for {name}.")
        resolved[name] = index
    return resolved


def _parse_twse_date(raw_value: object) -> date:
    text = _clean_twse_text(raw_value)
    if "-" in text:
        return date.fromisoformat(text)
    if "/" not in text:
        raise ProviderError(f"Unsupported TWSE date format: {text}")
    year_text, month_text, day_text = text.split("/")
    year = int(year_text)
    if year < 1911:
        year += 1911
    return date(year, int(month_text), int(day_text))


def _parse_twse_number(raw_value: object) -> float | None:
    text = _clean_twse_text(raw_value).replace(",", "")
    if text in {"", "--", "---", "----", "N/A"}:
        return None
    cleaned = _NON_NUMERIC_RE.sub("", text)
    if cleaned in {"", "-", ".", "-."}:
        return None
    return float(cleaned)


def _clean_twse_text(raw_value: object) -> str:
    text = str(raw_value).strip().lstrip("\ufeff")
    if text.startswith('="') and text.endswith('"'):
        text = text[2:-1]
    elif text.startswith("="):
        text = text[1:].strip('"')
    return text.strip()
