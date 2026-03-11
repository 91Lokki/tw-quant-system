"""Data providers for Taiwan daily market data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
import os
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"


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

    def _request(
        self,
        dataset: str,
        data_id: str,
        start_date: date,
        end_date: date,
    ) -> dict[str, Any]:
        query = urlencode(
            {
                "dataset": dataset,
                "data_id": data_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            }
        )
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


def build_provider(provider_name: str, token_env_var: str | None) -> DailyDataProvider:
    """Build the configured daily data provider."""

    if provider_name != "finmind":
        raise ValueError(f"Unsupported provider: {provider_name}")
    return FinMindProvider.from_env(token_env_var)
