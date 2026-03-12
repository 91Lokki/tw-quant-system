from __future__ import annotations

from datetime import date
from http.client import RemoteDisconnected
import json
from pathlib import Path
import requests
import sys
import unittest
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.data.providers import TwseOfficialProvider, build_provider


class _FakeHttpResponse:
    def __init__(self, payload: dict[str, object] | str) -> None:
        self._payload = payload

    def read(self) -> bytes:
        if isinstance(self._payload, str):
            return self._payload.encode("utf-8")
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self) -> "_FakeHttpResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeRequestsResponse:
    def __init__(self, text: str, status_code: int = 200, history_length: int = 0) -> None:
        self.text = text
        self.status_code = status_code
        self.history = [object() for _ in range(history_length)]

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")


class _FakeRequestsSession:
    def __init__(self, responses: list[object], captured_calls: list[dict[str, object]]) -> None:
        self._responses = list(responses)
        self._captured_calls = captured_calls
        self.headers: dict[str, str] = {}

    def __enter__(self) -> "_FakeRequestsSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def get(self, url: str, timeout: int, allow_redirects: bool) -> _FakeRequestsResponse:
        self._captured_calls.append(
            {
                "url": url,
                "timeout": timeout,
                "allow_redirects": allow_redirects,
                "headers": dict(self.headers),
            }
        )
        result = self._responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class TwseProviderTests(unittest.TestCase):
    def test_build_provider_supports_twse(self) -> None:
        provider = build_provider("twse", None)
        self.assertIsInstance(provider, TwseOfficialProvider)

    def test_fetch_market_snapshot_parses_twse_official_csv_table(self) -> None:
        payload = _market_csv(
            headers=[
                "Security Code",
                "Security Name",
                "Trade Volume (Shares)",
                "Trade Value (NT$)",
                "Opening Price",
                "Highest Price",
                "Lowest Price",
                "Closing Price",
            ],
            rows=[
                ["1101", "Taiwan Cement", "1,000", "50,000", "50.0", "51.0", "49.0", "50.5"],
                ["0050", "Taiwan 50 ETF", "3,000", "420,000", "140.0", "141.0", "139.0", "140.5"],
            ],
        )

        with patch(
            "tw_quant.data.providers.requests.Session",
            return_value=_FakeRequestsSession(
                [_FakeRequestsResponse(payload)],
                [],
            ),
        ):
            rows = TwseOfficialProvider().fetch_market_snapshot(date(2024, 1, 2)).rows

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["stock_id"], "1101")
        self.assertEqual(rows[0]["stock_name"], "Taiwan Cement")
        self.assertEqual(rows[0]["Trading_Volume"], 1000)
        self.assertEqual(rows[0]["Trading_money"], 50000.0)
        self.assertEqual(rows[0]["close"], 50.5)

    def test_fetch_market_snapshot_falls_back_to_all_when_allbut0999_lacks_table(self) -> None:
        provider = TwseOfficialProvider()
        empty_payload = {
            "stat": "OK",
            "tables": [
                {
                    "fields": ["Index", "Value"],
                    "data": [["TAIEX", "18000"]],
                }
            ],
        }
        valid_payload = {
            "stat": "OK",
            "tables": [
                {
                    "fields": [
                        "Security Code",
                        "Security Name",
                        "Trade Volume (Shares)",
                        "Trade Value (NT$)",
                        "Opening Price",
                        "Highest Price",
                        "Lowest Price",
                        "Closing Price",
                    ],
                    "data": [
                        ["1101", "Taiwan Cement", "1,000", "50,000", "50.0", "51.0", "49.0", "50.5"],
                    ],
                }
            ],
        }

        with patch.object(
            TwseOfficialProvider,
            "_request_market_snapshot_csv",
            side_effect=[
                {"format": "csv", "source": "csv-allbut0999", "text": "shell"},
                {"format": "csv", "source": "csv-all", "text": "shell"},
            ],
        ) as request_csv, patch.object(
            TwseOfficialProvider,
            "_request_market_snapshot_json",
            side_effect=[empty_payload, valid_payload],
        ) as request_json:
            result = provider.fetch_market_snapshot(date(2024, 1, 2))

        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0]["stock_id"], "1101")
        self.assertEqual(result.raw_payload["_requested_type"], "ALL")
        self.assertEqual(request_csv.call_count, 2)
        self.assertEqual(request_json.call_count, 2)

    def test_fetch_benchmark_month_parses_roc_dates(self) -> None:
        payload = _benchmark_csv(
            headers=["Date", "Opening Index", "Highest Index", "Lowest Index", "Closing Index"],
            rows=[
                ["113/01/02", "17,000", "17,100", "16,900", "17,050"],
                ["113/01/03", "17,050", "17,120", "17,000", "17,100"],
            ],
        )
        captured_calls: list[dict[str, object]] = []

        with patch(
            "tw_quant.data.providers.requests.Session",
            return_value=_FakeRequestsSession(
                [_FakeRequestsResponse(payload)],
                captured_calls,
            ),
        ):
            rows = TwseOfficialProvider().fetch_benchmark_month("TAIEX", date(2024, 1, 1)).rows

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["date"], "2024-01-02")
        self.assertEqual(rows[0]["stock_id"], "TAIEX")
        self.assertEqual(rows[0]["price"], 17050.0)
        self.assertEqual(captured_calls[0]["headers"]["Connection"], "close")
        self.assertTrue(captured_calls[0]["allow_redirects"])

    def test_fetch_benchmark_month_parses_zh_fields(self) -> None:
        payload = _benchmark_csv(
            headers=["日期", "開盤指數", "最高指數", "最低指數", "收盤指數"],
            rows=[["103/01/02", "8,500", "8,650", "8,400", "8,612"]],
        )

        with patch(
            "tw_quant.data.providers.requests.Session",
            return_value=_FakeRequestsSession(
                [_FakeRequestsResponse(payload)],
                [],
            ),
        ):
            rows = TwseOfficialProvider().fetch_benchmark_month("TAIEX", date(2014, 1, 1)).rows

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["date"], "2014-01-02")
        self.assertEqual(rows[0]["price"], 8612.0)

    def test_fetch_benchmark_month_retries_after_remote_disconnect(self) -> None:
        payload = _benchmark_csv(
            headers=["Date", "Opening Index", "Highest Index", "Lowest Index", "Closing Index"],
            rows=[["107/03/01", "10,700", "10,950", "10,650", "10,900"]],
        )
        captured_calls: list[dict[str, object]] = []
        sessions = [
            _FakeRequestsSession(
                [requests.ConnectionError(RemoteDisconnected("closed"))],
                captured_calls,
            ),
            _FakeRequestsSession(
                [_FakeRequestsResponse(payload)],
                captured_calls,
            ),
        ]

        with patch(
            "tw_quant.data.providers.requests.Session",
            side_effect=sessions,
        ), patch("tw_quant.data.providers.time.sleep", return_value=None):
            rows = TwseOfficialProvider().fetch_benchmark_month("TAIEX", date(2018, 3, 1)).rows

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["date"], "2018-03-01")
        self.assertEqual(rows[0]["price"], 10900.0)
        self.assertEqual(len(captured_calls), 2)

    def test_fetch_benchmark_month_rejects_excessive_redirect_history(self) -> None:
        payload = _benchmark_csv(
            headers=["Date", "Opening Index", "Highest Index", "Lowest Index", "Closing Index"],
            rows=[["103/01/02", "8,500", "8,650", "8,400", "8,612"]],
        )

        with patch(
            "tw_quant.data.providers.requests.Session",
            side_effect=_session_factory(
                [_FakeRequestsResponse(payload, history_length=6)],
                [],
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "TWSE request failed"):
                TwseOfficialProvider().fetch_benchmark_month("TAIEX", date(2014, 1, 1))

    def test_fetch_benchmark_month_starts_with_stable_official_endpoint(self) -> None:
        captured_urls: list[dict[str, object]] = []
        payload = _benchmark_csv(
            headers=["Date", "Opening Index", "Highest Index", "Lowest Index", "Closing Index"],
            rows=[["103/01/02", "8,500", "8,650", "8,400", "8,612"]],
        )

        with patch(
            "tw_quant.data.providers.requests.Session",
            return_value=_FakeRequestsSession(
                [_FakeRequestsResponse(payload)],
                captured_urls,  # type: ignore[arg-type]
            ),
        ):
            TwseOfficialProvider().fetch_benchmark_month("TAIEX", date(2014, 1, 1))

        self.assertTrue(captured_urls)
        self.assertTrue(
            str(captured_urls[0]["url"]).startswith("https://www.twse.com.tw/indicesReport/MI_5MINS_HIST"),
            str(captured_urls[0]["url"]),
        )
        self.assertIn("response=csv", str(captured_urls[0]["url"]))
        self.assertTrue(str(captured_urls[0]["url"]).endswith("date=20140101"))

    def test_fetch_benchmark_month_tries_additional_in_month_dates(self) -> None:
        captured_urls: list[dict[str, object]] = []
        valid_payload = _benchmark_csv(
            headers=["Date", "Opening Index", "Highest Index", "Lowest Index", "Closing Index"],
            rows=[["108/05/02", "10,900", "11,000", "10,850", "10,950"]],
        )
        with patch(
            "tw_quant.data.providers.requests.Session",
            side_effect=_session_factory(
                [*[_FakeRequestsResponse("no,data,table") for _ in range(3)], _FakeRequestsResponse(valid_payload)],
                captured_urls,
            ),
        ):
            rows = TwseOfficialProvider().fetch_benchmark_month("TAIEX", date(2019, 5, 1)).rows

        self.assertEqual(len(rows), 1)
        queried_urls = [str(call["url"]) for call in captured_urls]
        self.assertIn("date=20190501", queried_urls[0])
        self.assertTrue(any("date=20190515" in url for url in queried_urls), queried_urls)

    def test_fetch_benchmark_month_skips_shell_html_without_table(self) -> None:
        captured_calls: list[dict[str, object]] = []
        valid_payload = _benchmark_html(
            headers=["Date", "Opening Index", "Highest Index", "Lowest Index", "Closing Index"],
            rows=[["103/01/02", "8,500", "8,650", "8,400", "8,612"]],
        )
        with patch(
            "tw_quant.data.providers.requests.Session",
            side_effect=_session_factory(
                [*[_FakeRequestsResponse("Landing shell,not csv") for _ in range(9)], _FakeRequestsResponse(valid_payload)],
                captured_calls,
            ),
        ):
            rows = TwseOfficialProvider().fetch_benchmark_month("TAIEX", date(2014, 1, 1)).rows

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["date"], "2014-01-02")
        self.assertGreaterEqual(len(captured_calls), 2)

    def test_fetch_benchmark_month_skips_html_with_unrelated_table(self) -> None:
        captured_calls: list[dict[str, object]] = []
        valid_payload = _benchmark_html(
            headers=["Date", "Opening Index", "Highest Index", "Lowest Index", "Closing Index"],
            rows=[["103/01/02", "8,500", "8,650", "8,400", "8,612"]],
        )
        with patch(
            "tw_quant.data.providers.requests.Session",
            side_effect=_session_factory(
                [
                    *[_FakeRequestsResponse("Landing shell,not csv") for _ in range(9)],
                    _FakeRequestsResponse(_benchmark_html(headers=["Column A", "Column B"], rows=[["foo", "bar"]])),
                    _FakeRequestsResponse(valid_payload),
                ],
                captured_calls,
            ),
        ):
            rows = TwseOfficialProvider().fetch_benchmark_month("TAIEX", date(2014, 1, 1)).rows

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["date"], "2014-01-02")
        self.assertGreaterEqual(len(captured_calls), 2)

    def test_fetch_benchmark_month_parses_csv_with_preamble_rows(self) -> None:
        captured_calls: list[dict[str, object]] = []
        payload = _benchmark_csv(
            headers=["Date", "Opening Index", "Highest Index", "Lowest Index", "Closing Index"],
            rows=[["103/01/02", "8,500", "8,650", "8,400", "8,612"]],
            preamble=["TAIEX historical data", "Generated by TWSE"],
        )

        with patch(
            "tw_quant.data.providers.requests.Session",
            return_value=_FakeRequestsSession(
                [_FakeRequestsResponse(payload)],
                captured_calls,
            ),
        ):
            rows = TwseOfficialProvider().fetch_benchmark_month("TAIEX", date(2014, 1, 1)).rows

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["date"], "2014-01-02")


def _benchmark_html(headers: list[str], rows: list[list[str]]) -> str:
    head_cells = "".join(f"<th>{cell}</th>" for cell in headers)
    body_rows = "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return (
        "<html><body>"
        "<table>"
        f"<tr>{head_cells}</tr>"
        f"{body_rows}"
        "</table>"
        "</body></html>"
    )


def _benchmark_csv(headers: list[str], rows: list[list[str]], preamble: list[str] | None = None) -> str:
    lines: list[str] = []
    for line in preamble or []:
        lines.append(line)
    lines.append(",".join(f'"{cell}"' for cell in headers))
    for row in rows:
        lines.append(",".join(f'"{cell}"' for cell in row))
    return "\n".join(lines)


def _market_csv(headers: list[str], rows: list[list[str]], preamble: list[str] | None = None) -> str:
    return _benchmark_csv(headers=headers, rows=rows, preamble=preamble)


def _session_factory(responses: list[object], captured_calls: list[dict[str, object]]):
    remaining = list(responses)

    def _build_session() -> _FakeRequestsSession:
        if remaining:
            response = remaining.pop(0)
        else:
            response = responses[-1]
        return _FakeRequestsSession([response], captured_calls)

    return _build_session


if __name__ == "__main__":
    unittest.main()
