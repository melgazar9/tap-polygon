"""REST client handling, including PolygonStream base class."""

from __future__ import annotations

import logging
import decimal
import typing as t
from importlib import resources
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import RESTStream

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context

import requests
from singer_sdk.pagination import BaseHATEOASPaginator
from singer_sdk.exceptions import ConfigValidationError
from polygon import RESTClient
from tap_polygon.utils import check_missing_fields
from datetime import datetime, timezone


class PolygonAPIPaginator(BaseHATEOASPaginator):
    def get_next_url(self, response: requests.Response) -> t.Optional[str]:
        data = response.json()
        return data.get("next_url")


class PolygonRestStream(RESTStream):
    """Polygon rest API stream class."""

    def __init__(self, tap: TapBase):
        super().__init__(tap=tap)
        self.client = RESTClient(self.config["api_key"])

        self._use_cached_tickers = None
        self._clean_in_place = True
        self.parse_config_params()

        self.DEBUG = False

        self._cfg_start_timestamp_key = None

        timestamp_filter_fields = [
            "from",
            "from_",
            "date",
            "timestamp",
            "ex_dividend_date",
            "record_date",
            "declaration_date",
            "pay_date",
            "listing_date",
            "execution_date",
            "filing_date",
            "period_of_report_date",
            "settlement_date",
            "published_utc",
        ]

        timestamp_filter_suffixes = ["gt", "gte", "lt", "lte"]

        combinations = [
            f"{field}.{suffix}"
            for field in timestamp_filter_fields
            for suffix in timestamp_filter_suffixes
        ]

        for param_source in ("query_params", "path_params"):
            param_dict = getattr(self, param_source, None)
            if param_dict:
                if param_dict:
                    for k, v in param_dict.items():
                        if k in [
                            i
                            for i in combinations
                            if not i.endswith(".lt") and not i.endswith(".lte")
                        ]:
                            self._cfg_start_timestamp_key = k
                            self.cfg_starting_timestamp = v
                        if k in [
                            i
                            for i in combinations
                            if not i.endswith(".gt") and not i.endswith(".gte")
                        ]:
                            self._cfg_end_timestamp_key = k
                            self.cfg_ending_timestamp = v

                if self._cfg_start_timestamp_key:
                    break

        if self._cfg_start_timestamp_key and not self.cfg_starting_timestamp:
            raise ConfigValidationError(
                f"For stream {self.name} the starting timestamp field '{self._cfg_start_timestamp_key}' is required."
            )

    @property
    def url_base(self) -> str:
        base_url = (
            self.config.get("base_url")
            if self.config.get("base_url") is not None
            else "https://api.polygon.io"
        )
        return base_url

    @staticmethod
    def get_record_timestamp_key(record):
        record_timestamp_key = None
        for k in record.keys():
            if k in (
                "timestamp",
                "date",
                "ex_dividend_date",
                "record_date",
                "declaration_date",
                "pay_date",
            ):
                record_timestamp_key = k
                break
        return record_timestamp_key

    def get_new_paginator(self) -> PolygonAPIPaginator:
        return PolygonAPIPaginator()

    def paginate_records(
        self, url: str, query_params: dict[str, t.Any], **kwargs
    ) -> t.Iterable[dict[str, t.Any]]:
        query_params = query_params.copy()
        query_params_to_log = {k: v for k, v in query_params.items() if k != "apiKey"}
        logging.info(
            f"Streaming {self.name} with query_params: {query_params_to_log}..."
        )
        next_url = None
        while True:
            response = requests.get(next_url or url, params=query_params)
            response.raise_for_status()

            data = response.json()

            if isinstance(data, dict):
                records = data.get("results", data)
                if not data.get("results"):
                    break
            elif isinstance(data, list):
                records = data
                if not data:
                    break

            if isinstance(records, list):
                for record in records:
                    if self.DEBUG:
                        if self.name != "stock_tickers":
                            logging.debug("DEBUG")
                    if self._clean_in_place:
                        self.clean_record(record, **kwargs)
                    else:
                        record = self.clean_record(record, **kwargs)
                    check_missing_fields(self.schema, record)
                    yield record
            else:
                record = records
                if self.DEBUG:
                    if self.name != "stock_tickers":
                        logging.debug("DEBUG")
                if self._clean_in_place:
                    self.clean_record(record, **kwargs)
                else:
                    record = self.clean_record(record, **kwargs)
                if isinstance(record, list):
                    rs = record.copy()
                    for record in rs:
                        check_missing_fields(self.schema, record)
                        yield record
                else:
                    check_missing_fields(self.schema, record)
                    yield record

            if self.name == "market_holidays":
                break

            next_url = data.get("next_url")

            record_timestamp_key = self.get_record_timestamp_key(record)
            if (
                next_url
                and record_timestamp_key is not None
                and self._cfg_start_timestamp_key is not None
            ):
                if self.DEBUG and self.name != "stock_tickers":
                    logging.debug("DEBUG")
                if isinstance(record, list):
                    timestamps_seen = [
                        r.get(timestamp_key)
                        for r in record
                        if r.get(timestamp_key) is not None
                    ]
                    if not timestamps_seen:
                        logging.info(
                            f"No valid timestamps found in record list for {self.name}. Breaking."
                        )
                        break
                    last_timestamp = max(timestamps_seen)
                elif isinstance(record, dict):
                    last_timestamp = record.get(record_timestamp_key)
                    if last_timestamp is None:
                        logging.warning(
                            f"Timestamp key '{record_timestamp_key}' not found in record for {self.name}. Breaking."
                        )
                        break
                else:
                    logging.info(
                        f"No timestamps found in current batch for {self.name}. Breaking."
                    )
                    break

                if isinstance(last_timestamp, int) and last_timestamp > 1e12:
                    last_timestamp /= 1000

                if isinstance(last_timestamp, (int, float)):
                    last_ts_dt = datetime.utcfromtimestamp(last_timestamp).replace(
                        tzinfo=timezone.utc
                    )
                elif isinstance(last_timestamp, str):
                    last_ts_dt = datetime.fromisoformat(
                        last_timestamp.replace("Z", "+00:00")
                    ).replace(tzinfo=timezone.utc)

                if self._cfg_start_timestamp_key is None:
                    raise ConfigValidationError(
                        f"For stream {self.name} you must provide a starting timestamp in meltano.yml."
                    )

                cutoff_dt = datetime.fromisoformat(
                    self.cfg_starting_timestamp.replace("Z", "+00:00")
                ).replace(tzinfo=timezone.utc)

                if last_ts_dt < cutoff_dt:
                    logging.info(
                        f"Last record timestamp ({last_ts_dt}) in batch is older than 'from' timestamp ({cutoff_dt}). Breaking pagination."
                    )
                    break

                query_params = {"apiKey": self.query_params.get("apiKey")}

            if (
                next_url
                and record_timestamp_key is not None
                and hasattr(self, "_cfg_end_timestamp_key")
                and self._cfg_end_timestamp_key is not None
            ):
                if self.DEBUG and self.name != "stock_tickers":
                    logging.debug("DEBUG")

                last_timestamp_for_to = (
                    record.get(record_timestamp_key)
                    if isinstance(record, dict)
                    else (
                        max(
                            [
                                r.get(record_timestamp_key)
                                for r in record
                                if r.get(record_timestamp_key) is not None
                            ]
                        )
                        if isinstance(record, list)
                        and any(r.get(record_timestamp_key) is not None for r in record)
                        else None
                    )
                )

                if last_timestamp_for_to is None:
                    logging.info(
                        f"No valid timestamps found in current batch for {self.name} for 'to' check. Continuing."
                    )
                else:
                    if (
                        isinstance(last_timestamp_for_to, int)
                        and last_timestamp_for_to > 1e12
                    ):
                        last_timestamp_for_to /= 1000

                    last_ts_dt_for_to = datetime.utcfromtimestamp(
                        last_timestamp_for_to
                    ).replace(tzinfo=timezone.utc)

                    cutoff_to_dt_for_check = datetime.fromisoformat(
                        self.cfg_ending_timestamp.replace("Z", "+00:00")
                    ).replace(tzinfo=timezone.utc)

                    if last_ts_dt_for_to > cutoff_to_dt_for_check:
                        logging.info(
                            f"Latest record timestamp ({last_ts_dt_for_to}) in batch exceeds 'to' timestamp ({cutoff_to_dt_for_check}). Stopping pagination."
                        )
                        break

            if not next_url:
                if self.DEBUG and self.name != "stock_tickers":
                    logging.info("*** SHOULD BREAK ***")
                break

    def get_url(self, **kwargs):
        raise NotImplementedError(
            "Method get_url must be overridden in the stream class."
        )

    def get_url_params(
        self,
        context: t.Optional[t.Dict[str, t.Any]],
        next_page_token: t.Optional[t.Any],
    ) -> t.Union[dict[str, t.Any], str]:
        if next_page_token:
            return dict(parse_qsl(next_page_token.query))
        return {}

    def parse_config_params(self):
        cfg_params = self.config.get(self.name)

        self.path_params = {}
        self.query_params = {}

        if not cfg_params:
            logging.warning(f"No config set for stream '{self.name}', using defaults.")
        elif isinstance(cfg_params, dict):
            if "path_params" in cfg_params:
                self.path_params = cfg_params["path_params"]
            if "query_params" in cfg_params:
                self.query_params = cfg_params["query_params"]
        elif isinstance(cfg_params, list):
            for params in cfg_params:
                if not isinstance(params, dict):
                    raise ConfigValidationError(
                        f"Expected dict in '{self.name}', but got {type(params)}: {params}"
                    )
                if "path_params" in params:
                    self.path_params = params["path_params"]
                if "query_params" in params:
                    self.query_params = params["query_params"]
        else:
            raise ConfigValidationError(
                f"Config key '{self.name}' must be a dict or list of dicts."
            )

        if not isinstance(self.query_params, dict):
            self.query_params = {}

        self.query_params["apiKey"] = self.config.get("api_key")

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        if self._use_cached_tickers is None:
            raise ValueError(
                "The get_records method needs to know whether to use cached tickers."
            )

        if self._use_cached_tickers:
            ticker_records = self.tap.get_cached_tickers()
            for record in ticker_records:
                ticker = record.get("ticker")
                if self.DEBUG:
                    if self.name != "stock_tickers":
                        logging.debug("DEBUG")
                url = self.get_url(ticker=ticker)
                yield from self.paginate_records(url, self.query_params, ticker=ticker)
        else:
            url = self.get_url()
            yield from self.paginate_records(url, self.query_params)

    def clean_record(self, record: dict, **kwargs) -> dict:
        return record
