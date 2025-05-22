"""REST client handling, including PolygonStream base class."""

from __future__ import annotations

import logging
import typing as t
from datetime import datetime, timezone

import requests
from polygon import RESTClient
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers.types import Context
from singer_sdk.pagination import BaseHATEOASPaginator
from singer_sdk.streams import RESTStream

from tap_polygon.utils import check_missing_fields


class PolygonAPIPaginator(BaseHATEOASPaginator):
    def get_next_url(self, response: requests.Response) -> t.Optional[str]:
        data = response.json()
        return data.get("next_url")


class PolygonRestStream(RESTStream):
    """Polygon rest API stream class."""

    def __init__(self, tap):
        super().__init__(tap=tap)
        self.client = RESTClient(self.config["api_key"])

        self._use_cached_tickers = None
        self._clean_in_place = True
        self.parse_config_params()

        self._cfg_start_timestamp_key = None

        self.DEBUG = True

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

    def _debug(self):
        if self.DEBUG and self.name != "stock_tickers":
            logging.debug("DEBUG")
        return

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
                "last_updated",
                "ex_dividend_date",
                "record_date",
                "declaration_date",
                "pay_date",
                "last_updated_utc",
                "participant_timestamp",
                "sip_timestamp",
                "trf_timestamp",
                "announced_date",
                "listing_date",
                "execution_date",
                "filing_date",
                "acceptance_datetime",
                "end_date",
                "settlement_date",
                "published_utc",
            ):
                record_timestamp_key = k
                break
        return record_timestamp_key

    def get_new_paginator(self) -> PolygonAPIPaginator:
        return PolygonAPIPaginator()

    @staticmethod
    def _normalize_timestamp(ts):
        if isinstance(ts, int):
            if ts > 1e12:  # likely nanoseconds
                return ts / 1e9
            if ts > 1e9:  # likely microseconds
                return ts / 1e6
        return ts

    @staticmethod
    def _to_datetime(ts):
        if isinstance(ts, (int, float)):
            return datetime.utcfromtimestamp(ts).replace(tzinfo=timezone.utc)
        if isinstance(ts, str):
            return datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(
                tzinfo=timezone.utc
            )
        return

    def paginate_records(
        self, url: str, query_params: dict[str, t.Any], **kwargs
    ) -> t.Iterable[dict[str, t.Any]]:
        self._debug()
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
                if not data.get("results", data):
                    break
            elif isinstance(data, list):
                records = data
                if not data:
                    break
            else:
                logging.critical(
                    f"*** Could not parse record for stream {self.name} ***"
                )
                break

            if isinstance(records, list):
                for record in records:
                    self._debug()
                    if self._clean_in_place:
                        self.clean_record(record, **kwargs)
                    else:
                        record = self.clean_record(record, **kwargs)
                    check_missing_fields(self.schema, record)
                    yield record
            else:
                record = records
                self._debug()
                if self._clean_in_place:
                    self.clean_record(record, **kwargs)
                else:
                    record = self.clean_record(record, **kwargs)

                if isinstance(record, list):
                    for r in record.copy():
                        check_missing_fields(self.schema, record)
                        yield r
                else:
                    check_missing_fields(self.schema, record)
                    yield record

            if self.name == "market_holidays":
                break

            next_url = data.get("next_url")
            record_timestamp_key = self.get_record_timestamp_key(record)

            if not next_url:
                self._debug()
                break

            if record_timestamp_key and self._cfg_start_timestamp_key:
                self._debug()
                if isinstance(record, list):
                    timestamps_seen = [
                        r.get(record_timestamp_key)
                        for r in record
                        if r.get(record_timestamp_key) is not None
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

                last_timestamp = self._normalize_timestamp(last_timestamp)
                last_ts_dt = self._to_datetime(last_timestamp)

                if self._cfg_start_timestamp_key is None:
                    raise ConfigValidationError(
                        f"For stream {self.name} you must provide a starting timestamp in meltano.yml."
                    )

                cutoff_dt = self._to_datetime(self.cfg_starting_timestamp)

                if last_ts_dt < cutoff_dt:
                    logging.info(
                        f"Last record timestamp ({last_ts_dt}) in batch is older than 'from' timestamp ({cutoff_dt})."
                        f"Breaking pagination."
                    )
                    break

                query_params = {"apiKey": self.query_params.get("apiKey")}

            if (
                record_timestamp_key is not None
                and hasattr(self, "_cfg_end_timestamp_key")
                and self._cfg_end_timestamp_key is not None
            ):
                self._debug()

                if isinstance(record, dict):
                    last_timestamp_for_to = record.get(record_timestamp_key)
                elif isinstance(record, list):
                    valid_ts = [
                        r.get(record_timestamp_key)
                        for r in record
                        if r.get(record_timestamp_key) is not None
                    ]
                    last_timestamp_for_to = max(valid_ts) if valid_ts else None
                else:
                    last_timestamp_for_to = None

                if last_timestamp_for_to is None:
                    logging.info(
                        f"No valid timestamps found in current batch for {self.name} for 'to' check. Continuing."
                    )
                else:
                    last_timestamp_for_to = self._normalize_timestamp(
                        last_timestamp_for_to
                    )
                    last_ts_dt_for_to = self._to_datetime(last_timestamp_for_to)

                    self._debug()

                    if last_ts_dt_for_to is None:
                        raise ValueError("Could not parse last_ts_dt_for_to")

                    cutoff_to_dt_for_check = self._to_datetime(
                        self.cfg_ending_timestamp
                    )

                    if last_ts_dt_for_to > cutoff_to_dt_for_check:
                        logging.info(
                            f"Latest record timestamp ({last_ts_dt_for_to}) in batch exceeds 'to'"
                            f"timestamp ({cutoff_to_dt_for_check}). Breaking pagination."
                        )
                        break

            if not next_url:
                self._debug()
                break

    def get_url(self, **kwargs):
        raise NotImplementedError(
            "Method get_url must be overridden in the stream class."
        )

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
                self._debug()
                url = self.get_url(ticker=ticker)
                yield from self.paginate_records(url, self.query_params, ticker=ticker)
        else:
            url = self.get_url()
            yield from self.paginate_records(url, self.query_params)

    def clean_record(self, record: dict, **kwargs) -> dict:
        return record
