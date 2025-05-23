"""REST client handling, including PolygonStream base class."""

from __future__ import annotations

import inspect
import logging
import typing as t
from datetime import datetime, timedelta, timezone

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
        self.client = RESTClient(self.config["rest_api_key"])

        self._use_cached_tickers = None
        self._clean_in_place = True
        self.parse_config_params()

        self._cfg_start_timestamp_key = None

        self.DEBUG = False

        self.timestamp_filter_fields = [
            "from",
            "from_",
            "to",
            "to_",
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

        self.record_timestamp_keys = [
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
        ]

        timestamp_filter_suffixes = ["gt", "gte", "lt", "lte"]

        self.timestamp_field_combos = self.timestamp_filter_fields + [
            f"{field}.{suffix}"
            for field in self.timestamp_filter_fields
            for suffix in timestamp_filter_suffixes
        ]

        if self.DEBUG and self.name != "stock_tickers":
            logging.debug("DEBUG")

        disallowed_start_keys = {"to", "to_"}
        disallowed_end_keys = {"from", "from_"}

        for param_source in ("query_params", "path_params"):
            param_dict = getattr(self, param_source, None)
            if param_dict:
                for k, v in param_dict.items():
                    base_key = k.split(".")[0]

                    # Starting timestamp (gt/gte), disallow "to" and "to_"
                    if (
                        k
                        in [
                            i
                            for i in self.timestamp_field_combos
                            if not i.endswith(".lt") and not i.endswith(".lte")
                        ]
                        and base_key not in disallowed_start_keys
                    ):
                        self._cfg_start_timestamp_key = k
                        self.cfg_starting_timestamp = v

                    # Ending timestamp (lt/lte), disallow "from" and "from_"
                    if (
                        k
                        in [
                            i
                            for i in self.timestamp_field_combos
                            if not i.endswith(".gt") and not i.endswith(".gte")
                        ]
                        and base_key not in disallowed_end_keys
                    ):
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

    def get_record_timestamp_key(self, record):
        record_timestamp_key = None
        if isinstance(record, dict):
            for k in record.keys():
                if k in self.record_timestamp_keys:
                    record_timestamp_key = k
                    break
        elif isinstance(record, list) and len(list):
            logging.info('hi')
        else:
            raise ValueError(f"Record is not a dict or list in stream {self.name}!")
        return record_timestamp_key

    def get_new_paginator(self) -> PolygonAPIPaginator:
        return PolygonAPIPaginator()

    @staticmethod
    def _normalize_timestamp(ts):
        if isinstance(ts, (int, float)):
            if ts > 1e15:  # nanoseconds
                return ts / 1e9
            if ts > 1e13:  # microseconds
                return ts / 1e6
            if ts > 1e10:  # milliseconds
                return ts / 1e3
        return ts

    @staticmethod
    def _to_datetime(ts):
        if isinstance(ts, (int, float)):
            return datetime.utcfromtimestamp(ts).replace(tzinfo=timezone.utc)
        if isinstance(ts, str):
            return datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(
                tzinfo=timezone.utc
            )

    def _update_query_params(self, query_params):
        query_params = {
            k: v
            for k, v in query_params.items()
            if k not in self.timestamp_field_combos
        }
        return query_params

    def _break_loop_check(self, next_url, record, record_timestamp_key):
        if self.name != 'stock_tickers':
            logging.debug('debug')
        if not next_url:
            if self.name != "stock_tickers":
                logging.debug("DEBUG")
            return True

        if record_timestamp_key and self._cfg_start_timestamp_key:
            if self.name != "stock_tickers":
                logging.debug("DEBUG")

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
                    return True
                last_timestamp = max(timestamps_seen)
            elif isinstance(record, dict):
                last_timestamp = record.get(record_timestamp_key)
                if last_timestamp is None:
                    logging.warning(
                        f"Timestamp key '{record_timestamp_key}' not found in record for {self.name}. Breaking."
                    )
                    return True
            else:
                logging.info(
                    f"No timestamps found in current batch for {self.name}. Breaking."
                )
                return True

            last_timestamp = self._normalize_timestamp(last_timestamp)
            last_ts_dt = self._to_datetime(last_timestamp)

            if self._cfg_start_timestamp_key is None:
                raise ConfigValidationError(
                    f"For stream {self.name} you must provide a starting timestamp in meltano.yml."
                )

            cutoff_start_dt = self._to_datetime(self.cfg_starting_timestamp)

            if last_ts_dt < cutoff_start_dt:
                logging.info(
                    f"Last record timestamp ({last_ts_dt}) in batch is older than 'from' timestamp ({cutoff_start_dt})."
                    f"Breaking pagination."
                )
                return True

        if (
                record_timestamp_key is not None
                and hasattr(self, "_cfg_end_timestamp_key")
                and self._cfg_end_timestamp_key is not None
        ):
            if self.name != "stock_tickers":
                logging.debug("DEBUG")

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

                if last_ts_dt_for_to is None:
                    raise ValueError("Could not parse last_ts_dt_for_to")

                cutoff_end_to_dt = self._to_datetime(self.cfg_ending_timestamp)

                if last_ts_dt_for_to > cutoff_end_to_dt:
                    logging.info(
                        f"Latest record timestamp ({last_ts_dt_for_to}) in batch exceeds 'to'"
                        f"timestamp ({cutoff_end_to_dt}). Breaking pagination."
                    )
                    return True

    def paginate_records(self, context: Context) -> t.Iterable[dict[str, t.Any]]:
        """ Context needs to contain url: str and query_params: dict[str, t.Any]"""
        if self.name != 'stock_tickers':
            logging.debug('hi')

        if "path_params" in context:
            path_params = context.get("path_params").copy()
        if "query_params" in context:
            query_params = context["query_params"].copy()
            query_params_to_log = {k: v for k, v in query_params.items() if k != "apiKey"}
            logging.info(
            f"Streaming {self.name} with query_params: {query_params_to_log}..."
        )

        count = 0
        next_url = None
        while True:
            request_url = next_url or self.get_url(context)
            response = requests.get(request_url, params=query_params)

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
                    if self.name != "stock_tickers":
                        logging.debug("DEBUG")
                    if self._clean_in_place:
                        self.clean_record(record, ticker=context.get("ticker"))
                    else:
                        record = self.clean_record(record, ticker=context.get("ticker"))
                    check_missing_fields(self.schema, record)
                    yield record
            else:
                record = records
                if self.name != "stock_tickers":
                    logging.debug("DEBUG")

                if self._clean_in_place:
                    self.clean_record(record, ticker=context.get("ticker"))
                else:
                    record = self.clean_record(record, ticker=context.get("ticker"))

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
            if isinstance(record, list):
                logging.debug('h')
            record_timestamp_key = self.get_record_timestamp_key(record)
            count += 1
            if self.name != 'stock_tickers':
                logging.debug('debug')

            if self._break_loop_check(next_url, record, record_timestamp_key):
                break

            query_params = self._update_query_params(query_params)

    def get_url(self, context: Context):
        raise NotImplementedError(
            "Method get_url must be overridden in the stream class."
        )

    def parse_config_params(self):
        cfg_params = self.config.get(self.name)

        self.path_params = {}
        self.query_params = {}
        self.other_params = {}

        if not cfg_params:
            logging.warning(f"No config set for stream '{self.name}', using defaults.")
        elif isinstance(cfg_params, dict):
            if "path_params" in cfg_params:
                self.path_params = cfg_params["path_params"]
            if "query_params" in cfg_params:
                self.query_params = cfg_params["query_params"]
            if "other_params" in cfg_params:
                self.other_params = cfg_params["other_params"]
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
                if "other_params" in params:
                    self.other_params = params["other_params"]
        else:
            raise ConfigValidationError(
                f"Config key '{self.name}' must be a dict or list of dicts."
            )

        self.query_params["apiKey"] = self.config.get("rest_api_key")

    def _yield_records_for_tickers(self, context):
        path_params = {} if context.get("path_params") is None else context.get("path_params")
        assert isinstance(context.get("ticker_records"), list), "When yielding records by ticker context must be a list."
        for record in context.get("ticker_records"):
            context["ticker"] = record.get("ticker")
            context["url"] = self.get_url(context)
            yield from self.paginate_records(context)

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        if self._use_cached_tickers is None:
            raise ValueError(
                "The get_records method needs to know whether to use cached tickers."
            )

        context = {} if context is None else context

        loop_over_dates = self.other_params.get("loop_over_dates_gte_date", False)
        query_params = self.query_params.copy()
        path_params = self.path_params.copy()

        if loop_over_dates:
            start_date = datetime.strptime(
                self.cfg_starting_timestamp, "%Y-%m-%d"
            ).date()
            end_date = (
                min(
                    datetime.strptime(self._cfg_end_timestamp, "%Y-%m-%d"),
                    datetime.today(),
                ).date()
                if hasattr(self, "_cfg_end_timestamp")
                else datetime.today().date()
            )

            current_date = start_date
            while current_date < end_date:
                # query_params date update
                if self._cfg_start_timestamp_key in query_params:
                    query_params[self._cfg_start_timestamp_key] = (
                        current_date.isoformat()
                    )

                # path_params date update
                if (
                    self._cfg_start_timestamp_key
                    in inspect.getfullargspec(self.get_url).args
                ):
                    path_params[self._cfg_start_timestamp_key] = current_date.isoformat()

                context["query_params"] = query_params
                context["path_params"] = path_params

                if self._use_cached_tickers:
                    context["ticker_records"] = self.tap.get_cached_tickers()
                    yield from self._yield_records_for_tickers(context)
                else:
                    yield from self.paginate_records(context)
                current_date += timedelta(days=1)
        else:
            context["query_params"] = query_params
            context["path_params"] = path_params
            if self._use_cached_tickers:
                context["ticker_records"] = self.tap.get_cached_tickers()
                yield from self._yield_records_for_tickers(context)
            else:
                context["url"] = self.get_url(context)
                yield from self.paginate_records(context)

    def clean_record(self, record: dict, ticker=None) -> dict:
        return record
