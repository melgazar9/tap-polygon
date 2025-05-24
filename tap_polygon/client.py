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


class PolygonAPIPaginator(BaseHATEOASPaginator):
    def get_next_url(self, response: requests.Response) -> t.Optional[str]:
        data = response.json()
        return data.get("next_url")


class PolygonRestStream(RESTStream):
    def __init__(self, tap):
        super().__init__(tap=tap)
        self.client = RESTClient(self.config["rest_api_key"])
        self._use_cached_tickers: bool | None = None
        self._clean_in_place = True
        self.parse_config_params()

        self._cfg_start_timestamp_key: str | None = None
        self._cfg_end_timestamp_key: str | None = None
        self.cfg_starting_timestamp: str | None = None
        self.cfg_end_timestamp_value: str | None = None

        self.timestamp_filter_fields = [
            "from", "from_", "to", "to_", "date", "timestamp", "ex_dividend_date",
            "record_date", "declaration_date", "pay_date", "listing_date",
            "execution_date", "filing_date", "period_of_report_date",
            "settlement_date", "published_utc",
        ]
        self.record_timestamp_keys = [
            "timestamp", "date", "last_updated", "ex_dividend_date", "record_date",
            "declaration_date", "pay_date", "last_updated_utc",
            "participant_timestamp", "sip_timestamp", "trf_timestamp",
            "announced_date", "listing_date", "execution_date", "filing_date",
            "acceptance_datetime", "end_date", "settlement_date", "published_utc",
        ]
        timestamp_filter_suffixes = ["gt", "gte", "lt", "lte"]
        self.timestamp_field_combos = self.timestamp_filter_fields + [
            f"{field}.{suffix}"
            for field in self.timestamp_filter_fields
            for suffix in timestamp_filter_suffixes
        ]

        self._set_timestamp_config_keys()

        if self._cfg_start_timestamp_key and not self.cfg_starting_timestamp:
            raise ConfigValidationError(
                f"For stream {self.name} the starting timestamp field "
                f"'{self._cfg_start_timestamp_key}' is configured but has no value."
            )

    def _check_missing_fields(self, schema: dict, record: dict):
        schema_fields = set(schema.get("properties", {}).keys())
        record_keys = set(record.keys())

        missing_in_record = schema_fields - record_keys
        if missing_in_record:
            logging.debug(
                f"*** Missing fields in record that are present in schema: {missing_in_record} for tap {self.name} ***"
            )

        missing_in_schema = record_keys - schema_fields
        if missing_in_schema:
            logging.critical(
                f"*** Missing fields in schema that are present record: {missing_in_schema} ***"
            )

    def _set_timestamp_config_keys(self) -> None:
        disallowed_start_keys = {"to", "to_"}
        allowed_end_keys = {"to", "to_"}

        for param_source in ("query_params", "path_params"):
            param_dict = getattr(self, param_source, None)
            if not param_dict:
                continue

            for k, v in param_dict.items():
                base_key = k.split(".")[0]

                if (
                        k in [i for i in self.timestamp_field_combos if not i.endswith((".lt", ".lte"))]
                        and base_key not in disallowed_start_keys
                ):
                    self._cfg_start_timestamp_key = k
                    self.cfg_starting_timestamp = v

                if (
                        k in [i for i in self.timestamp_field_combos if not i.endswith((".gt", ".gte"))]
                        and base_key in allowed_end_keys  # changed from NOT in disallowed_end_keys
                ):
                    self._cfg_end_timestamp_key = k
                    self.cfg_end_timestamp_value = v

            if self._cfg_start_timestamp_key:
                break

    @property
    def url_base(self) -> str:
        return self.config.get("base_url", "https://api.polygon.io")

    def get_starting_replication_key_value(self, context: Context | None) -> t.Any | None:
        def safe_parse_datetime(dt_str: t.Any) -> datetime | None:
            if not isinstance(dt_str, str):
                return None
            try:
                return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
            except ValueError:
                logging.warning(f"Could not parse datetime string: {dt_str}")
                return None

        cfg_dt = safe_parse_datetime(self.cfg_starting_timestamp)
        last_dt = safe_parse_datetime(context.get("last_ts_dt") if context else None)

        if self.replication_method == "INCREMENTAL":
            if last_dt and cfg_dt:
                return max(cfg_dt, last_dt).isoformat()
            if last_dt:
                return last_dt.isoformat()
            if cfg_dt:
                return cfg_dt.isoformat()

        if last_dt:
            return last_dt.isoformat()
        if cfg_dt:
            return cfg_dt.isoformat()

        logging.warning(f"No valid starting timestamp config or context for stream {self.name}.")
        return None

    def get_record_timestamp_key(self, record: dict | list) -> str | None:
        if isinstance(record, dict):
            for k in record.keys():
                if k in self.record_timestamp_keys:
                    return k
        elif isinstance(record, list) and record:
            if isinstance(record[0], dict):
                for k in record[0].keys():
                    if k in self.record_timestamp_keys:
                        return k

    def get_new_paginator(self) -> PolygonAPIPaginator:
        return PolygonAPIPaginator()

    @staticmethod
    def _normalize_timestamp(ts: int | float | str) -> float | str:
        if isinstance(ts, (int, float)):
            if ts > 1e15:  # likely nanoseconds
                return ts / 1e9
            if ts > 1e13:  # likely microseconds
                return ts / 1e6
            if ts > 1e10:  # likely milliseconds
                return ts / 1e3
        return ts

    @staticmethod
    def _to_datetime(ts: int | float | str | None) -> datetime | None:
        if isinstance(ts, (int, float)):
            try:
                return datetime.utcfromtimestamp(ts).replace(tzinfo=timezone.utc)
            except (ValueError, OSError) as e:
                logging.error(f"Error converting timestamp {ts} to datetime: {e}")
        if isinstance(ts, str):
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(
                    tzinfo=timezone.utc
                )
            except ValueError as e:
                logging.error(f"Error converting timestamp string '{ts}' to datetime: {e}")

    def _update_params_with_timestamp(self, current_timestamp: datetime.date, params: dict, is_path_param: bool) -> dict:
        """Updates parameters with the current date (which is the as-of/point-in-time timestamp), handling query vs. path keys."""
        updated_params = params.copy()
        if self._cfg_start_timestamp_key:
            if is_path_param:
                base_key = self._cfg_start_timestamp_key.split(".")[0]
                if base_key in inspect.getfullargspec(self.get_url).args:
                    updated_params[self._cfg_start_timestamp_key] = current_timestamp.isoformat()
            else:
                updated_params[self._cfg_start_timestamp_key] = current_timestamp.isoformat()
        return updated_params

    def _break_loop_check(self, context: Context) -> bool:
        if not context.get("next_url"):
            logging.debug(f"No 'next_url' in context for stream {self.name}. Breaking pagination.")
            return True

        record = context.get("record")
        record_timestamp_key = context.get("record_timestamp_key")

        if not record_timestamp_key:
            logging.info(f"No record timestamp key found for stream {self.name}. Continuing pagination.")
            return False

        last_timestamp = None
        if isinstance(record, list):
            # should be list of dicts
            timestamps_seen = [
                r.get(record_timestamp_key) for r in record
                if r.get(record_timestamp_key) is not None
            ]
            if not timestamps_seen:
                logging.info(f"No valid timestamps found in record list for {self.name}. Continuing.")
                return False
            last_timestamp = max(timestamps_seen)
        elif isinstance(record, dict):
            last_timestamp = record.get(record_timestamp_key)
            if last_timestamp is None:
                logging.warning(
                    f"Timestamp key '{record_timestamp_key}' not found in record dict for {self.name}. Continuing."
                )
                return False
        else:
            logging.info(f"Record is not a dict or list for {self.name}. Cannot perform timestamp checks. Continuing.")
            return False

        last_timestamp = self._normalize_timestamp(last_timestamp)
        last_ts_dt = self._to_datetime(last_timestamp)
        if last_ts_dt is None:
            logging.warning(f"Could not parse last timestamp for stream {self.name}. Continuing.")
            return False

        if self._cfg_start_timestamp_key:
            starting_replication_value = self.get_starting_replication_key_value(context)
            cutoff_start_dt = self._to_datetime(starting_replication_value)
            if cutoff_start_dt and last_ts_dt < cutoff_start_dt:
                logging.info(
                    f"Last record timestamp ({last_ts_dt}) in batch is older than 'from' timestamp ({cutoff_start_dt}). "
                    f"Breaking pagination for {self.name}."
                )
                return True

        if self._cfg_end_timestamp_key and self.cfg_end_timestamp_value:
            cutoff_end_dt = self._to_datetime(self.cfg_end_timestamp_value)
            if cutoff_end_dt and last_ts_dt > cutoff_end_dt:
                logging.info(
                    f"Latest record timestamp ({last_ts_dt}) in batch exceeds 'to' timestamp ({cutoff_end_dt}). "
                    f"Breaking pagination for {self.name}."
                )
                return True
        return False

    def _update_query_params(self, query_params):
        query_params = {
            k: v
            for k, v in query_params.items()
            if k not in self.timestamp_field_combos
        }
        return query_params

    def paginate_records(self, context: Context) -> t.Iterable[dict[str, t.Any]]:
        if "last_ts_dt" not in context or context["last_ts_dt"] is None:
            context["last_ts_dt"] = self.cfg_starting_timestamp or None
        query_params = context.get("query_params", {}).copy()
        path_params = context.get("path_params", {}).copy()  # copy to ensure get_url doesn't mutate the context's path_params

        next_url = None
        count = 0
        while True:
            if self.name != 'stock_tickers' and count > 0:
                logging.debug("DEBUG")
            request_url = next_url or self.get_url(context)
            query_params_to_log = {k: v for k, v in query_params.items() if k != "apiKey"}
            logging.info(
                f"Streaming {self.name} from URL: {request_url} with query_params: {query_params_to_log}..."
            )
            try:
                response = requests.get(request_url, params=query_params)
                response.raise_for_status()
                data = response.json()
                if self.name != 'stock_tickers' and count == 0:
                    logging.debug("DEBUG")
            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed for {self.name} at {request_url}: {e}")
                break
            except ValueError as e:
                logging.error(f"Failed to decode JSON for {self.name} at {request_url}: {e}")
                break

            if isinstance(data, dict):  # data from response should either be a list or dict
                records = data.get("results", data)
            elif isinstance(data, list):
                records = data
            else:
                raise ValueError(f"Expecting response data to be type list or dict, got type {type(data)}")

            if not records:
                logging.info(f"No records returned for {self.name}. Breaking pagination.")
                break

            if not isinstance(records, list):
                records = [records]

            processed_records = []  # keeps only a list of records in single response and then resets to an empty list [].
            for record in records:
                if self._clean_in_place:
                    self.clean_record(record, ticker=context.get("ticker"))
                    processed_records.append(record)
                else:
                    cleaned_record = self.clean_record(record, ticker=context.get("ticker"))
                    processed_records.append(cleaned_record)
                self._check_missing_fields(self.schema, record)
            yield from processed_records

            if self.name != 'stock_tickers':
                logging.debug('DEBUG')

            if isinstance(data, list):
                logging.info(f"Breaking out of loop for stream {self.name}")
                break

            next_url = data.get("next_url")
            query_params = self._update_query_params(query_params)
            count += 1

            context.update(
                {
                    "next_url": next_url,
                    "record": processed_records,
                    "record_timestamp_key": self.get_record_timestamp_key(processed_records),
                }
            )

            if self._break_loop_check(context):
                break

    def get_url(self, context: Context) -> str:
        raise NotImplementedError(
            "Method get_url must be overridden in the stream class."
        )

    def parse_config_params(self) -> None:
        cfg_params = self.config.get(self.name)
        self.path_params = {}
        self.query_params = {}
        self.other_params = {}

        if not cfg_params:
            logging.warning(f"No config set for stream '{self.name}', using defaults.")
        elif isinstance(cfg_params, dict):
            self.path_params = cfg_params.get("path_params", {})
            self.query_params = cfg_params.get("query_params", {})
            self.other_params = cfg_params.get("other_params", {})
        elif isinstance(cfg_params, list):
            for params_dict in cfg_params:
                if not isinstance(params_dict, dict):
                    raise ConfigValidationError(
                        f"Expected dict in '{self.name}', but got {type(params_dict)}: {params_dict}"
                    )
                self.path_params.update(params_dict.get("path_params", {}))
                self.query_params.update(params_dict.get("query_params", {}))
                self.other_params.update(params_dict.get("other_params", {}))
        else:
            raise ConfigValidationError(
                f"Config key '{self.name}' must be a dict or list of dicts."
            )
        self.query_params["apiKey"] = self.config.get("rest_api_key")

    def _yield_records_for_tickers(self, context: Context) -> t.Iterable[dict[str, t.Any]]:
        ticker_records = context.get("ticker_records")
        if not isinstance(ticker_records, list):
            raise ValueError("When yielding records by ticker, context['ticker_records'] must be a list.")

        for record in ticker_records:
            context["ticker"] = record.get("ticker")
            context["url"] = self.get_url(context)
            yield from self.paginate_records(context)

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        if self._use_cached_tickers is None:
            raise ValueError(
                "The get_records method needs to know whether to use cached tickers. "
                "Set _use_cached_tickers in your stream's __init__ or dynamically."
            )

        context = context if context is not None else {}
        loop_over_dates = self.other_params.get("loop_over_dates_gte_date", False)

        base_query_params = self.query_params.copy()
        base_path_params = self.path_params.copy()

        if loop_over_dates:
            if not self.cfg_starting_timestamp:
                raise ConfigValidationError(
                    f"Stream {self.name} is configured to loop over dates, but "
                    "'cfg_starting_timestamp' is not set."
                )

            start_date = datetime.strptime(self.cfg_starting_timestamp.split('T')[0], "%Y-%m-%d").date()
            end_date = datetime.today().date()

            if hasattr(self, "_cfg_end_timestamp_key") and self.cfg_end_timestamp_value:
                try:
                    configured_end_date = datetime.strptime(self.cfg_end_timestamp_value.split('T')[0], "%Y-%m-%d").date()
                    end_date = min(configured_end_date, end_date)
                except ValueError:
                    logging.warning(f"Could not parse _cfg_end_timestamp '{self.cfg_end_timestamp_value}'. Using today's date.")
            
            current_timestamp = start_date
            while current_timestamp <= end_date:
                date_loop_context = context.copy()

                date_loop_context["query_params"] = self._update_params_with_timestamp(
                    current_timestamp, base_query_params, is_path_param=False
                )
                date_loop_context["path_params"] = self._update_params_with_timestamp(
                    current_timestamp, base_path_params, is_path_param=True
                )

                if self._use_cached_tickers:
                    date_loop_context["ticker_records"] = self.tap.get_cached_tickers()
                    yield from self._yield_records_for_tickers(date_loop_context)
                else:
                    date_loop_context["url"] = self.get_url(date_loop_context)
                    yield from self.paginate_records(date_loop_context)
                
                current_timestamp += timedelta(days=1)
        else:
            context["query_params"] = base_query_params
            context["path_params"] = base_path_params

            if self._use_cached_tickers:
                context["ticker_records"] = self.tap.get_cached_tickers()
                yield from self._yield_records_for_tickers(context)
            else:
                context["url"] = self.get_url(context)
                yield from self.paginate_records(context)

    def clean_record(self, record: dict, ticker: str | None = None) -> dict:
        return record