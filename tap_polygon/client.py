import logging
import typing as t
from datetime import datetime, timedelta, timezone

import requests
from polygon import RESTClient

from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers._state import increment_state
from singer_sdk.helpers.types import Context
from singer_sdk.streams import RESTStream
from singer_sdk import typing as th


class PolygonRestStream(RESTStream):
    _incremental_timestamp_is_date = False
    _api_expects_unix_timestamp = False
    _unix_timestamp_unit = None
    _use_cached_tickers_default = True

    def __init__(self, tap):
        super().__init__(tap=tap)
        self.client = RESTClient(self.config["rest_api_key"])
        self.parse_config_params()

        self._cfg_start_timestamp_key: str | None = None
        self._cfg_end_timestamp_key: str | None = None
        self._cfg_starting_timestamp_value: str | None = None
        self._cfg_end_timestamp_value: str | None = None

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
            "announced_date",
            "published_utc",
            "ex_dividend_date",
            "sip_timestamp",
            "record_date",
            "trf_timestamp",
            "declaration_date",
            "pay_date",
            "last_updated_utc",
            "listing_date",
            "execution_date",
            "participant_timestamp",
            "filing_date",
            "acceptance_datetime",
            "end_date",
            "settlement_date",
        ]
        timestamp_filter_suffixes = ["gt", "gte", "lt", "lte"]
        self.timestamp_field_combos = self.timestamp_filter_fields + [
            f"{field}.{suffix}"
            for field in self.timestamp_filter_fields
            for suffix in timestamp_filter_suffixes
        ]

        self._set_timestamp_config_keys()

        if self._cfg_start_timestamp_key and not self._cfg_starting_timestamp_value:
            raise ConfigValidationError(
                f"For stream {self.name} the starting timestamp field "
                f"'{self._cfg_start_timestamp_key}' is configured but has no value."
            )

    @staticmethod
    def iso_to_unix_timestamp(dt_str: str, unit: str) -> int:
        """
        Convert ISO8601 string to Unix timestamp.
        unit: "s" (seconds), "ms" (milliseconds), "ns" (nanoseconds)
        """
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        ts = dt.timestamp()
        if unit == "s":
            return int(ts)
        elif unit == "ms":
            return int(ts * 1000)
        elif unit == "ns":
            return int(ts * 1_000_000_000)
        else:
            raise ValueError(f"Unknown unit: {unit}")

    def get_config_other_params(
        self, child_key: str, parent_key: str = "other_params", default=None
    ):
        """Fetch stream-specific config value."""
        stream_configs = self.config.get(self.name, [])
        for params in stream_configs:
            if parent_key in params:
                value = params["other_params"].get(child_key, default)
                return value
        return default

    @property
    def use_cached_tickers(self) -> bool:
        config_val = self.get_config_other_params("use_cached_tickers")
        if config_val is not None:
            assert isinstance(
                config_val, bool
            ), f"Config for use_cached_tickers must be bool, got {type(config_val)}"
            return config_val
        if hasattr(type(self), "_use_cached_tickers_default"):
            return getattr(type(self), "_use_cached_tickers_default")
        raise AttributeError("use_cached_tickers is not defined in config or class")

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
                    k
                    in [
                        i
                        for i in self.timestamp_field_combos
                        if not i.endswith((".lt", ".lte"))
                    ]
                    and base_key not in disallowed_start_keys
                ):
                    self._cfg_start_timestamp_key = k
                    self._cfg_starting_timestamp_value = v
                if (
                    k
                    in [
                        i
                        for i in self.timestamp_field_combos
                        if not i.endswith((".gt", ".gte"))
                    ]
                    and base_key in allowed_end_keys
                ):
                    self._cfg_end_timestamp_key = k
                    self._cfg_end_timestamp_value = v
            if self._cfg_start_timestamp_key:
                break

    @property
    def url_base(self) -> str:
        return self.config.get("base_url", "https://api.polygon.io")

    def safe_parse_datetime(self, dt_str: t.Any) -> datetime | None:
        if isinstance(dt_str, datetime):
            return (
                dt_str.replace(tzinfo=timezone.utc) if dt_str.tzinfo is None else dt_str
            )
        if isinstance(dt_str, (int, float)):
            try:
                seconds = self._timestamp_to_epoch(dt_str)
                return datetime.fromtimestamp(seconds, tz=timezone.utc)
            except (ValueError, OSError) as e:
                logging.warning(
                    f"Could not parse numeric timestamp: {dt_str}, error: {e}"
                )
                return None
        if isinstance(dt_str, str):
            try:
                return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                logging.warning(f"Could not parse datetime string: {dt_str}")
                return None
        return None

    @staticmethod
    def _timestamp_to_epoch(ts: int | float | str | None) -> float | None:
        if ts is None:
            return None
        if isinstance(ts, (int, float)):
            if ts > 1e15:  # nanoseconds
                return ts / 1e9
            if ts > 1e13:  # microseconds
                return ts / 1e6
            if ts > 1e10:  # milliseconds
                return ts / 1e3
            return float(ts)
        return None

    def get_starting_replication_key_value(
        self, context: Context | None
    ) -> t.Any | None:
        if (
            not self.is_timestamp_replication_key
            or self.replication_method != "INCREMENTAL"
        ):
            return None

        state = self.get_context_state(context)

        state_replication_value = state.get("replication_key_value") if state else None

        state_dt = self.safe_parse_datetime(state_replication_value)
        cfg_dt = self.safe_parse_datetime(self._cfg_starting_timestamp_value)

        if (
            state is not None
            and state_replication_value is None
            and "replication_key_value" in state
        ) or (state_dt is None and cfg_dt is None):
            logging.critical(
                f"Unable to parse datetimes for state_dt and cfg_dt! Check stream {self.name}"
            )

        if (
            self.is_timestamp_replication_key
            and self.replication_method == "INCREMENTAL"
        ):
            if state_dt and cfg_dt:
                return max(state_dt, cfg_dt).isoformat()
            if state_dt:
                return state_dt.isoformat()
            if cfg_dt:
                return cfg_dt.isoformat()
        return None

    def get_record_timestamp_key(self, record: dict | list) -> str | None:
        target_record = None
        if isinstance(record, dict):
            target_record = record
        elif isinstance(record, list) and record and isinstance(record[0], dict):
            target_record = record[0]
        if target_record:
            for preferred_key in self.record_timestamp_keys:
                if preferred_key in target_record:
                    return preferred_key

    def _update_query_path_params_with_state(
        self,
        context: Context,
        query_params: dict,
        path_params: dict,
        pop_timestamp: bool = False,
    ) -> tuple[dict, dict]:
        if pop_timestamp:
            query_params = {
                k: v
                for k, v in query_params.items()
                if k not in self.timestamp_field_combos
            }
            path_params = {
                k: v
                for k, v in path_params.items()
                if k not in self.timestamp_field_combos
            }
        else:
            if self.is_timestamp_replication_key and self._cfg_start_timestamp_key:
                starting_replication_key_value = (
                    self.get_starting_replication_key_value(context)
                )
                if starting_replication_key_value:
                    if self._cfg_start_timestamp_key in query_params:
                        query_params[self._cfg_start_timestamp_key] = (
                            starting_replication_key_value
                        )
                    if self._cfg_start_timestamp_key in path_params:
                        path_params[self._cfg_start_timestamp_key] = (
                            starting_replication_key_value
                        )
        return query_params, path_params

    def paginate_records(self, context: Context) -> t.Iterable[dict[str, t.Any]]:
        query_params = context.get("query_params", {}).copy()
        path_params = context.get("path_params", {}).copy()
        self.normalize_date_params(
            query_params, force_date=self._incremental_timestamp_is_date
        )
        self.normalize_date_params(
            path_params, force_date=self._incremental_timestamp_is_date
        )

        request_context = dict(
            ticker=context.get("ticker"),
            query_params=query_params,
            path_params=path_params,
        )

        if "query_params" in context:
            context.pop("query_params")
        if "path_params" in context:
            context.pop("path_params")

        next_url = None
        no_records_counter = 0

        state = self.get_context_state(context)

        while True:
            request_url = next_url or self.get_url(request_context)
            query_params_to_log = {
                k: v for k, v in query_params.items() if k != "apiKey"
            }
            logging.info(
                f"Streaming {self.name} from URL: {request_url} with query_params: {query_params_to_log}..."
            )
            try:
                response = requests.get(request_url, params=query_params)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed for {self.name} at {request_url}: {e}")
                break
            except ValueError as e:
                logging.error(
                    f"Failed to decode JSON for {self.name} at {request_url}: {e}"
                )
                break

            if isinstance(data, dict):
                records = data.get("results", data)
            elif isinstance(data, list):
                records = data
            else:
                raise ValueError(
                    f"Expecting response data to be type list or dict, got type {type(data)} for stream {self.name}."
                )

            if not records:
                logging.info(
                    f"No records returned for {self.name} in this batch. Checking if it's a persistent empty response."
                )
                no_records_counter += 1
                if no_records_counter >= 3:
                    logging.info(
                        f"Breaking pagination for {self.name} due to {no_records_counter} consecutive empty record batches."
                    )
                    break
            else:
                no_records_counter = 0

            if not isinstance(records, list):
                records = [records]

            latest_record = None
            for raw_record in records:
                for record in self.parse_response(raw_record, context):
                    record = self.post_process(record, context)
                    if not record:
                        continue
                    try:
                        self._check_missing_fields(self.schema, record)
                    except TypeError as e:
                        logging.error(
                            f"Failed to parse record for {self.name} at {request_url}: {e}"
                        )
                    latest_record = record
                    yield record

            if self.replication_method == "INCREMENTAL" and latest_record is not None:
                increment_state(
                    state,
                    replication_key=self.replication_key,
                    latest_record=latest_record,
                    is_sorted=self.is_sorted,
                    check_sorted=self.check_sorted,
                )

                if (
                    "progress_markers" in state
                    and "replication_key_value" in state["progress_markers"]
                ):
                    state["replication_key_value"] = state["progress_markers"][
                        "replication_key_value"
                    ]

            if isinstance(data, list):
                logging.info(
                    f"Breaking out of loop for stream {self.name}. Not checking pagination for next_url."
                )
                break

            next_url = data.get("next_url")
            query_params, path_params = self._update_query_path_params_with_state(
                context, query_params, path_params
            )
            replication_key_value = self.get_starting_replication_key_value(context)
            if self._break_loop_check(next_url, replication_key_value):
                break

    def get_url(self, context: Context) -> str:
        raise NotImplementedError(
            "Method get_url must be overridden in the stream class."
        )

    def parse_response(self, record: dict, context: Context) -> t.Iterable[dict]:
        """Default passthrough: yield the record unchanged."""
        yield record

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

    def normalize_date_params(self, params: dict, force_date: bool = False) -> None:
        """
        Normalize date/datetime params inplace, for (query and path)
          params prior to sending a request or building the url endpoint.
        """
        if self.is_timestamp_replication_key:
            if self._cfg_start_timestamp_key in params:
                params[self._cfg_start_timestamp_key] = self.safe_parse_datetime(
                    params[self._cfg_start_timestamp_key]
                )

                if self._incremental_timestamp_is_date:
                    params[self._cfg_start_timestamp_key] = (
                        params[self._cfg_start_timestamp_key].date().isoformat()
                    )
                else:
                    params[self._cfg_start_timestamp_key] = params[
                        self._cfg_start_timestamp_key
                    ].isoformat()
                if self._api_expects_unix_timestamp:
                    assert self._unix_timestamp_unit in (
                        "s",
                        "ms",
                        "ns",
                    ), (
                        f"_unix_timestamp_unit in stream {self.name} must be int."
                        f"Currently it's set to value {self._unix_timestamp_unit}"
                    )
                    params[self._cfg_start_timestamp_key] = self.iso_to_unix_timestamp(
                        params[self._cfg_start_timestamp_key],
                        unit=self._unix_timestamp_unit,
                    )

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        """
        Only handle one partition per get_records call.
        Let the tap framework handle looping over partitions (i.e., per ticker).
        """
        context = context if context is not None else {}
        loop_over_dates = self.other_params.get("loop_over_dates_gte_date", False)

        base_query_params = self.query_params.copy()
        base_path_params = self.path_params.copy()

        query_params, path_params = self._update_query_path_params_with_state(
            context, base_query_params, base_path_params
        )
        context["query_params"] = query_params
        context["path_params"] = path_params

        if not loop_over_dates:
            yield from self.paginate_records(context)
        else:
            if not self._cfg_starting_timestamp_value:
                raise ConfigValidationError(
                    f"Stream {self.name} is configured to loop over dates, but "
                    "'_cfg_starting_timestamp' is not set."
                )

            start_date = datetime.strptime(
                self._cfg_starting_timestamp_value.split("T")[0], "%Y-%m-%d"
            ).date()

            end_date = datetime.today().date()

            if self._cfg_end_timestamp_key and self.cfg_end_timestamp_value:
                try:
                    configured_end_date = datetime.strptime(
                        self._cfg_end_timestamp_value.split("T")[0], "%Y-%m-%d"
                    ).date()
                    end_date = min(configured_end_date, end_date)
                except ValueError:
                    logging.warning(
                        f"Could not parse _cfg_end_timestamp '{self.cfg_end_timestamp_value}'. Using today's date."
                    )

            current_timestamp = start_date

            while current_timestamp <= end_date:
                context["query_params"] = query_params.copy()
                context["path_params"] = path_params.copy()
                if self._cfg_start_timestamp_key in context.get("query_params"):
                    context["query_params"][
                        self._cfg_start_timestamp_key
                    ] = current_timestamp.isoformat()
                if self._cfg_start_timestamp_key in context.get("path_params"):
                    context["path_params"][
                        self._cfg_start_timestamp_key
                    ] = current_timestamp.isoformat()
                yield from self.paginate_records(context)
                current_timestamp += timedelta(days=1)

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

    def _has_timestamp_field(self) -> bool:
        """Return True if this stream has a valid timestamp/replication key field."""
        return bool(
            self.replication_key and self.replication_key in self.record_timestamp_keys
        )

    def _break_loop_check(self, next_url, replication_key_value) -> bool:
        if not next_url:
            logging.debug(
                f"No 'next_url' in context for stream {self.name}. Breaking pagination."
            )
            return True

        if not self._has_timestamp_field():
            return False

        if self.replication_method != "INCREMENTAL":
            return False

        if replication_key_value is None:
            logging.info(
                f"No '{self.replication_key}' found in context for stream {self.name}. Continuing pagination."
            )
            return False

        last_ts_dt = self.safe_parse_datetime(replication_key_value)

        if last_ts_dt is None:
            logging.warning(
                f"Could not parse '{self.replication_key}' from context for stream {self.name}. Continuing."
            )
            return False

        if self._cfg_start_timestamp_key:
            cutoff_start_dt = self.safe_parse_datetime(
                self._cfg_starting_timestamp_value
            )

            if cutoff_start_dt and last_ts_dt < cutoff_start_dt:
                logging.info(
                    f"Last record timestamp ({last_ts_dt}) in batch is older than 'from' timestamp ({cutoff_start_dt}). "
                    f"Breaking pagination for {self.name}."
                )
                return True

        if self._cfg_end_timestamp_key and self._cfg_end_timestamp_value:
            cutoff_end_dt = self.safe_parse_datetime(self._cfg_end_timestamp_value)
            if cutoff_end_dt and last_ts_dt > cutoff_end_dt:
                logging.info(
                    f"Latest record timestamp ({last_ts_dt}) in batch exceeds 'to' timestamp ({cutoff_end_dt}). "
                    f"Breaking pagination for {self.name}."
                )
                return True
        return False


class TickersStream(PolygonRestStream):
    """Fetch all tickers from Polygon."""

    name = "tickers"

    primary_keys = ["cik", "ticker"]

    schema = th.PropertiesList(
        th.Property("cik", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("name", th.StringType),
        th.Property("active", th.BooleanType),
        th.Property("currency_symbol", th.StringType),
        th.Property("currency_name", th.StringType),
        th.Property("base_currency_symbol", th.StringType),
        th.Property("composite_figi", th.StringType),
        th.Property("base_currency_name", th.StringType),
        th.Property("delisted_utc", th.StringType),
        th.Property("last_updated_utc", th.StringType),
        th.Property("locale", th.StringType),
        th.Property("market", th.StringType),
        th.Property("primary_exchange", th.StringType),
        th.Property("share_class_figi", th.StringType),
        th.Property("type", th.StringType),
        th.Property("source_feed", th.StringType),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _break_loop_check(self, next_url, replication_key_value=None) -> bool:
        return not next_url

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/reference/tickers"

    def get_ticker_list(self) -> list[str] | None:
        tickers_cfg = self.config.get("tickers", {})
        tickers = tickers_cfg.get("tickers") if tickers_cfg else None

        if not tickers:
            return None

        if isinstance(tickers, str):
            if tickers == "*":
                return None
            try:
                parsed = json.loads(tickers)
                if parsed == ["*"]:
                    return None
                return parsed if isinstance(parsed, list) else [parsed]
            except json.JSONDecodeError:
                return [tickers]

        if isinstance(tickers, list):
            if tickers == ["*"]:
                return None
            return tickers
        return None

    def get_child_context(self, record, context):
        return {"ticker": record.get("ticker")}

    def get_records(
        self, context: dict[str, t.Any] | None
    ) -> t.Iterable[dict[str, t.Any]]:
        context = {} if context is None else context
        ticker_list = self.get_ticker_list()
        query_params = self.query_params.copy()
        if not ticker_list:
            logging.info("Pulling all tickers...")
            yield from self.paginate_records(context)
        else:
            logging.info(f"Pulling specific tickers: {ticker_list}")
            for ticker in ticker_list:
                query_params.update({"ticker": ticker})
                context["query_params"] = query_params
                yield from self.paginate_records(context)


class CachedTickerProvider:
    def __init__(self, tap):
        self.tap = tap
        self._tickers = None

    def get_tickers(self):
        if self._tickers is None:
            logging.info(
                "Tickers have not been downloaded yet. Retrieving from tap cache..."
            )
            self._tickers = self.tap.get_cached_tickers()
        return self._tickers