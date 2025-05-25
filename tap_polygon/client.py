import inspect
import logging
import typing as t
from datetime import datetime, timedelta

import requests
from polygon import RESTClient
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers._state import increment_state
from singer_sdk.helpers.types import Context
from singer_sdk.streams import RESTStream

from tap_polygon.utils import safe_parse_datetime


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
                    k
                    in [
                        i
                        for i in self.timestamp_field_combos
                        if not i.endswith((".lt", ".lte"))
                    ]
                    and base_key not in disallowed_start_keys
                ):
                    self._cfg_start_timestamp_key = k
                    self.cfg_starting_timestamp = v

                if (
                    k
                    in [
                        i
                        for i in self.timestamp_field_combos
                        if not i.endswith((".gt", ".gte"))
                    ]
                    and base_key
                    in allowed_end_keys  # changed from NOT in disallowed_end_keys
                ):
                    self._cfg_end_timestamp_key = k
                    self.cfg_end_timestamp_value = v

            if self._cfg_start_timestamp_key:
                break

    @property
    def url_base(self) -> str:
        return self.config.get("base_url", "https://api.polygon.io")

    @staticmethod
    def get_next_url(data) -> t.Optional[str]:
        return data.get("next_url")

    def get_starting_replication_key_value(
        self, context: Context | None
    ) -> t.Any | None:
        state_replication_value = (
            context.get("replication_key_value") if context else None
        )
        state_dt = safe_parse_datetime(state_replication_value)
        cfg_dt = safe_parse_datetime(self.cfg_starting_timestamp)

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

        logging.warning(
            f"No valid starting timestamp config or context for stream {self.name}."
        )
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

    def _update_params_with_timestamp(
        self, current_timestamp: datetime.date, params: dict, is_path_param: bool
    ) -> dict:
        """Updates parameters with the current date (the as-of/point-in-time timestamp), handling query vs. path keys."""
        updated_params = params.copy()
        if self._cfg_start_timestamp_key:
            if is_path_param:
                base_key = self._cfg_start_timestamp_key.split(".")[0]
                if base_key in inspect.getfullargspec(self.get_url).args:
                    updated_params[self._cfg_start_timestamp_key] = (
                        current_timestamp.isoformat()
                    )
            else:
                updated_params[self._cfg_start_timestamp_key] = (
                    current_timestamp.isoformat()
                )
        return updated_params

    def _update_query_params(self, query_params):
        query_params = {
            k: v
            for k, v in query_params.items()
            if k not in self.timestamp_field_combos
        }
        return query_params

    def _break_loop_check(self, context: Context) -> bool:
        if not context.get("next_url"):
            logging.debug(
                f"No 'next_url' in context for stream {self.name}. Breaking pagination."
            )
            return True

        last_timestamp_str = context.get("replication_key_value")

        if last_timestamp_str is None:
            logging.info(
                f"No '{self.replication_key}' found in context for stream {self.name}. Continuing pagination."
            )
            return False

        last_ts_dt = safe_parse_datetime(last_timestamp_str)

        if last_ts_dt is None:
            logging.warning(
                f"Could not parse '{self.replication_key}' from context for stream {self.name}. Continuing."
            )
            return False

        if self._cfg_start_timestamp_key:
            starting_replication_value = self.get_starting_replication_key_value(
                context
            )
            cutoff_start_dt = safe_parse_datetime(starting_replication_value)

            if cutoff_start_dt and last_ts_dt < cutoff_start_dt:
                logging.info(
                    f"Last record timestamp ({last_ts_dt}) in batch is older than 'from' timestamp ({cutoff_start_dt}). "
                    f"Breaking pagination for {self.name}."
                )
                return True

        if self._cfg_end_timestamp_key and self.cfg_end_timestamp_value:
            cutoff_end_dt = safe_parse_datetime(self.cfg_end_timestamp_value)
            if cutoff_end_dt and last_ts_dt > cutoff_end_dt:
                logging.info(
                    f"Latest record timestamp ({last_ts_dt}) in batch exceeds 'to' timestamp ({cutoff_end_dt}). "
                    f"Breaking pagination for {self.name}."
                )
                return True
        return False

    def paginate_records(self, context: Context) -> t.Iterable[dict[str, t.Any]]:
        partition_context = {"ticker": context.get("ticker")}
        query_params = context.get("query_params", {}).copy()
        next_url = None
        no_records_counter = 0  # counter in case API sends back next_url but no data is returned. Prevents infinite loop.
        while True:
            state = (
                self.get_context_state(partition_context)
                if self.replication_method == "INCREMENTAL"
                else {}
            )
            request_url = next_url or self.get_url(context)
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

            if isinstance(
                data, dict
            ):  # data from response should either be a list or dict
                records = data.get("results", data)
            elif isinstance(data, list):
                records = data
            else:
                raise ValueError(
                    f"Expecting response data to be type list or dict, got type {type(data)}"
                )

            if self.name == "market_holidays":
                break

            logging.info(
                f"Stream {self.name}: Received {len(records)} records in this batch. Next URL: {self.get_next_url(data)}"
            )

            if not records:
                logging.info(
                    f"No records returned for {self.name} in this batch. Checking if it's a persistent empty response."
                )
                no_records_counter += 1
                if no_records_counter >= 3:  # Break after 3 consecutive empty responses
                    logging.info(
                        f"Breaking pagination for {self.name} due to {no_records_counter} consecutive empty record batches."
                    )
                    break
            else:
                no_records_counter = 0

            if not isinstance(records, list):
                records = [records]

            for record in records:
                if self._clean_in_place:
                    self.clean_record(record, ticker=context.get("ticker"))
                else:
                    record = self.clean_record(record, ticker=context.get("ticker"))
                self._check_missing_fields(self.schema, record)
                yield record

            if self.replication_method == "INCREMENTAL":
                increment_state(
                    state,
                    replication_key=self.replication_key,
                    latest_record=record,
                    is_sorted=self.is_sorted,
                    check_sorted=self.check_sorted,
                )

                if "progress_markers" not in state:
                    state["progress_markers"] = {}

                logging.info(f"*** STATE UPDATED TO {state} ***")

            progress_markers = state.get("progress_markers", {}) if state else {}

            if isinstance(data, list):
                logging.info(f"Breaking out of loop for stream {self.name}")
                break

            next_url = self.get_next_url(data)
            query_params = self._update_query_params(query_params)

            context.update(
                {
                    "next_url": next_url,
                    "replication_key_value": progress_markers.get(
                        "replication_key_value"
                    ),
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

        if self._use_cached_tickers:
            ticker_records = self.tap.get_cached_tickers()
            for ticker_record in ticker_records:
                partition_context = {"ticker": ticker_record["ticker"]}
                state = self.get_context_state(partition_context)
                request_context = partition_context.copy()
                request_context["query_params"] = state.get(
                    "query_params", base_query_params.copy()
                )
                request_context["path_params"] = state.get(
                    "path_params", base_path_params.copy()
                )
                request_context["replication_key"] = state.get(
                    "replication_key", self.replication_key
                )
                request_context["replication_key_value"] = state.get(
                    "replication_key_value"
                )
                request_context["url"] = self.get_url(request_context)

                if loop_over_dates:
                    if not self.cfg_starting_timestamp:
                        raise ConfigValidationError(
                            f"Stream {self.name} is configured to loop over dates, but "
                            "'cfg_starting_timestamp' is not set."
                        )
                    start_date = datetime.strptime(
                        self.cfg_starting_timestamp.split("T")[0], "%Y-%m-%d"
                    ).date()
                    end_date = datetime.today().date()
                    if self._cfg_end_timestamp_key and self.cfg_end_timestamp_value:
                        try:
                            configured_end_date = datetime.strptime(
                                self.cfg_end_timestamp_value.split("T")[0], "%Y-%m-%d"
                            ).date()
                            end_date = min(configured_end_date, end_date)
                        except ValueError:
                            logging.warning(
                                f"Could not parse _cfg_end_timestamp '{self.cfg_end_timestamp_value}'. Using today's date."
                            )
                    current_timestamp = start_date
                    while current_timestamp <= end_date:
                        date_loop_context = partition_context.copy()
                        date_loop_context["query_params"] = (
                            self._update_params_with_timestamp(
                                current_timestamp,
                                base_query_params,
                                is_path_param=False,
                            )
                        )
                        date_loop_context["path_params"] = (
                            self._update_params_with_timestamp(
                                current_timestamp, base_path_params, is_path_param=True
                            )
                        )
                        date_loop_context["url"] = self.get_url(date_loop_context)
                        yield from self.paginate_records(date_loop_context)
                        current_timestamp += timedelta(days=1)
                else:
                    partition_context["url"] = self.get_url(partition_context)
                    yield from self.paginate_records(request_context)
        else:
            state = self.get_context_state(context)
            request_context = context.copy()
            request_context.update(
                {
                    "query_params": state.get("query_params", base_query_params.copy()),
                    "path_params": state.get("path_params", base_path_params.copy()),
                    "replication_key": state.get(
                        "replication_key", self.replication_key
                    ),
                    "replication_key_value": state.get("replication_key_value"),
                }
            )
            request_context["url"] = self.get_url(request_context)
            yield from self.paginate_records(request_context)

    def clean_record(self, record: dict, ticker: str | None = None) -> dict:
        return record
