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

        self.DEBUG = True

    @property
    def url_base(self) -> str:
        base_url = (
            self.config.get("base_url")
            if self.config.get("base_url") is not None
            else "https://api.polygon.io"
        )
        return base_url

    def get_new_paginator(self) -> PolygonAPIPaginator:
        return PolygonAPIPaginator()

    def paginate_records(
        self, url: str, query_params: dict[str, t.Any], **kwargs
    ) -> t.Iterable[dict[str, t.Any]]:
        query_params_to_log = {k: v for k, v in query_params.items() if k != "apiKey"}
        logging.info(
            f"Streaming {self.name} with query_params: {query_params_to_log}..."
        )
        next_url = None
        while True:
            response = requests.get(next_url or url, params=query_params)
            response.raise_for_status()
            data = response.json()

            records = data.get("results", data)

            if isinstance(records, list):
                for record in records:
                    if self.DEBUG:
                        if self.name != "stock_tickers":
                            logging.debug("DEBUG")
                    self.clean_record(record, **kwargs)
                    check_missing_fields(self.schema, record)
                    yield record
            else:
                record = records
                if self.DEBUG:
                    if self.name != "stock_tickers":
                        logging.debug("DEBUG")
                self.clean_record(record, **kwargs)
                check_missing_fields(self.schema, record)
                yield record

            next_url = data.get("next_url")
            if not next_url:
                break

    def get_url(self, **kwargs):
        raise NotImplementedError(
            "Method get_url_for_ticker must be overridden in the stream class."
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
