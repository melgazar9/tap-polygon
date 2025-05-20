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
        self.DEBUG = False

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
        query_params_to_log = {k: v for k, v in query_params.items() if k != 'apiKey'}
        logging.info(
            f"Streaming {self.name} with query_params: {query_params_to_log}..."
        )
        next_url = None
        while True:
            response = requests.get(next_url or url, params=query_params)
            response.raise_for_status()
            data = response.json()

            if "results" in data and isinstance(data["results"], list):
                for record in data["results"]:
                    if self.DEBUG:
                        if self.name != "stock_tickers":
                            logging.info("Breakpoint")
                    if self.name == "custom_bars":
                        self.clean_custom_bars_record(record, ticker=kwargs.get("ticker"))
                    check_missing_fields(self.schema, record)
                    yield record
            elif "results" in data and isinstance(data["results"], dict):
                check_missing_fields(self.schema, data["results"])
                yield data["results"]
            else:
                check_missing_fields(self.schema, data)
                yield data

            next_url = data.get("next_url")
            if not next_url:
                break

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

    @staticmethod
    def clean_custom_bars_record(record, ticker):
        rename_map = {
            "t": "timestamp",
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "v": "volume",
            "vw": "vwap",
            "n": "transactions",
        }

        for old_key, new_key in rename_map.items():
            if old_key in record:
                record[new_key] = record.pop(old_key)

        record["ticker"] = ticker
        record["otc"] = record.get("otc", None)