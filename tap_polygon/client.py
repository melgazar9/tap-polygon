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


class PolygonAPIPaginator(BaseHATEOASPaginator):
    def get_next_url(self, response: requests.Response) -> t.Optional[str]:
        data = response.json()
        return data.get("next_url")


class PolygonRestStream(RESTStream):
    """Polygon rest API stream class."""

    def __init__(self, tap: TapBase):
        super().__init__(tap=tap)
        self.client = RESTClient(self.config["api_key"])

    @property
    def url_base(self) -> str:
        base_url = (
            self.config.get("base_url")
            if self.config.get("base_url") is not None
            else "https://api.polygon.io"
        )
        return base_url

    def get_query_params(self, override: dict = None) -> dict:
        base_params = self.config.get(self.name, {}).get("query_params", [{}])
        base_params["apiKey"] = self.config.get("api_key")
        if override:
            base_params.update(override)
        return base_params

    def get_new_paginator(self) -> PolygonAPIPaginator:
        return PolygonAPIPaginator()

    def paginate_records(
        self, url: str, params: dict[str, t.Any]
    ) -> t.Iterable[dict[str, t.Any]]:
        next_url = None
        while True:
            response = requests.get(next_url or url, params=params)
            response.raise_for_status()
            data = response.json()

            for record in data.get("results", []):
                yield record

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
        if not cfg_params:
            logging.warning(f"No config set for stream '{self.name}', using defaults.")
            self.path_params = {}
            self.query_params = {}
            return
        if not cfg_params:
            # No config set for this stream; safe fallback
            self.path_params = {}
            self.query_params = {}
            return

        if isinstance(cfg_params, dict):
            if "path_params" in cfg_params:
                self.path_params = cfg_params.get("path_params")
                assert len(self.path_params), ConfigValidationError(
                    f"Error validating path_params in config for {self.name}"
                )
            if "query_params" in cfg_params:
                self.query_params = cfg_params.get("query_params")
                assert len(self.query_params), ConfigValidationError(
                    f"Error validating query_params in config for {self.name}"
                )

        elif isinstance(cfg_params, list):
            for params in cfg_params:
                if not isinstance(params, dict):
                    raise ConfigValidationError(
                        f"Expected dict in '{self.name}', but got {type(params)}: {params}"
                    )

                if "path_params" in params:
                    self.path_params = params.get("path_params")
                    assert len(self.path_params), ConfigValidationError(
                        f"Error validating path_params in config for {self.name}"
                    )
                elif "query_params" in params:
                    self.query_params = params.get("query_params")
                    assert len(self.query_params), ConfigValidationError(
                        f"Error validating query_params in config for {self.name}"
                    )
        else:
            raise ConfigValidationError(
                f"Config key '{self.name}' must be a dict or list of dicts."
            )

        # always ensure API key is set for query_params, or set empty dict first
        if not hasattr(self, "query_params") or self.query_params is None:
            self.query_params = {}
        self.query_params["apiKey"] = self.config.get("api_key")
