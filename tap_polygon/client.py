"""REST client handling, including PolygonStream base class."""

from __future__ import annotations

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

from polygon import RESTClient
from singer_sdk.pagination import BaseHATEOASPaginator


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

    def get_new_paginator(self) -> PolygonAPIPaginator:
        return PolygonAPIPaginator()

    def get_url_params(
        self,
        context: t.Optional[t.Dict[str, t.Any]],
        next_page_token: t.Optional[t.Any],
    ) -> t.Union[dict[str, t.Any], str]:
        if next_page_token:
            return dict(parse_qsl(next_page_token.query))
        return {}
