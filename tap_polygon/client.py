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


class PolygonRestStream(RESTStream):
    """Polygon rest API stream class."""

    def __init__(self, tap: TapBase):
        super().__init__(tap=tap)
        self.client = RESTClient(self.config["api_key"])

    @property
    def url_base(self) -> str:
        return "https://api.polygon.io"
