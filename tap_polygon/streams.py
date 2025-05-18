"""Stream type classes for tap-polygon."""

from __future__ import annotations

import typing as t
from importlib import resources
from singer_sdk import typing as th

from tap_polygon.client import PolygonRestStream
import pandas as pd

class StockTickersStream(PolygonRestStream):
    """Fetch all stock tickers from Polygon."""

    name = "stock_tickers"
    replication_key = "ticker"
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

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        tickers = []
        for t in self.client.list_tickers(
                active="true",
                order="asc",
                limit="100",
                sort="ticker",
        ):
            tickers.append(t)

        df = pd.DataFrame(tickers)

        for row in df.to_dict(orient="records"):
            yield row