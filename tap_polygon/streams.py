"""Stream type classes for tap-polygon."""

from __future__ import annotations

import typing as t
from importlib import resources
from singer_sdk import typing as th
import logging
from tap_polygon.client import PolygonRestStream
import pandas as pd
from dataclasses import asdict
import json


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

    def get_child_context(self, record, context):
        return {"ticker": record.get("ticker")}

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        logging.info(
            f"""\n self.config['stock_tickers']\n {self.config['stock_tickers']}"""
        )
        tickers = ""
        if "stock_tickers" in self.config.keys():
            tickers = self.config.get("stock_tickers")
            if isinstance(tickers, str):
                tickers = json.loads(tickers)
        if tickers == "":
            logging.info("Pulling all tickers...")
            for ticker in self.client.list_tickers(
                active="true",
                order="asc",
                limit="100",
                sort="ticker",
            ):
                ticker = asdict(ticker)
                yield ticker
        else:
            logging.info(f"Pulling tickers {tickers}...")
            for t in tickers:
                for ticker in self.client.list_tickers(
                    active="true",
                    order="asc",
                    limit="100",
                    sort="ticker",
                    ticker=t,
                ):
                    ticker = asdict(ticker)
                    yield ticker


class CachedTickerProvider:
    def __init__(self, stream: StockTickersStream):
        self.stream = stream
        self._tickers = None

    def get_tickers(self):
        if self._tickers is None:
            logging.info("Tickers have not been downloaded yet. Downloading now...")
            self._tickers = list(self.stream.get_records(context=None))
        return self._tickers


### Additional streams below that may use output from streaming all tickers ###


class TickerDetailsStream(PolygonRestStream):
    name = "ticker_details"
    primary_keys = ["ticker"]
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("homepage_url", th.StringType),
        th.Property("list_date", th.StringType),
        th.Property("market", th.StringType),
        th.Property("locale", th.StringType),
        th.Property("primary_exchange", th.StringType),
        th.Property("type", th.StringType),
        th.Property("sic_code", th.AnyType()),
        th.Property("sic_description", th.StringType),
        th.Property("total_employees", th.IntegerType),
        th.Property("logo", th.StringType),
        th.Property("phone_number", th.StringType),
        th.Property(
            "address",
            th.ObjectType(
                th.Property("address1", th.StringType),
                th.Property("city", th.StringType),
                th.Property("state", th.StringType),
                th.Property("postal_code", th.StringType),
            ),
        ),
        th.Property(
            "branding",
            th.ObjectType(
                th.Property("icon_url", th.StringType),
                th.Property("logo_url", th.StringType),
            ),
        ),
        th.Property("share_class_figi", th.StringType),
        th.Property("composite_figi", th.StringType),
        th.Property("share_class_shares_outstanding", th.NumberType),
        th.Property("weighted_shares_outstanding", th.NumberType),
        th.Property("round_lot", th.IntegerType),
        th.Property("active", th.BooleanType),
        th.Property("updated_utc", th.StringType),
    ).to_dict()

    def __init__(self, tap, ticker_provider: CachedTickerProvider):
        super().__init__(tap)
        self.ticker_provider = ticker_provider

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        ticker_records = self.ticker_provider.get_tickers()
        for record in ticker_records:
            ticker = record.get("ticker")
            ticker_details = self.client.get_ticker_details(ticker)
            yield asdict(ticker_details)
