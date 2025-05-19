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
from tap_polygon.utils import check_missing_fields


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
                check_missing_fields(ticker)
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
                    check_missing_fields(self.schema, ticker)
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
    schema = th.PropertiesList(
        th.Property("active", th.BooleanType, optional=True),
        th.Property(
            "address",
            th.ObjectType(
                th.Property("address1", th.StringType, optional=True),
                th.Property("address2", th.StringType, optional=True),
                th.Property("city", th.StringType, optional=True),
                th.Property("postal_code", th.StringType, optional=True),
                th.Property("state", th.StringType, optional=True),
            ),
            optional=True,
        ),
        th.Property(
            "branding",
            th.ObjectType(
                th.Property("icon_url", th.StringType, optional=True),
                th.Property("logo_url", th.StringType, optional=True),
            ),
            optional=True,
        ),
        th.Property("cik", th.StringType, optional=True),
        th.Property("composite_figi", th.StringType, optional=True),
        th.Property("currency_name", th.StringType, optional=True),
        th.Property("delisted_utc", th.StringType, optional=True),
        th.Property("description", th.StringType, optional=True),
        th.Property("homepage_url", th.StringType, optional=True),
        th.Property("list_date", th.StringType, optional=True),
        th.Property("locale", th.StringType, optional=True),  # enum: "us", "global"
        th.Property(
            "market", th.StringType, optional=True
        ),  # enum: "stocks", "crypto", "fx", "otc", "indices"
        th.Property("market_cap", th.NumberType, optional=True),
        th.Property("name", th.StringType, optional=True),
        th.Property("phone_number", th.StringType, optional=True),
        th.Property("primary_exchange", th.StringType, optional=True),
        th.Property("round_lot", th.NumberType, optional=True),
        th.Property("share_class_figi", th.StringType, optional=True),
        th.Property("share_class_shares_outstanding", th.NumberType, optional=True),
        th.Property("sic_code", th.StringType, optional=True),
        th.Property("sic_description", th.StringType, optional=True),
        th.Property("ticker", th.StringType, optional=True),
        th.Property("ticker_root", th.StringType, optional=True),
        th.Property("ticker_suffix", th.StringType, optional=True),
        th.Property("total_employees", th.NumberType, optional=True),
        th.Property("type", th.StringType, optional=True),
        th.Property("weighted_shares_outstanding", th.NumberType, optional=True),
        th.Property("base_currency_name", th.StringType, optional=True),
        th.Property("base_currency_symbol", th.StringType, optional=True),
        th.Property("currency_symbol", th.StringType, optional=True),
    ).to_dict()

    def __init__(self, tap, ticker_provider: CachedTickerProvider):
        super().__init__(tap)
        self.ticker_provider = ticker_provider

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        ticker_records = self.ticker_provider.get_tickers()
        for record in ticker_records:
            ticker = record.get("ticker")
            ticker_details = asdict(self.client.get_ticker_details(ticker))
            check_missing_fields(self.schema, ticker_details)
            yield ticker_details


class TickerTypesStream(PolygonRestStream):
    name = "ticker_types"
    schema = th.PropertiesList(
        th.Property(
            "asset_class", th.StringType
        ),  # enum: stocks, options, crypto, fx, indices
        th.Property("code", th.StringType),
        th.Property("description", th.StringType),
        th.Property("locale", th.StringType),  # enum: us, global
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        ticker_types = self.client.get_ticker_types()
        for ticker_type in ticker_types:
            tt = asdict(ticker_type)
            check_missing_fields(self.schema, tt)
            yield tt


class RelatedCompaniesStream(PolygonRestStream):
    name = "related_companies"
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property(
            "related_companies",
            th.ArrayType(th.ObjectType(th.Property("ticker", th.StringType))),
        ),
    ).to_dict()

    def __init__(self, tap, ticker_provider: CachedTickerProvider):
        super().__init__(tap)
        self.ticker_provider = ticker_provider

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        ticker_records = self.ticker_provider.get_tickers()
        for ticker_record in ticker_records:
            ticker = ticker_record["ticker"]
            related_companies = self.client.get_related_companies(ticker)
            related_list = [asdict(rc) for rc in related_companies]
            for rc in related_list:
                check_missing_fields(self.schema, rc)

            related_companies_output = {
                "ticker": ticker,
                "related_companies": related_list,
            }

            yield related_companies_output


class CustomBarsStream(PolygonRestStream):
    name = "custom_bars"
    replication_key = "timestamp"
    is_sorted = True
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("timestamp", th.NumberType),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("vwap", th.NumberType),
        th.Property("transactions", th.NumberType),
        th.Property("otc", th.BooleanType),
    ).to_dict()

    def __init__(self, tap, ticker_provider: CachedTickerProvider):
        super().__init__(tap)
        self.ticker_provider = ticker_provider

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        custom_bars_config = self.config.get("custom_bars")
        if not custom_bars_config or len(custom_bars_config) != 1:
            raise ValueError("Must supply exactly one params object in custom_bars.")

        base_params = custom_bars_config[0].get("params", {})
        ticker_records = self.ticker_provider.get_tickers()

        for ticker_record in ticker_records:
            ticker = ticker_record.get("ticker")
            params = base_params.copy()
            params["ticker"] = ticker

            if "from" in params:
                params["from_"] = params.pop("from")

            logging.info(
                f"Streaming {params.get('multiplier')} {params.get('timespan')} bars for ticker {ticker}..."
            )

            for bar in self.client.list_aggs(**params):
                record = asdict(bar)
                record["ticker"] = ticker
                check_missing_fields(self.schema, record)
                yield record
