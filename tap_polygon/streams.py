"""Stream type classes for tap-polygon."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import typing as t
from dataclasses import asdict
from datetime import datetime

import requests
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_polygon.client import PolygonRestStream


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
        th.Property("replication_key", th.StringType),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/reference/tickers"

    def get_ticker_list(self) -> list[str] | None:
        stock_tickers_cfg = self.config.get("stock_tickers", {})
        tickers = stock_tickers_cfg.get("tickers") if stock_tickers_cfg else None

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
        base_url = self.get_url()
        query_params = self.query_params.copy()
        if not ticker_list:
            logging.info("Pulling all tickers...")
            yield from self.paginate_records(context)
        else:
            logging.info(f"Pulling specific tickers: {ticker_list}")
            for ticker in ticker_list:
                query_params.update({"ticker": ticker})
                context["url"] = base_url
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


### Additional streams below that may use output from streaming all tickers ###


class TickerDetailsStream(PolygonRestStream):
    name = "ticker_details"
    schema = th.PropertiesList(
        th.Property("active", th.BooleanType),
        th.Property(
            "address",
            th.ObjectType(
                th.Property("address1", th.StringType),
                th.Property("address2", th.StringType),
                th.Property("city", th.StringType),
                th.Property("postal_code", th.StringType),
                th.Property("state", th.StringType),
            ),
            optional=True,
        ),
        th.Property(
            "branding",
            th.ObjectType(
                th.Property("icon_url", th.StringType),
                th.Property("logo_url", th.StringType),
            ),
            optional=True,
        ),
        th.Property("cik", th.StringType),
        th.Property("composite_figi", th.StringType),
        th.Property("currency_name", th.StringType),
        th.Property("delisted_utc", th.StringType),
        th.Property("description", th.StringType),
        th.Property("homepage_url", th.StringType),
        th.Property("list_date", th.StringType),
        th.Property("locale", th.StringType),  # enum: "us", "global"
        th.Property(
            "market", th.StringType
        ),  # enum: "stocks", "crypto", "fx", "otc", "indices"
        th.Property("market_cap", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("phone_number", th.StringType),
        th.Property("primary_exchange", th.StringType),
        th.Property("round_lot", th.NumberType),
        th.Property("share_class_figi", th.StringType),
        th.Property("share_class_shares_outstanding", th.NumberType),
        th.Property("sic_code", th.StringType),
        th.Property("sic_description", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("ticker_root", th.StringType),
        th.Property("ticker_suffix", th.StringType),
        th.Property("total_employees", th.NumberType),
        th.Property("type", th.StringType),
        th.Property("weighted_shares_outstanding", th.NumberType),
        th.Property("base_currency_name", th.StringType),
        th.Property("base_currency_symbol", th.StringType),
        th.Property("currency_symbol", th.StringType),
        th.Property("cusip", th.StringType),
        th.Property("last_updated_utc", th.DateTimeType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

        self._use_cached_tickers = True

    def get_url(self, context: Context):
        ticker = context.get("ticker")
        return f"{self.url_base}/v3/reference/tickers/{ticker}"


class TickerTypesStream(PolygonRestStream):
    """TickerTypesStream - does not require pagination so use Polygon's RESTClient() to fetch the data."""

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
            yield tt


class RelatedCompaniesStream(PolygonRestStream):
    name = "related_companies"
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("related_company", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

        self._use_cached_tickers = True

    def get_url(self, context: Context):
        ticker = context.get("ticker")
        return f"{self.url_base}/v1/related-companies/{ticker}"

    @staticmethod
    def clean_record(record: dict, ticker: str) -> None:
        record["related_company"] = record["ticker"]
        record["ticker"] = ticker


class CustomBarsStream(PolygonRestStream):
    name = "custom_bars"
    replication_key = "timestamp"
    is_sorted = False
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

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

        self._use_cached_tickers = True

    def build_path_params(self, path_params: dict) -> str:
        keys = ["multiplier", "timespan", "from", "to"]
        return "/" + "/".join(str(path_params[k]) for k in keys if k in path_params)

    def get_url(self, context: Context):
        ticker = context.get("ticker")
        context.get("path_params")
        return f"{self.url_base}/v2/aggs/ticker/{ticker}/range{self.build_path_params(self.path_params)}"

    @staticmethod
    def clean_record(record, ticker) -> None:
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


class DailyMarketSummaryStream(PolygonRestStream):
    name = "daily_market_summary"
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

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

        self._use_cached_tickers = False

    def get_url(self, context: Context):
        date = context.get("path_params").get("date")
        if date is None:
            date = datetime.today().date().isoformat()
        return f"{self.url_base}/v2/aggs/grouped/locale/us/market/stocks/{date}"

    @staticmethod
    def clean_record(record, ticker=None):
        mapping = {
            "T": "ticker",
            "t": "timestamp",
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "v": "volume",
            "vw": "vwap",
            "n": "transactions",
            "otc": "otc",
        }
        for old_key, new_key in mapping.items():
            if old_key in record:
                record[new_key] = record.pop(old_key)


class DailyTickerSummaryStream(PolygonRestStream):
    name = "daily_ticker_summary"
    schema = th.PropertiesList(
        th.Property("symbol", th.StringType),
        th.Property("from", th.StringType),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("otc", th.BooleanType),
        th.Property("pre_market", th.NumberType),
        th.Property("after_hours", th.NumberType),
        th.Property("status", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

        self._use_cached_tickers = True

    def get_url(self, context: Context):
        date = self.path_params.get("date")
        ticker = context.get("ticker")
        if date is None:
            date = datetime.today().date().isoformat()
        return f"{self.url_base}/v1/open-close/{ticker}/{date}"

    @staticmethod
    def clean_record(record, ticker=None):
        record["pre_market"] = record.pop("preMarket")
        record["after_hours"] = record.pop("afterHours")


class PreviousDayBarSummaryStream(PolygonRestStream):
    """Retrieve the previous trading day's OHLCV data for a specified stock ticker.
    Not really useful given we have the other streams."""

    name = "previous_day_bar"
    pass


class TickerSnapshotStream(PolygonRestStream):
    """Retrieve the most recent market data snapshot for a single ticker.
    Not really useful given we have the other streams."""

    name = "ticker_snapshot"
    pass


class FullMarketSnapshotStream(PolygonRestStream):
    """
    Retrieve a comprehensive snapshot of the entire U.S. stock market, covering over 10,000+ actively traded
    tickers in a single response. Not really useful given we have the other streams.
    """

    name = "full_market_snapshot"
    pass


class UnifiedSnapshotStream(PolygonRestStream):
    """
    Retrieve unified snapshots of market data for multiple asset classes including stocks, options, forex,
    and cryptocurrencies in a single request. Not really useful given we have the other streams.
    """

    name = "unified_snapshot"
    pass


class TopMarketMoversStream(PolygonRestStream):
    """
    Retrieve snapshot data highlighting the top 20 gainers or losers in the U.S. stock market.
    Gainers are stocks with the largest percentage increase since the previous day’s close, and losers are those
    with the largest percentage decrease. Only tickers with a minimum trading volume of 10,000 are included.
    Snapshot data is cleared daily at 3:30 AM EST and begins repopulating as exchanges report new information,
    typically starting around 4:00 AM EST.
    """

    name = "top_market_movers"
    schema = th.PropertiesList(
        th.Property("day", th.AnyType()),
        th.Property("last_quote", th.AnyType()),
        th.Property("last_trade", th.AnyType()),
        th.Property("min", th.AnyType()),
        th.Property("prev_day", th.AnyType()),
        th.Property("ticker", th.StringType),
        th.Property("todays_change", th.NumberType),
        th.Property("todays_change_percent", th.NumberType),
        th.Property("updated", th.NumberType),
        th.Property("fair_market_value", th.BooleanType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

        self._use_cached_tickers = False

    def get_url(self, context: Context):
        direction = context.get("direction")
        return f"{self.url_base}/v2/snapshot/locale/us/markets/stocks/{direction}"

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        if (
            self.path_params.get("direction") is None
            or self.path_params.get("direction") == ""
            or self.path_params.get("direction").lower() == "both"
            or "direction" not in self.path_params
        ):
            for direction in ["gainers", "losers"]:
                url = self.get_url(context={"direction": direction})
                data = requests.get(url, params=self.query_params)
                for record in data.json().get("tickers"):
                    record["direction"] = direction
                    self.clean_record(record)
                    yield record
        else:
            direction = self.path_params.get("direction")
            url = self.get_url(context)
            data = requests.get(url, params=self.query_params)
            for record in data.json().get("tickers"):
                record["direction"] = direction
                self.clean_record(record)
                yield record

    @staticmethod
    def clean_record(record: dict, ticker=None) -> dict:
        def to_snake_case(s):
            return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()

        def clean_keys(d):
            keys = list(d.keys())
            for key in keys:
                value = d.pop(key)
                new_key = to_snake_case(key)
                if isinstance(value, dict):
                    clean_keys(value)
                d[new_key] = value

        clean_keys(record)


class TradeStream(PolygonRestStream):
    """
    Retrieve comprehensive, tick-level trade data for a specified stock ticker within a defined time range.
    Each record includes price, size, exchange, trade conditions, and precise timestamp information.
    This granular data is foundational for constructing aggregated bars and performing in-depth analyses, as it captures
    every eligible trade that contributes to calculations of open, high, low, and close (OHLC) values.
    By leveraging these trades, users can refine their understanding of intraday price movements, test and optimize
    algorithmic strategies, and ensure compliance by maintaining an auditable record of market activity.

    Use Cases: Intraday analysis, algorithmic trading, market microstructure research, data integrity and compliance.

    NOTE: This stream cannot stream multiple tickers at once, so if we want to stream or fetch trades for multiple
    tickers you need to send multiple parallel or sequential API requests (one for each ticker).
    Data is delayed 15 minutes for developer plan. For real-time data top the Advanced Subscription is needed.
    """

    name = "trades"
    replication_key = "replication_key"
    replication_method = "INCREMENTAL"
    is_sorted = False
    is_timestamp_replication_key = False
    schema = th.PropertiesList(
        th.Property("conditions", th.ArrayType(th.AnyType())),
        th.Property("correction", th.AnyType()),
        th.Property("exchange", th.NumberType),
        th.Property("id", th.StringType),
        th.Property("participant_timestamp", th.IntegerType),
        th.Property("price", th.NumberType),
        th.Property("sequence_number", th.IntegerType),
        th.Property("sip_timestamp", th.IntegerType),
        th.Property("size", th.NumberType),
        th.Property("tape", th.IntegerType),
        th.Property("trf_id", th.IntegerType),
        th.Property("trf_timestamp", th.NumberType),
        th.Property("replication_key", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

        self._use_cached_tickers = True

    @property
    def partitions(self) -> list[dict]:
        return [{"ticker": t["ticker"]} for t in self.tap.get_cached_tickers()]

    def get_url(self, context: Context):
        ticker = context.get("ticker")
        return f"{self.url_base}/v3/trades/{ticker}"

    @staticmethod
    def clean_record(record: dict, ticker=None) -> dict:
        surrogate_key = ""
        if "exchange" in record:
            surrogate_key = f"{surrogate_key}_{record['exchange']}"
        if "trf_id" in record:
            surrogate_key = f"{surrogate_key}_{record['trf_id']}"
        if "id" in record:
            surrogate_key = f"{surrogate_key}_{record['id']}"
        if "participant_timestamp" in record:
            surrogate_key = f"{surrogate_key}_{record['participant_timestamp']}"
        record["replication_key"] = hashlib.sha256(surrogate_key.encode()).hexdigest()


class QuoteStream(TradeStream):
    name = "quotes"
    schema = th.PropertiesList(
        th.Property("ask_exchange", th.IntegerType),
        th.Property("ask_price", th.NumberType),
        th.Property("ask_size", th.NumberType),
        th.Property("bid_exchange", th.IntegerType),
        th.Property("bid_price", th.NumberType),
        th.Property("bid_size", th.NumberType),
        th.Property("conditions", th.ArrayType(th.IntegerType)),
        th.Property("indicators", th.ArrayType(th.IntegerType)),
        th.Property("participant_timestamp", th.IntegerType),
        th.Property("sequence_number", th.IntegerType),
        th.Property("sip_timestamp", th.IntegerType),
        th.Property("tape", th.IntegerType),
        th.Property("trf_timestamp", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get("ticker")
        return f"{self.url_base}/v3/quotes/{ticker}"


class LastQuoteStream(QuoteStream):
    name = "last_quote"
    schema = th.PropertiesList(
        th.Property("P", th.NumberType),
        th.Property("S", th.IntegerType),
        th.Property("T", th.StringType),
        th.Property("X", th.IntegerType),
        th.Property("c", th.ArrayType(th.IntegerType)),
        th.Property("f", th.IntegerType),
        th.Property("i", th.ArrayType(th.IntegerType)),
        th.Property("p", th.NumberType),
        th.Property("q", th.IntegerType),
        th.Property("s", th.IntegerType),
        th.Property("t", th.IntegerType),
        th.Property("x", th.IntegerType),
        th.Property("y", th.IntegerType),
        th.Property("z", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context):
        ticker = context.get("ticker")
        return f"{self.url_base}/v2/last/nbbo/{ticker}"


class IndicatorStream(PolygonRestStream):
    schema = th.PropertiesList(
        th.Property(
            "underlying",
            th.ObjectType(
                th.Property(
                    "aggregates",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("T", th.StringType),
                            th.Property("v", th.NumberType),
                            th.Property("vw", th.NumberType),
                            th.Property("o", th.NumberType),
                            th.Property("c", th.NumberType),
                            th.Property("h", th.NumberType),
                            th.Property("l", th.NumberType),
                            th.Property("t", th.IntegerType),
                            th.Property("n", th.IntegerType),
                        )
                    ),
                ),
                th.Property("url", th.StringType),
            ),
        ),
        th.Property(
            "values",
            th.ArrayType(
                th.ObjectType(
                    th.Property("timestamp", th.IntegerType),
                    th.Property("value", th.NumberType),
                )
            ),
        ),
        th.Property("series_window_timespan", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap

        self._use_cached_tickers = True
        self._clean_in_place = True

    def base_indicator_url(self):
        return f"{self.url_base}/v1/indicators"

    def get_url(self, context: Context):
        ticker = context.get("ticker")
        return f"{self.base_indicator_url()}/{self.name}/{ticker}"

    def clean_record(self, record: dict, ticker=None) -> dict:
        max_agg_ts = max(
            item["t"] for item in record.get("underlying").get("aggregates")
        )
        agg_window = self.query_params.get("window")
        agg_timespan = self.query_params.get("timespan")
        agg_series_type = self.query_params.get("series_type")
        record["series_window_timespan"] = (
            f"{agg_series_type}_{agg_timespan}_{agg_window}"
        )
        record["max_underlying_timestamp"] = max_agg_ts
        record["max_indicator_timestamp"] = max_agg_ts


class SmaStream(IndicatorStream):
    name = "sma"


class EmaStream(IndicatorStream):
    name = "ema"


class MACDStream(IndicatorStream):
    name = "macd"


class RSIStream(IndicatorStream):
    name = "rsi"


class ExchangesStream(PolygonRestStream):
    """Fetch Exchanges"""

    name = "exchanges"
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("type", th.StringType),
        th.Property("asset_class", th.StringType),
        th.Property("locale", th.StringType),
        th.Property("name", th.StringType),
        th.Property("acronym", th.StringType),
        th.Property("mic", th.StringType),
        th.Property("operating_mic", th.StringType),
        th.Property("participant_id", th.StringType),
        th.Property("url", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap
        self._use_cached_tickers = False

    def get_url(self, context: Context = None):
        return f"{self.url_base}/v3/reference/exchanges"


class MarketHolidaysStream(PolygonRestStream):
    """Market Holidays Stream (forward-looking)"""

    name = "market_holidays"
    schema = th.PropertiesList(
        th.Property("close", th.StringType),
        th.Property("date", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("name", th.StringType),
        th.Property("open", th.StringType),
        th.Property("status", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap
        self._use_cached_tickers = False

    def get_url(self, context: Context = None):
        return f"{self.url_base}/v1/marketstatus/upcoming"


class MarketStatusStream(PolygonRestStream):
    """Market Status Stream"""

    name = "market_status"

    schema = th.PropertiesList(
        th.Property("afterHours", th.BooleanType),
        th.Property(
            "currencies",
            th.ObjectType(
                th.Property("crypto", th.StringType),
                th.Property("fx", th.StringType),
            ),
        ),
        th.Property("earlyHours", th.BooleanType),
        th.Property(
            "exchanges",
            th.ObjectType(
                th.Property("nasdaq", th.StringType),
                th.Property("nyse", th.StringType),
                th.Property("otc", th.StringType),
            ),
        ),
        th.Property(
            "indicesGroups",
            th.ObjectType(
                th.Property("cccy", th.StringType),
                th.Property("cgi", th.StringType),
                th.Property("dow_jones", th.StringType),
                th.Property("ftse_russell", th.StringType),
                th.Property("msci", th.StringType),
                th.Property("mstar", th.StringType),
                th.Property("mstarc", th.StringType),
                th.Property("nasdaq", th.StringType),
                th.Property("s_and_p", th.StringType),
                th.Property("societe_generale", th.StringType),
            ),
        ),
        th.Property("market", th.StringType),
        th.Property("serverTime", th.StringType),
    ).to_dict()

    def get_records(
        self, context: dict[str, t.Any] | None
    ) -> t.Iterable[dict[str, t.Any]]:
        market_status = self.client.get_market_status()
        yield asdict(market_status)


class ConditionCodesStream(PolygonRestStream):
    """Condition Codes Stream"""

    name = "condition_codes"

    schema = th.PropertiesList(
        th.Property("abbreviation", th.StringType),
        th.Property(
            "asset_class", th.StringType, enum=["stocks", "options", "crypto", "fx"]
        ),
        th.Property("data_types", th.ArrayType(th.StringType)),
        th.Property("description", th.StringType),
        th.Property("exchange", th.IntegerType),
        th.Property("id", th.IntegerType),
        th.Property("legacy", th.BooleanType),
        th.Property("name", th.StringType),
        th.Property(
            "sip_mapping",
            th.ObjectType(
                th.Property("CTA", th.StringType),
                th.Property("OPRA", th.StringType),
                th.Property("UTP", th.StringType),
            ),
        ),
        th.Property(
            "type",
            th.StringType,
            enum=[
                "sale_condition",
                "quote_condition",
                "sip_generated_flag",
                "financial_status_indicator",
                "short_sale_restriction_indicator",
                "settlement_condition",
                "market_condition",
                "trade_thru_exempt",
                "regular",
                "buy_or_sell_side",
            ],
        ),
        th.Property(
            "update_rules",
            th.ObjectType(
                th.Property(
                    "consolidated",
                    th.ObjectType(
                        th.Property("updates_high_low", th.BooleanType),
                        th.Property("updates_open_close", th.BooleanType),
                        th.Property("updates_volume", th.BooleanType),
                    ),
                ),
                th.Property(
                    "market_center",
                    th.ObjectType(
                        th.Property("updates_high_low", th.BooleanType),
                        th.Property("updates_open_close", th.BooleanType),
                        th.Property("updates_volume", th.BooleanType),
                    ),
                ),
            ),
        ),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap
        self._use_cached_tickers = False

    def get_url(self, context: Context = None):
        return f"{self.url_base}/v3/reference/conditions"


class IPOsStream(PolygonRestStream):
    """IPOs Stream"""

    name = "ipos"
    schema = th.PropertiesList(
        th.Property("announced_date", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("final_issue_price", th.NumberType),
        th.Property("highest_offer_price", th.NumberType),
        th.Property(
            "ipo_status",
            th.StringType,
            enum=[
                "direct_listing_process",
                "history",
                "new",
                "pending",
                "postponed",
                "rumor",
                "withdrawn",
            ],
        ),
        th.Property("isin", th.StringType),
        th.Property("issuer_name", th.StringType),
        th.Property("last_updated", th.StringType),
        th.Property("listing_date", th.StringType),
        th.Property("lot_size", th.NumberType),
        th.Property("lowest_offer_price", th.NumberType),
        th.Property("max_shares_offered", th.NumberType),
        th.Property("min_shares_offered", th.NumberType),
        th.Property("primary_exchange", th.StringType),
        th.Property("security_description", th.StringType),
        th.Property("security_type", th.StringType),
        th.Property("shares_outstanding", th.NumberType),
        th.Property("ticker", th.StringType),
        th.Property("total_offer_size", th.NumberType),
        th.Property("us_code", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap
        self._use_cached_tickers = False

    def get_url(self, context: Context = None):
        return f"{self.url_base}/vX/reference/ipos"

    def clean_record(self, record: dict, ticker=None) -> dict:
        record["ipo_status"] = record["ipo_status"].lower()


class SplitsStream(PolygonRestStream):
    """Splits Stream"""

    name = "splits"
    schema = th.PropertiesList(
        th.Property("execution_date", th.StringType),
        th.Property("id", th.StringType),
        th.Property("split_from", th.NumberType),
        th.Property("split_to", th.NumberType),
        th.Property("ticker", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap
        self._use_cached_tickers = False

    def get_url(self, context: Context = None):
        return f"{self.url_base}/v3/reference/splits"


class DividendsStream(PolygonRestStream):
    """Dividends Stream"""

    name = "dividends"
    schema = th.PropertiesList(
        th.Property("cash_amount", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("declaration_date", th.StringType),
        th.Property("dividend_type", th.StringType, enum=["CD", "SC", "LT", "ST"]),
        th.Property("ex_dividend_date", th.StringType),
        th.Property("frequency", th.IntegerType),
        th.Property("id", th.StringType),
        th.Property("pay_date", th.StringType),
        th.Property("record_date", th.StringType),
        th.Property("ticker", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap
        self._use_cached_tickers = False

    def get_url(self, context: Context = None):
        return f"{self.url_base}/v3/reference/dividends"


class TickerEventsStream(PolygonRestStream):
    """Ticker Events Stream"""

    name = "ticker_events"
    schema = th.PropertiesList(
        th.Property(
            "events",
            th.ArrayType(
                th.ObjectType(
                    th.Property("date", th.StringType),
                    th.Property(
                        "ticker_change",
                        th.ObjectType(th.Property("ticker", th.StringType)),
                    ),
                    th.Property("type", th.StringType),
                )
            ),
        ),
        th.Property("name", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("composite_figi", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap
        self._use_cached_tickers = True

    def get_url(self, context: Context):
        ticker = context.get("ticker")
        return f"{self.url_base}/vX/reference/tickers/{ticker}/events"


def _financial_metric_property(name: str):
    return th.Property(
        name,
        th.ObjectType(
            th.Property("label", th.StringType),
            th.Property("order", th.IntegerType),
            th.Property("unit", th.StringType),
            th.Property("value", th.NumberType),
        ),
    )


class FinancialsStream(PolygonRestStream):
    """Financials Stream"""

    name = "financials"
    schema = th.PropertiesList(
        th.Property("acceptance_datetime", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("end_date", th.StringType),
        th.Property("filing_date", th.StringType),
        th.Property(
            "financials",
            th.ObjectType(
                th.Property(
                    "comprehensive_income",
                    th.ObjectType(
                        _financial_metric_property("comprehensive_income_loss"),
                        _financial_metric_property("other_comprehensive_income_loss"),
                        _financial_metric_property(
                            "comprehensive_income_loss_attributable_to_parent"
                        ),
                        _financial_metric_property(
                            "comprehensive_income_loss_attributable_to_noncontrolling_interest"
                        ),
                        _financial_metric_property(
                            "other_comprehensive_income_loss_attributable_to_parent"
                        ),
                        _financial_metric_property(
                            "other_comprehensive_income_loss_attributable_to_noncontrolling_interest"
                        ),
                        additional_properties=True,
                    ),
                ),
                th.Property(
                    "income_statement",
                    th.ObjectType(
                        _financial_metric_property("revenues"),
                        _financial_metric_property("cost_of_revenue"),
                        _financial_metric_property("gross_profit"),
                        _financial_metric_property("operating_expenses"),
                        _financial_metric_property("operating_income_loss"),
                        _financial_metric_property(
                            "income_loss_from_continuing_operations_before_tax"
                        ),
                        _financial_metric_property(
                            "income_loss_from_continuing_operations_after_tax"
                        ),
                        _financial_metric_property("net_income_loss"),
                        _financial_metric_property(
                            "net_income_loss_attributable_to_parent"
                        ),
                        _financial_metric_property(
                            "net_income_loss_attributable_to_noncontrolling_interest"
                        ),
                        _financial_metric_property(
                            "net_income_loss_available_to_common_stockholders_basic"
                        ),
                        _financial_metric_property("basic_earnings_per_share"),
                        _financial_metric_property("diluted_earnings_per_share"),
                        _financial_metric_property("basic_average_shares"),
                        _financial_metric_property("diluted_average_shares"),
                        _financial_metric_property(
                            "preferred_stock_dividends_and_other_adjustments"
                        ),
                        _financial_metric_property(
                            "participating_securities_distributed_and_undistributed_earnings_loss_basic"
                        ),
                        _financial_metric_property("benefits_costs_expenses"),
                        _financial_metric_property("depreciation_and_amortization"),
                        _financial_metric_property(
                            "income_tax_expense_benefit_current"
                        ),
                        _financial_metric_property("research_and_development"),
                        _financial_metric_property("costs_and_expenses"),
                        _financial_metric_property(
                            "income_loss_from_equity_method_investments"
                        ),
                        _financial_metric_property(
                            "income_tax_expense_benefit_deferred"
                        ),
                        _financial_metric_property("income_tax_expense_benefit"),
                        _financial_metric_property("other_operating_expenses"),
                        _financial_metric_property("interest_expense_operating"),
                        _financial_metric_property(
                            "income_loss_before_equity_method_investments"
                        ),
                        _financial_metric_property(
                            "selling_general_and_administrative_expenses"
                        ),
                        _financial_metric_property(
                            "income_loss_from_discontinued_operations_net_of_tax"
                        ),
                        _financial_metric_property("nonoperating_income_loss"),
                        _financial_metric_property(
                            "provision_for_loan_lease_and_other_losses"
                        ),
                        _financial_metric_property(
                            "interest_income_expense_after_provision_for_losses"
                        ),
                        _financial_metric_property(
                            "interest_income_expense_operating_net"
                        ),
                        _financial_metric_property("noninterest_income"),
                        _financial_metric_property(
                            "undistributed_earnings_loss_allocated_to_participating_securities_basic"
                        ),
                        _financial_metric_property(
                            "net_income_loss_attributable_to_nonredeemable_noncontrolling_interest"
                        ),
                        _financial_metric_property("interest_and_debt_expense"),
                        _financial_metric_property("other_operating_income_expenses"),
                        _financial_metric_property(
                            "net_income_loss_attributable_to_redeemable_noncontrolling_interest"
                        ),
                        _financial_metric_property(
                            "income_loss_from_discontinued_operations_net_of_tax_during_phase_out"
                        ),
                        _financial_metric_property(
                            "income_loss_from_discontinued_operations_net_of_tax_gain_loss_on_disposal"
                        ),
                        _financial_metric_property("common_stock_dividends"),
                        _financial_metric_property(
                            "interest_and_dividend_income_operating"
                        ),
                        _financial_metric_property("noninterest_expense"),
                        _financial_metric_property(
                            "income_loss_from_discontinued_operations_net_of_tax_provision_for_gain_loss_on_disposal"
                        ),
                        _financial_metric_property(
                            "income_loss_from_discontinued_operations_net_of_tax_adjustment_to_prior_year_gain_loss_on_disposal"
                        ),
                    ),
                ),
                th.Property(
                    "balance_sheet",
                    th.ObjectType(
                        _financial_metric_property("assets"),
                        _financial_metric_property("current_assets"),
                        _financial_metric_property("noncurrent_assets"),
                        _financial_metric_property("liabilities"),
                        _financial_metric_property("current_liabilities"),
                        _financial_metric_property("noncurrent_liabilities"),
                        _financial_metric_property("equity"),
                        _financial_metric_property("equity_attributable_to_parent"),
                        _financial_metric_property(
                            "equity_attributable_to_noncontrolling_interest"
                        ),
                        _financial_metric_property("liabilities_and_equity"),
                        _financial_metric_property("other_current_liabilities"),
                        _financial_metric_property("wages"),
                        _financial_metric_property("intangible_assets"),
                        _financial_metric_property("prepaid_expenses"),
                        _financial_metric_property("noncurrent_prepaid_expenses"),
                        _financial_metric_property("fixed_assets"),
                        _financial_metric_property("other_noncurrent_assets"),
                        _financial_metric_property("other_current_assets"),
                        _financial_metric_property("accounts_payable"),
                        _financial_metric_property("long_term_debt"),
                        _financial_metric_property("inventory"),
                        _financial_metric_property("cash"),
                        _financial_metric_property("commitments_and_contingencies"),
                        _financial_metric_property("temporary_equity"),
                        _financial_metric_property(
                            "temporary_equity_attributable_to_parent"
                        ),
                        _financial_metric_property("accounts_receivable"),
                        _financial_metric_property(
                            "redeemable_noncontrolling_interest"
                        ),
                        _financial_metric_property(
                            "redeemable_noncontrolling_interest_preferred"
                        ),
                        _financial_metric_property("interest_payable"),
                        _financial_metric_property(
                            "redeemable_noncontrolling_interest_other"
                        ),
                        _financial_metric_property(
                            "redeemable_noncontrolling_interest_common"
                        ),
                        _financial_metric_property("long_term_investments"),
                        _financial_metric_property("other_noncurrent_liabilities"),
                    ),
                ),
                th.Property(
                    "cash_flow_statement",
                    th.ObjectType(
                        _financial_metric_property("net_cash_flow"),
                        _financial_metric_property("net_cash_flow_continuing"),
                        _financial_metric_property(
                            "net_cash_flow_from_operating_activities"
                        ),
                        _financial_metric_property(
                            "net_cash_flow_from_operating_activities_continuing"
                        ),
                        _financial_metric_property("exchange_gains_losses"),
                        _financial_metric_property(
                            "net_cash_flow_from_financing_activities"
                        ),
                        _financial_metric_property(
                            "net_cash_flow_from_operating_activities_discontinued"
                        ),
                        _financial_metric_property(
                            "net_cash_flow_from_investing_activities_discontinued"
                        ),
                        _financial_metric_property(
                            "net_cash_flow_from_investing_activities"
                        ),
                        _financial_metric_property("net_cash_flow_discontinued"),
                        _financial_metric_property(
                            "net_cash_flow_from_investing_activities_continuing"
                        ),
                        _financial_metric_property(
                            "net_cash_flow_from_financing_activities_continuing"
                        ),
                        _financial_metric_property(
                            "net_cash_flow_from_financing_activities_discontinued"
                        ),
                    ),
                ),
            ),
        ),
        th.Property("fiscal_period", th.StringType),
        th.Property("fiscal_year", th.StringType),
        th.Property("sic", th.StringType),
        th.Property("source_filing_file_url", th.StringType),
        th.Property("source_filing_url", th.StringType),
        th.Property("start_date", th.StringType),
        th.Property("tickers", th.ArrayType(th.StringType)),
        th.Property("timeframe", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap
        self._use_cached_tickers = False

    def get_url(self, context: Context = None):
        return f"{self.url_base}/vX/reference/financials"


class ShortInterestStream(PolygonRestStream):
    """Short Interest Stream"""

    name = "short_interest"
    schema = th.PropertiesList(
        th.Property("avg_daily_volume", th.IntegerType),
        th.Property("days_to_cover", th.NumberType),
        th.Property("settlement_date", th.StringType),
        th.Property("short_interest", th.IntegerType),
        th.Property("ticker", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap
        self._use_cached_tickers = False

    def get_url(self, context: Context = None):
        return f"{self.url_base}/stocks/vX/short-interest"


class ShortVolumeStream(PolygonRestStream):
    """Short Volume Stream"""

    name = "short_volume"
    schema = th.PropertiesList(
        th.Property("adf_short_volume", th.IntegerType),
        th.Property("adf_short_volume_exempt", th.IntegerType),
        th.Property("date", th.StringType),
        th.Property("exempt_volume", th.IntegerType),
        th.Property("nasdaq_carteret_short_volume", th.IntegerType),
        th.Property("nasdaq_carteret_short_volume_exempt", th.IntegerType),
        th.Property("nasdaq_chicago_short_volume", th.IntegerType),
        th.Property("nasdaq_chicago_short_volume_exempt", th.IntegerType),
        th.Property("non_exempt_volume", th.IntegerType),
        th.Property("nyse_short_volume", th.IntegerType),
        th.Property("nyse_short_volume_exempt", th.IntegerType),
        th.Property("short_volume", th.IntegerType),
        th.Property("short_volume_ratio", th.NumberType),
        th.Property("ticker", th.StringType),
        th.Property("total_volume", th.IntegerType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap
        self._use_cached_tickers = False

    def get_url(self, context: Context = None):
        return f"{self.url_base}/stocks/vX/short-volume"


class NewsStream(PolygonRestStream):
    """News Stream"""

    name = "news"
    publisher_schema = th.ObjectType(
        th.Property("homepage_url", th.StringType),
        th.Property("logo_url", th.StringType),
        th.Property("name", th.StringType),
        th.Property("favicon_url", th.StringType),
    )

    insight_schema = th.ObjectType(
        th.Property("ticker", th.StringType),
        th.Property("sentiment", th.StringType),
        th.Property("sentiment_reasoning", th.StringType),
        additional_properties=True,
    )

    schema = th.PropertiesList(
        th.Property("amp_url", th.StringType),
        th.Property("article_url", th.StringType),
        th.Property("author", th.StringType),
        th.Property("description", th.StringType),
        th.Property("id", th.StringType),
        th.Property("image_url", th.StringType),
        th.Property("insights", th.ArrayType(insight_schema)),
        th.Property("keywords", th.ArrayType(th.StringType)),
        th.Property("published_utc", th.StringType),
        th.Property("publisher", publisher_schema),
        th.Property("tickers", th.ArrayType(th.StringType)),
        th.Property("title", th.StringType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        self.tap = tap
        self._use_cached_tickers = False

    def get_url(self, context: Context = None):
        return f"{self.url_base}/v2/reference/news"
