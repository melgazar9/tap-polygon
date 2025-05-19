"""Stream type classes for tap-polygon."""

from __future__ import annotations
from singer_sdk.exceptions import ConfigValidationError
import typing as t
from importlib import resources
from singer_sdk import typing as th
import logging
from tap_polygon.client import PolygonRestStream
import pandas as pd
from dataclasses import asdict
import json
from datetime import datetime, timezone

from singer_sdk.helpers._state import increment_state
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
                check_missing_fields(self.schema, ticker)
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

    # def partitions(self) -> list[dict] | None:
    #     return {ticker: self.ticker}

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

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        config_params = self.config.get("daily_market_summary")

        if len(config_params) == 1 and "params" in config_params[0]:
            params = config_params[0]["params"]

        if "date" in params:
            date = params.get("date")
            if date is None or date == "":
                date = datetime.today().date().isoformat()
        else:
            date = datetime.today().date().isoformat()

        params["date"] = date

        data = self.client.get_grouped_daily_aggs(**params)

        for record in data:
            yield asdict(record)

class DailyTickerSummaryStream(PolygonRestStream):
    name = "daily_ticker_summary"
    schema = th.PropertiesList(
        th.Property("symbol", th.StringType),
        th.Property("from_", th.StringType),
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

    def __init__(self, tap, ticker_provider: CachedTickerProvider):
        super().__init__(tap)
        self.ticker_provider = ticker_provider

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        config_params = self.config.get("daily_ticker_summary")

        if len(config_params) == 1 and "params" in config_params[0]:
            params = config_params[0]["params"]

        if "date" in params:
            date = params.get("date")
            if date is None or date == "":
                date = datetime.today().date().isoformat()
        else:
            date = datetime.today().date().isoformat()

        params["date"] = date

        ticker_records = self.ticker_provider.get_tickers()
        for ticker_record in ticker_records:
            ticker = ticker_record["ticker"]
            params["ticker"] = ticker
            ticker_summary = self.client.get_daily_open_close_agg(**params)
            ticker_summary = asdict(ticker_summary)
            check_missing_fields(self.schema, ticker_summary)
            yield ticker_summary

class PreviousDayBarSummaryStream(PolygonRestStream):
    """ Retrieve the previous trading day's OHLCV data for a specified stock ticker. Not really useful given we have the other streams. """
    name = "previous_day_bar"
    pass

class TickerSnapshotStream(PolygonRestStream):
    """ Retrieve the most recent market data snapshot for a single ticker. Not really useful given we have the other streams."""
    name = "ticker_snapshot"
    pass

class FullMarketSnapshotStream(PolygonRestStream):
    """
        Retrieve a comprehensive snapshot of the entire U.S. stock market, covering over 10,000+ actively traded tickers in a single response.
        Not really useful given we have the other streams.
    """
    name = "full_market_snapshot"
    pass

class UnifiedSnapshotStream(PolygonRestStream):
    """
        Retrieve unified snapshots of market data for multiple asset classes including stocks, options, forex, and cryptocurrencies in a single request.
        Not really useful given we have the other streams.
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

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        config_params = self.config.get("top_market_movers")
        if len(config_params) == 1 and "params" in config_params[0]:
            params = config_params[0]["params"]
            if "market_type" not in params.keys():
                params["market_type"] = "stocks"
        else:
            raise ConfigValidationError("Could not parse config properly for top_market_movers")

        if params["direction"] == "" or params["direction"].lower() == "both" or "direction" not in params:
            for direction in ["gainers", "losers"]:
                params["direction"] = direction
                data = self.client.get_snapshot_direction(**params)
                for record in data:
                    record = asdict(record)
                    record["direction"] = direction
                    yield record
        else:
            data = self.client.get_snapshot_direction(**params)
            for record in data:
                record = asdict(record)
                yield record


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
    replication_key = "participant_timestamp"
    replication_method = "INCREMENTAL"
    is_sorted = False  # Issue updating the incremental state
    is_timestamp_replication_key = False  # It's technically true but set to False because the incremental key is in nanosecond epoch time.
    schema = th.PropertiesList(
        th.Property("conditions", th.ArrayType(th.AnyType())),
        th.Property("correction", th.AnyType()),
        th.Property("exchange", th.NumberType),
        th.Property("id", th.StringType),
        th.Property("participant_timestamp", th.IntegerType),
        th.Property("price", th.NumberType),
        th.Property("sequence_number", th.IntegerType),
        th.Property("sip_timestamp", th.IntegerType),
        th.Property("size", th.IntegerType),
        th.Property("tape", th.IntegerType),
        th.Property("trf_id", th.IntegerType),
        th.Property("trf_timestamp", th.NumberType),
    ).to_dict()

    def __init__(self, tap, ticker_provider: CachedTickerProvider):
        super().__init__(tap)
        self.ticker_provider = ticker_provider
        self._ticker_records = self.ticker_provider.get_tickers()

    @property
    def partitions(self) -> list[dict]:
        return [{"ticker": t["ticker"]} for t in self._ticker_records]

    def get_starting_timestamp(self, context: dict) -> int:
        state = self.get_context_state(context)

        start_timestamp_cfg = self.config.get("start_date")
        start_timestamp_cfg_ns = int(
            datetime.fromisoformat(start_timestamp_cfg.replace("Z", "+00:00")).timestamp() * 1e9
        )

        state_timestamp_ns = state.get("replication_key_value")

        if state_timestamp_ns is None:
            state_timestamp_ns = start_timestamp_cfg_ns
        else:
            state_timestamp_ns = int(state_timestamp_ns)

        start_timestamp_ns = max(start_timestamp_cfg_ns, state_timestamp_ns)
        start_timestamp_iso = datetime.fromtimestamp(start_timestamp_ns / 1e9, tz=timezone.utc).isoformat()

        return start_timestamp_iso

    def get_params(self, context):
        if context is None or "ticker" not in context:
            raise RuntimeError("Partition context must include a 'ticker'.")

        ticker = context["ticker"]
        trades_config = self.config.get("trades")
        if len(trades_config) == 1 and "params" in trades_config[0]:
            base_params = trades_config[0]["params"]
        else:
            raise ConfigValidationError("Could not parse config for trades stream.")

        params = base_params.copy()
        params.pop("tickers", None)
        params["ticker"] = ticker
        params["timestamp_gte"] = self.get_starting_timestamp(context)
        return params

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        params = self.get_params(context)
        state = self.get_context_state(context)

        for trade in self.client.list_trades(**params):
            record = asdict(trade)
            check_missing_fields(self.schema, record)
            increment_state(
                state,
                replication_key=self.replication_key,
                latest_record=record,
                is_sorted=self.is_sorted,
                check_sorted=self.check_sorted,
            )
            yield record


class QuoteStream(TradeStream):
    name = "quotes"

    schema = th.PropertiesList(
        th.Property("ask_exchange", th.IntegerType, optional=True),
        th.Property("ask_price", th.NumberType, optional=True),
        th.Property("ask_size", th.NumberType, optional=True),
        th.Property("bid_exchange", th.IntegerType, optional=True),
        th.Property("bid_price", th.NumberType, optional=True),
        th.Property("bid_size", th.NumberType, optional=True),
        th.Property("conditions", th.ArrayType(th.IntegerType), optional=True),
        th.Property("indicators", th.ArrayType(th.IntegerType), optional=True),
        th.Property("participant_timestamp", th.IntegerType),
        th.Property("sequence_number", th.IntegerType),
        th.Property("sip_timestamp", th.IntegerType),
        th.Property("tape", th.IntegerType, optional=True),
        th.Property("trf_timestamp", th.IntegerType, optional=True),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        params = self.get_params(context)
        state = self.get_context_state(context)

        for trade in self.client.list_quotes(**params):
            record = asdict(trade)
            check_missing_fields(self.schema, record)
            increment_state(
                state,
                replication_key=self.replication_key,
                latest_record=record,
                is_sorted=self.is_sorted,
                check_sorted=self.check_sorted,
            )
            yield record

class LastQuoteStream(QuoteStream):
    pass  # Need Advanced Subscription


class IndicatorStream(PolygonRestStream):
    def __init__(self, tap, ticker_provider: CachedTickerProvider):
        super().__init__(tap)
        self.ticker_provider = ticker_provider

    def get_params(self) -> t.Iterable[dict[str, t.Any]]:
        cfg_params = self.config.get(self.name)
        if len(cfg_params) == 1 and "params" in cfg_params[0]:
            params = cfg_params[0].get("params")
        else:
            raise ValueError(f"Must supply exactly one params object in the stream {self.name}.")
        return params

class SmaStream(IndicatorStream):
    name = "sma"
    schema = th.PropertiesList(
        th.Property("timestamp", th.IntegerType),
        th.Property("value", th.NumberType),
        th.Property("url", th.StringType),
        th.Property("agg_open", th.NumberType),
        th.Property("agg_high", th.NumberType),
        th.Property("agg_low", th.NumberType),
        th.Property("agg_close", th.NumberType),
        th.Property("agg_volume", th.NumberType),
        th.Property("agg_vwap", th.NumberType),
        th.Property("agg_transactions", th.NumberType),
        th.Property("agg_otc", th.BooleanType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        ticker_records = self.ticker_provider.get_tickers()
        base_params = self.get_params()
        params = base_params.copy()
        params.pop("tickers")
        for ticker_record in ticker_records:
            ticker = ticker_record.get("ticker")
            params["ticker"] = ticker
            record = self.client.get_sma(**params)
            record = asdict(record)
            flattened_records = []
            aggregates_by_ts = {agg['timestamp']: agg for agg in record['underlying']['aggregates']}

            for val in record['values']:
                ts = val['timestamp']
                agg = aggregates_by_ts.get(ts, {})
                flat_record = {
                    'timestamp': ts,
                    'value': val['value'],
                    'url': record['underlying']['url'],
                    # Add all aggregate fields with a prefix 'agg_'
                    **{f"agg_{k}": v for k, v in agg.items() if k != 'timestamp'}
                }
                flattened_records.append(flat_record)

            for fr in flattened_records:
                check_missing_fields(self.schema, fr)
                yield fr