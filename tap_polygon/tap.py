"""Polygon tap class."""

from __future__ import annotations

import threading
import typing as t

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_polygon.base_streams import (
    DailyMarketSummaryStream,
    DailyTickerSummaryStream,
    PreviousDayBarSummaryStream,
    TickerDetailsStream,
    TopMarketMoversStream,
    TradeStream,
)
from tap_polygon.economy_streams import (
    InflationExpectationsStream,
    InflationStream,
    TreasuryYieldStream,
)
from tap_polygon.option_streams import (
    OptionsContractsStream,
)
from tap_polygon.stock_streams import (
    Bars1DayStream,
    Bars1HourStream,
    Bars1MinuteStream,
    Bars1MonthStream,
    Bars1SecondStream,
    Bars1WeekStream,
    Bars5MinuteStream,
    Bars30MinuteStream,
    Bars30SecondStream,
    ConditionCodesStream,
    DividendsStream,
    EmaStream,
    ExchangesStream,
    FinancialsStream,
    IPOsStream,
    MACDStream,
    MarketHolidaysStream,
    MarketStatusStream,
    NewsStream,
    PolygonRestStream,
    RelatedCompaniesStream,
    RSIStream,
    ShortInterestStream,
    ShortVolumeStream,
    SmaStream,
    SplitsStream,
    StockTickerStream,
    TickerEventsStream,
    TickerTypesStream,
)

# Streams to implement later:
# - TickerSnapshotStream
# - FullMarketSnapshotStream
# - UnifiedSnapshotStream

# Streams not imported because advanced subscription is required
# - QuoteStream
# - LastQuoteStream


class TapPolygon(Tap):
    name = "tap-polygon"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
        ),
    ).to_dict()

    _cached_stock_tickers: t.List[dict] | None = None
    _stock_tickers_stream_instance: StockTickerStream | None = None
    _stock_tickers_lock = threading.Lock()

    _cached_option_tickers: t.List[dict] | None = None
    _option_tickers_stream_instance: "OptionsContractsStream" | None = None
    _option_tickers_lock = threading.Lock()

    def get_stock_tickers_stream(self) -> StockTickerStream:
        if self._stock_tickers_stream_instance is None:
            self.logger.info("Creating StockTickerStream instance...")
            self._stock_tickers_stream_instance = StockTickerStream(self)
        return self._stock_tickers_stream_instance

    def get_cached_stock_tickers(self) -> t.List[dict]:
        if self._cached_stock_tickers is None:
            with self._stock_tickers_lock:
                if self._cached_stock_tickers is None:
                    self.logger.info("Fetching and caching stock tickers...")
                    stock_tickers_stream = self.get_stock_tickers_stream()
                    self._cached_stock_tickers = list(
                        stock_tickers_stream.get_records(context=None)
                    )
                    self.logger.info(
                        f"Cached {len(self._cached_stock_tickers)} stock tickers."
                    )
        return self._cached_stock_tickers

    def get_option_tickers_stream(self) -> "OptionsContractsStream":
        if self._option_tickers_stream_instance is None:
            self.logger.info("Creating OptionsContractsStream instance...")
            from tap_polygon.option_streams import OptionsContractsStream

            self._option_tickers_stream_instance = OptionsContractsStream(self)
        return self._option_tickers_stream_instance

    def get_cached_option_tickers(self) -> t.List[dict]:
        if self._cached_option_tickers is None:
            with self._option_tickers_lock:
                if self._cached_option_tickers is None:
                    self.logger.info("Fetching and caching option tickers...")
                    option_tickers_stream = self.get_option_tickers_stream()
                    self._cached_option_tickers = list(
                        option_tickers_stream.get_records(context=None)
                    )
                    self.logger.info(
                        f"Cached {len(self._cached_option_tickers)} option tickers."
                    )
        return self._cached_option_tickers

    def discover_streams(self) -> list[PolygonRestStream]:
        stock_tickers_stream = self.get_stock_tickers_stream()

        streams: list[PolygonRestStream] = [
            # Stock streams
            stock_tickers_stream,
            TickerDetailsStream(self),
            TickerTypesStream(self),
            RelatedCompaniesStream(self),
            DailyMarketSummaryStream(self),
            DailyTickerSummaryStream(self),
            PreviousDayBarSummaryStream(self),
            TopMarketMoversStream(self),
            TradeStream(self),
            SmaStream(self),
            EmaStream(self),
            MACDStream(self),
            RSIStream(self),
            ExchangesStream(self),
            MarketHolidaysStream(self),
            MarketStatusStream(self),
            ConditionCodesStream(self),
            IPOsStream(self),
            SplitsStream(self),
            DividendsStream(self),
            TickerEventsStream(self),
            FinancialsStream(self),
            ShortInterestStream(self),
            ShortVolumeStream(self),
            NewsStream(self),
            TreasuryYieldStream(self),
            InflationExpectationsStream(self),
            InflationStream(self),
            Bars1SecondStream(self),
            Bars30SecondStream(self),
            Bars1MinuteStream(self),
            Bars5MinuteStream(self),
            Bars30MinuteStream(self),
            Bars1HourStream(self),
            Bars1DayStream(self),
            Bars1WeekStream(self),
            Bars1MonthStream(self),
            # Options streams
            OptionsContractsStream(self),
        ]

        return streams


if __name__ == "__main__":
    TapPolygon.cli()
