"""Polygon tap class."""

from __future__ import annotations

import typing as t

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_polygon.client import StockTickerStream

from tap_polygon.base_streams import (
    DailyMarketSummaryStream,
    DailyTickerSummaryStream,
    PreviousDayBarSummaryStream,
    TickerDetailsStream,
    TopMarketMoversStream,
    TradeStream,
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
    TickerEventsStream,
    TickerTypesStream,
)

from tap_polygon.economy_streams import (
    InflationExpectationsStream,
    InflationStream,
    TreasuryYieldStream,
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

    _cached_tickers: t.List[dict] | None = None
    _tickers_stream_instance: StockTickerStream | None = None

    def get_stock_tickers_stream(self) -> StockTickerStream:
        if self._tickers_stream_instance is None:
            self.logger.info("Creating StockTickerStream instance...")
            self._tickers_stream_instance = StockTickerStream(self)
        return self._tickers_stream_instance

    def get_cached_stock_tickers(self) -> t.List[dict]:
        if self._cached_tickers is None:
            self.logger.info("Fetching and caching stock tickers...")
            stock_tickers_stream = self.get_stock_tickers_stream()
            self._cached_tickers = list(stock_tickers_stream.get_records(context=None))
            self.logger.info(f"Cached {len(self._cached_tickers)} tickers.")
        return self._cached_tickers

    def discover_streams(self) -> list[PolygonRestStream]:
        stock_tickers_stream = self.get_stock_tickers_stream()

        streams: list[PolygonRestStream] = [
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
        ]

        return streams


if __name__ == "__main__":
    TapPolygon.cli()
