"""Polygon tap class."""

from __future__ import annotations

import typing as t

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_polygon.client import TickerStream
from tap_polygon.stock_streams import (
    ConditionCodesStream,
    DailyMarketSummaryStream,
    DailyTickerSummaryStream,
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
    PreviousDayBarSummaryStream,
    RelatedCompaniesStream,
    RSIStream,
    ShortInterestStream,
    ShortVolumeStream,
    SmaStream,
    SplitsStream,
    StockBars1DayStream,
    StockBars1HourStream,
    StockBars1MinuteStream,
    StockBars1MonthStream,
    StockBars1SecondStream,
    StockBars1WeekStream,
    StockBars5MinuteStream,
    StockBars30MinuteStream,
    StockBars30SecondStream,
    StockTradeStream,
    TickerDetailsStream,
    TickerEventsStream,
    TickerTypesStream,
    TopMarketMoversStream,
    TreasuryYieldStream,
)

# Streams to implement later:
# - TickerSnapshotStream
# - FullMarketSnapshotStream
# - UnifiedSnapshotStream

# Streams not imported because advanced subscription is required
# - StockQuoteStream
# - StockLastQuoteStream


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
    _tickers_stream_instance: TickerStream | None = None

    def get_cached_tickers(self) -> t.List[dict]:
        if self._cached_tickers is None:
            self.logger.info("Fetching and caching stock tickers...")
            tickers_stream = self.get_tickers_stream()
            self._cached_tickers = list(tickers_stream.get_records(context=None))
            self.logger.info(f"Cached {len(self._cached_tickers)} tickers.")
        return self._cached_tickers

    def get_tickers_stream(self) -> TickerStream:
        if self._tickers_stream_instance is None:
            self.logger.info("Creating TickerStream instance...")
            self._tickers_stream_instance = TickerStream(self)
        return self._tickers_stream_instance

    def discover_streams(self) -> list[PolygonRestStream]:
        tickers_stream = self.get_tickers_stream()

        streams: list[PolygonRestStream] = [
            tickers_stream,
            TickerDetailsStream(self),
            TickerTypesStream(self),
            RelatedCompaniesStream(self),
            DailyMarketSummaryStream(self),
            DailyTickerSummaryStream(self),
            PreviousDayBarSummaryStream(self),
            TopMarketMoversStream(self),
            StockTradeStream(self),
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
            StockBars1SecondStream(self),
            StockBars30SecondStream(self),
            StockBars1MinuteStream(self),
            StockBars5MinuteStream(self),
            StockBars30MinuteStream(self),
            StockBars1HourStream(self),
            StockBars1DayStream(self),
            StockBars1WeekStream(self),
            StockBars1MonthStream(self),
        ]

        return streams


if __name__ == "__main__":
    TapPolygon.cli()
