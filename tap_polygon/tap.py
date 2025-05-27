"""Polygon tap class."""

from __future__ import annotations

import typing as t

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_polygon.stock_streams import (
    ConditionCodesStream,
    CustomBarsStream,
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
    StockTickersStream,
    TickerDetailsStream,
    TickerEventsStream,
    TickerTypesStream,
    TopMarketMoversStream,
    TradeStream,
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
            "rest_api_key",
            th.StringType,
            required=True,
            secret=True,
        ),
    ).to_dict()

    _cached_tickers: t.List[dict] | None = None
    _stock_tickers_stream_instance: StockTickersStream | None = None

    def get_cached_tickers(self) -> t.List[dict]:
        if self._cached_tickers is None:
            self.logger.info("Fetching and caching stock tickers...")
            stock_tickers_stream = self.get_stock_tickers_stream()
            self._cached_tickers = list(stock_tickers_stream.get_records(context=None))
            self.logger.info(f"Cached {len(self._cached_tickers)} tickers.")
        return self._cached_tickers

    def get_stock_tickers_stream(self) -> StockTickersStream:
        if self._stock_tickers_stream_instance is None:
            self.logger.info("Creating StockTickersStream instance...")
            self._stock_tickers_stream_instance = StockTickersStream(self)
        return self._stock_tickers_stream_instance

    def discover_streams(self) -> list[PolygonRestStream]:
        stock_tickers_stream = self.get_stock_tickers_stream()

        streams: list[PolygonRestStream] = [
            stock_tickers_stream,
            TickerDetailsStream(self),
            TickerTypesStream(self),
            RelatedCompaniesStream(self),
            CustomBarsStream(self),
            DailyMarketSummaryStream(self),
            DailyTickerSummaryStream(self),
            PreviousDayBarSummaryStream(self),
            TopMarketMoversStream(self),
            TradeStream(self),
            # QuoteStream(self),
            # LastQuoteStream(self),
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
        ]

        return streams


if __name__ == "__main__":
    TapPolygon.cli()
