"""Polygon tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_polygon.streams import (
    StockTickersStream,
    CachedTickerProvider,
    TickerDetailsStream,
    TickerTypesStream,
    RelatedCompaniesStream,
    CustomBarsStream,
    DailyMarketSummaryStream,
    DailyTickerSummaryStream,
    TopMarketMoversStream,
    TradeStream,
    QuoteStream,
    SmaStream,
    EmaStream,
    MACDStream,
    RSIStream,
    ExchangesStream,
    MarketHolidaysStream,
    MarketStatusStream,
    ConditionCodesStream,
)


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
    _stock_tickers_stream_instance: StockTickersStream | None = None

    def get_cached_tickers(self) -> t.List[dict]:
        if self._cached_tickers is None:
            self.logger.info("Fetching and caching all stock tickers...")
            stock_tickers_stream = StockTickersStream(self)
            self._cached_tickers = list(stock_tickers_stream.get_records(context=None))
            self.logger.info(f"Cached {len(self._cached_tickers)} tickers.")
        return self._cached_tickers

    def discover_streams(self) -> list[PolygonRestStream]:
        stock_tickers_stream = StockTickersStream(self)
        ticker_provider = CachedTickerProvider(stock_tickers_stream)

        streams: list[PolygonRestStream] = [
            stock_tickers_stream,
            TickerDetailsStream(self, ticker_provider),
            TickerTypesStream(self),
            RelatedCompaniesStream(self, ticker_provider),
            CustomBarsStream(self, ticker_provider),
            DailyMarketSummaryStream(self),
            DailyTickerSummaryStream(self, ticker_provider),
            TopMarketMoversStream(self),
            TradeStream(self, ticker_provider),
            QuoteStream(self, ticker_provider),
            SmaStream(self, ticker_provider),
            EmaStream(self, ticker_provider),
            MACDStream(self, ticker_provider),
            RSIStream(self, ticker_provider),
            ExchangesStream(self),
            MarketHolidaysStream(self),
            MarketStatusStream(self),
            ConditionCodesStream(self),
        ]

        return streams


if __name__ == "__main__":
    TapPolygon.cli()
