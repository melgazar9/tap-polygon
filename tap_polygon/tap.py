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
        ]

        return streams


if __name__ == "__main__":
    TapPolygon.cli()
