"""Polygon tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_polygon.streams import StockTickersStream

STREAMS = [
    StockTickersStream
]


class TapPolygon(Tap):
    """Polygon tap class."""

    name = "tap-polygon"

    def discover_streams(self) -> list[streams.PolygonStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [stream(self) for stream in STREAMS]


if __name__ == "__main__":
    TapPolygon.cli()
