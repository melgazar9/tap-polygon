"""Options stream classes for tap-polygon."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_polygon.client import BaseTickerStream


class OptionsContractsStream(BaseTickerStream):
    """Stream for retrieving all options contracts."""

    name = "options_contracts"
    primary_keys = ["ticker"]
    market = "option"
    ticker_param = "underlying_ticker"  # Options API uses underlying_ticker
    _ticker_in_path_params = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("contract_type", th.StringType),
        th.Property("expiration_date", th.DateType),
        th.Property("strike_price", th.NumberType),
        th.Property("underlying_ticker", th.StringType),
        th.Property("exercise_style", th.StringType),
        th.Property("shares_per_contract", th.IntegerType),
        th.Property("cfi", th.StringType),
        th.Property("primary_exchange", th.StringType),
        th.Property("additional_underlyings", th.ArrayType(th.ObjectType())),
    ).to_dict()

    def get_url(self, context: Context = None) -> str:
        return f"{self.url_base}/v3/reference/options/contracts"

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        context, query_params, path_params = self._prepare_context_and_params(context)
        context["query_params"] = query_params
        context["path_params"] = path_params
        yield from super().get_records(context)
