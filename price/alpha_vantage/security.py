from datetime import datetime, timedelta

from beancount.core.number import D
from beancount.prices import source

from .base import AlphaVantageBase


class Source(AlphaVantageBase, source.Source):
    currency = 'USD'
    default_timezone = 'America/New_York'
    walk_back_days = 14

    def get_latest_price(self, ticker):
        resp = self.cached_request("GLOBAL_QUOTE", ticker)
        return source.SourcePrice(
            D(resp['Global Quote']['05. price']),
            datetime.fromisoformat(
                resp['Global Quote']['07. latest trading day']).replace(
                tzinfo=self.get_timezone(self.default_timezone)),
            self.currency)

    def get_historical_price(self, ticker, time):
        date = time.strftime('%Y-%m-%d')
        resp = self.cached_request("TIME_SERIES_DAILY", ticker)
        meta = resp['Meta Data']
        walk_back = 0
        while date not in resp['Time Series (Daily)']:
            if walk_back > self.walk_back_days:
                return None
            time -= timedelta(1)
            walk_back += 1
            date = time.strftime('%Y-%m-%d')

        return source.SourcePrice(
            D(resp['Time Series (Daily)'][date]['4. close']),
            datetime.fromisoformat(date).replace(
                tzinfo=self.get_timezone(meta['5. Time Zone'])),
            self.currency)

    def get_params_for_ticker(self, ticker):
        return {
            "symbol": ticker,
        }
