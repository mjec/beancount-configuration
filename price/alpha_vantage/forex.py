from datetime import datetime, timedelta

from beancount.core.number import D
from beancount.prices import source

from .base import AlphaVantageBase


class Source(AlphaVantageBase, source.Source):
    currency = 'USD'
    walk_back_days = 14

    def get_latest_price(self, ticker):
        resp = self.cached_request("CURRENCY_EXCHANGE_RATE", ticker)
        base = resp['Realtime Currency Exchange Rate']
        last_refreshed = base["6. Last Refreshed"]
        last_refreshed_tz = base['7. Time Zone']
        return source.SourcePrice(
            D(base['5. Exchange Rate']),
            datetime.fromisoformat(last_refreshed).replace(
                tzinfo=self.get_timezone(last_refreshed_tz)),
            self.currency)

    def get_historical_price(self, ticker, time):
        date = time.strftime('%Y-%m-%d')
        resp = self.cached_request("FX_DAILY", ticker)
        walk_back = 0
        while date not in resp['Time Series FX (Daily)']:
            if walk_back > self.walk_back_days:
                return None
            time -= timedelta(1)
            walk_back += 1
            date = time.strftime('%Y-%m-%d')
        return source.SourcePrice(
            D(resp['Time Series FX (Daily)'][date]['4. close']),
            datetime.fromisoformat(date).replace(
                tzinfo=self.get_timezone(resp['Meta Data']['6. Time Zone'])),
            self.currency)

    def get_params_for_ticker(self, ticker):
        return {
            # this pair is used by FX_DAILY...
            "from_symbol": ticker,
            "to_symbol": self.currency,

            # ... and this pair is used by CURRENCY_EXCHANGE_RATE
            "from_currency": ticker,
            "to_currency": self.currency,
        }
