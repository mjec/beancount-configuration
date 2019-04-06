import abc
import datetime
import os
import re
import requests
import shelve
import tempfile


def now():
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp())


class MissingApiKeyException(NameError):
    pass


class AlphaVantageBase(abc.ABC):
    @abc.abstractmethod
    def get_params_for_ticker(self, ticker): pass

    def __init__(
        self,
        api_key=None,
        *args,
        currency=None,
        temp_directory=None,
        **kwargs
    ):
        self.currency = currency or self.currency
        self.api_key = api_key or os.environ.get("ALPHAVANTAGE_API_KEY")
        if self.api_key is None:
            raise MissingApiKeyException(
                "An Alpha Vantage API key is required; put this in the "
                "ALPHAVANTAGE_API_KEY environment variable.")
        self.temp_directory = temp_directory or tempfile.gettempdir()
        super().__init__(*args, **kwargs)

    def direct_request(self, function, ticker):
        params = {
            "apikey": self.api_key,
            "function": function,
            "outputsize": "full",
        }
        params.update(self.get_params_for_ticker(ticker))
        return requests.get("https://www.alphavantage.co/query", params)

    def cached_request(self, function, ticker):
        invalid_chars = re.compile('[^A-Za-z0-9_.-]')
        filepath = os.path.join(self.temp_directory,
                                'alphavantage-{}-{}'.format(
                                    re.sub(invalid_chars, '_', function),
                                    re.sub(invalid_chars, '_', ticker)))
        with shelve.open(filepath) as db:
            if db.get('expiry', -1) < now():
                db['expiry'] = now() + (3600 * 24)
                db['result'] = self.direct_request(function, ticker).json()
            return db.get('result')
