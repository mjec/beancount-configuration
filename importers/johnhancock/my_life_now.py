from beancount.core import data, account
from beancount.core.number import D

from ..bank_account import BankAccountBase, make_row_class
from ..categorizers import CategorizerResult


class Importer(BankAccountBase):
    '''
    Importer for John Hancock MyLifeNow account exports.
    '''

    prefix = 'John-Hancock'
    row_class = make_row_class([
        ('Investment', 'investment'),
        ('Date', 'date'),
        ('Investment Activity', 'description'),
        ('Shares', 'amount'),
        ('Price per Share', 'price_per_share'),
        ('Market Value', 'price_total'),
    ])
    currency_symbols = {'$', '€', '£'}
    currency = 'USD'
    investments: dict = {}
    fees_categorizer = None

    def __init__(self, investments, fees_categorizer, *args, **kwargs):
        self.investments.update(investments)
        self.fees_categorizer = fees_categorizer
        super().__init__(*args, **kwargs)

    def get_amount(self, row):
        self.assert_is_row(row)

        commodity = self.investments.get(row.investment)

        if self.invert_amounts:
            return data.Amount(-D(row.amount), commodity)
        else:
            return data.Amount(D(row.amount), commodity)

    def get_price(self, row):
        self.assert_is_row(row)
        price_per_share = row.price_per_share.strip(
            ''.join(self.currency_symbols) + "()")

        return data.Amount(D(price_per_share), self.currency)

    def get_account(self, row):
        result = self.fees_categorizer(row)
        if result is not None:
            if not isinstance(result, CategorizerResult):
                result = CategorizerResult(account=result)
            assert account.is_valid(result.account),\
                "{} is not a valid account".format(result.account)
            return result.account

        return self.account
