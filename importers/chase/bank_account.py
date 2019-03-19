from beancount.utils.date_utils import parse_date_liberally

from collections import OrderedDict

from ..bank_account import BankAccountBase, RowBase


class Row(RowBase):
    row_fields_dict = OrderedDict([
        ('Transaction Date', 'transaction_date'),
        ('Post Date', 'date'),
        ('Description', 'description'),
        ('Category', 'category'),
        ('Type', 'type'),
        ('Amount', 'amount'),
    ])


class Importer(BankAccountBase):
    '''
    Importer for Chase Bank account exports.
    '''

    account = None
    currency = 'USD'
    prefix = 'Chase'
    row_class = Row

    def get_extra_metadata(self, row):
        self.assert_is_row(row)
        return {
            'purchase_date': parse_date_liberally(row.transaction_date),
        }
