from beancount.utils.date_utils import parse_date_liberally

from ..bank_account import BankAccountBase, make_row_class


class Importer(BankAccountBase):
    '''
    Importer for Chase Bank account exports.
    '''

    prefix = 'Chase'
    row_class = make_row_class([
        ('Transaction Date', 'transaction_date'),
        ('Post Date', 'date'),
        ('Description', 'description'),
        ('Category', 'category'),
        ('Type', 'type'),
        ('Amount', 'amount'),
    ])

    def get_extra_metadata(self, row):
        self.assert_is_row(row)
        return {
            'purchase_date': parse_date_liberally(row.transaction_date),
        }
