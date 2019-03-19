from collections import OrderedDict

from ..bank_account import BankAccountBase, RowBase


class Row(RowBase):
    row_fields_dict = OrderedDict([
        ('Date', 'date'),
        ('Time', 'time'),
        ('Amount', 'amount'),
        ('Type', 'type'),
        ('Description', 'description'),
    ])


class Importer(BankAccountBase):
    '''
    Importer for Ally Bank account exports.
    '''

    account = None
    currency = 'USD'
    tags = set()
    debug = False
    row_class = Row

    def get_extra_metadata(self, row):
        self.assert_is_row(row)
        return {
            'time': row.time,
        }
