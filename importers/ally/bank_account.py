from ..bank_account import BankAccountBase, make_row_class


class Importer(BankAccountBase):
    '''
    Importer for Ally Bank account exports.
    '''

    prefix = 'Ally'
    row_class = make_row_class([
        ('Date', 'date'),
        ('Time', 'time'),
        ('Amount', 'amount'),
        ('Type', 'type'),
        ('Description', 'description'),
    ])

    def get_extra_metadata(self, row):
        self.assert_is_row(row)
        return {
            'time': row.time,
        }
