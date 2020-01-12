from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.importers.mixins import identifier, filing
from beancount.core import data, flags, number
from beancount.utils.date_utils import parse_date_liberally

from collections import namedtuple, OrderedDict

from typing import Dict

import csv

GIFT_CERTIFICATE_STRING = 'Gift Certificate/Card'
RECEIPT_URL = 'https://www.amazon.com/gp/your-account/order-details?orderID={}'

RowFields = OrderedDict()
RowFields['Investment'] = 'investment'
RowFields['Date'] = 'date'
RowFields['Investment Activity'] = 'narration'
RowFields['Shares'] = 'shares'
RowFields['Price per Share'] = 'price_per_share'
RowFields['Market Value'] = 'price_total'

Row = namedtuple('Row', RowFields.values())


class Importer(identifier.IdentifyMixin, filing.FilingMixin, ImporterProtocol):
    '''
    Importer for John Hancock My Life Now reports.
    '''

    matchers = [  # Ways we know this is a John Hancock My Life Now report file
        ('mime', 'text/csv'),
        ('content', ','.join(RowFields.keys()))
    ]

    funding_account: str = ''
    fee_account: str = ''
    commodities: dict = {}
    debug = False

    def __init__(
            self,
            asset_accounts: Dict[str, Resolver],
            commodities: dict,
            *args,
            prefix='JohnHancock',
            debug=False,
            **kwargs):
        '''
        Create an importer for John Hancock My Life Now reports. Available
        keyword arguments:
            asset_accounts  a dictionary mapping an activity (as appears
                            in the CSV) to the relevant asset account.
            commodities     a dictionary mapping an investment name (as
                            appears in the CSV) to the relevant commodity
                            code.
            prefix          the filename prefix to use when beancount-file
                            moves files (defaults to "JohnHancock").
            debug           if True, every row will be printed to stdout.
        Additional keyword arguments for IdentifyMixin and FilingMixin are
        permitted. Most significantly:
            filing          the name of the account used to set the storage
                            location for these files.
        '''
        self.funding_account = funding_account
        self.asset_accounts = asset_accounts
        self.commodities = commodities
        self.debug = debug
        super().__init__(prefix=prefix, *args, **kwargs)

    def extract(self, file, existing_entries=None):
        transactions = []

        for row_number, row in self.get_rows(file):
            if self.debug:
                print("Row #{}: {}".format(row_number, row))
            transactions.append(self.get_transaction(
                row, file.name, row_number))

        transactions.sort(key=lambda txn: txn.date)

        return transactions

    def file_date(self, file):
        max_date = None
        for _, row in self.get_rows(file):
            parsed_date = parse_date_liberally(row.order_date)
            if max_date is None or parsed_date > max_date:
                max_date = parsed_date
        return max_date

    def get_rows(self, file):
        reader = csv.DictReader(open(file.name))
        assert reader.fieldnames == list(RowFields.keys()), "Header mismatch!"
        for row_number, row_dict in enumerate(reader, 1):
            if not row_dict or list(row_dict)[0][0].startswith('#'):
                continue
            yield (
                row_number,
                Row(**{RowFields[k]: v for k, v in row_dict.items()})
            )

    def get_transaction(self, row: Row, file_name, row_number):
        meta = data.new_metadata(file_name, row_number)
        postings = self.get_postings(row)
        if self.expense_account:
            postings.append(self.get_expense_posting(row))
        t = data.Transaction(
            meta,
            parse_date_liberally(row.order_date),
            flags.FLAG_WARNING if len(postings) == 0 else flags.FLAG_OKAY,
            row.seller.strip(),
            row.description.strip(),
            self.tags,
            data.EMPTY_SET,  # links
            postings)
        return t

    def get_postings(self, row: Row):
        if "fee" in row.narration.casefold():
        elif "contribution" in row.narration.casefold():
        postings = [
            data.Posting(
                account=account,
                units=-self.get_amount(row),
                cost=None,
                price=None,
                flag=flag,
                meta={},
            )
        ]
        return postings

    def get_expense_posting(self, row: Row):
        return data.Posting(
            account=self.expense_account,
            units=self.get_amount(row),
            cost=None,
            price=None,
            flag=None,
            meta={},
        )

    def get_amount(self, row: Row):
        total = row.item_total.strip(''.join(self.currency_symbols))
        return data.Amount(number.D(total), row.currency)
