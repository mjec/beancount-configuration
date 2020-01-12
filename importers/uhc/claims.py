from collections import namedtuple, OrderedDict

from beancount.utils.date_utils import parse_date_liberally
from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.importers.mixins import identifier, filing
from beancount.core import data, flags, account as beancount_account
from beancount.core.number import D

from ..mixins import CsvMixin
from ..categorizers import CategorizerResult


row_fields = OrderedDict([
    ('Claim Number', 'claim_number'),
    ('Patient Name', 'patient'),
    ('Date Visited', 'visit_date'),
    ('Visited Provider', 'provider'),
    ('Claim Type', 'claim_type'),
    ('Claim Status', 'claim_status'),
    ('Payment Status', 'payment_status'),
    ('Date Processed', 'date'),
    ('Amount Billed', 'amount_billed'),
    ('Deductible', 'amount_deductible'),
    ('Your Plan', 'amount_plan_paid'),
    ('Plan Discount', 'amount_plan_discount'),
    ('Your Responsibility', 'amount_responsible'),
    ('Paid at Visit/Pharmacy', 'amount_paid_at_visit'),
    ('You Owe', 'amount_owed'),
    ('Flagged To Watch', 'flagged_to_watch'),
    ('Marked as Paid', 'marked_as_paid'),
])
Row = namedtuple('Row', row_fields.values())


class Importer(CsvMixin,
               identifier.IdentifyMixin,
               filing.FilingMixin,
               ImporterProtocol):
    '''
    Importer for UHC claims exports.
    '''

    account_formats = {
        'deductible': 'Equity:Virtual:{prefix}:'
    }
    currency = 'USD'
    prefix = 'UHC'
    tags = set()
    debug = False

    def __init__(
            self,
            *args,
            patient_to_subaccount={},
            account=None,
            currency=None,
            prefix=None,
            tags=set(),
            debug=False,
            invert_amounts=None,
            **kwargs):
        '''
        Available keyword arguments:
            account         the account to use for every transaction.
            currency        the account currency (defaults to "USD").
            tags            a set of tags to add to every transaction.
            prefix          the filename prefix to use when beancount-file
                            moves files (defaults to "UHC").
            debug           if True, every row will be printed to stdout.
        Additional keyword arguments for IdentifyMixin and FilingMixin are
        permitted. Most significantly:
            matchers        a list of 2-tuples, where the first item is one of
                            "mime", "filename", or "content"; and the second is
                            a regular expression which, if matched, identifies
                            a file as one for which this importer should be
                            used.
        '''
        self.account = account or self.account
        self.currency = currency or self.currency
        self.tags.update(tags)

        if debug is not None:
            self.debug = debug

        assert beancount_account.is_valid(self.account),\
            "{} is not a valid account".format(self.account)

        super().__init__(
            *args,
            prefix=prefix or self.prefix,
            filing=self.account,
            row_fields=row_fields,
            row_class=Row,
            **kwargs
        )

    def format_account(self, row):
        '{type}:{virtual}'
        return self.account_format.format(

        )

    def extract(self, file, existing_entries=None):
        transactions = []

        for row_number, row in self.get_rows(file):
            if self.debug:
                print("Row #{}: {}".format(row_number, row))
            transactions.extend(self.get_transactions_from_row(
                row, file.name, row_number, existing_entries))
        transactions.sort(key=lambda t: t.date)

    def get_transactions_from_row(
            self,
            row,
            file_name,
            row_number,
            existing_entries=None):
        self.assert_is_row(row)

        postings, categorizer_result = self.get_categorized_postings(row)

        if categorizer_result is None:
            # Make a dummy result to avoid having NoneType errors
            categorizer_result = CategorizerResult(self.account)

        if len(postings) != 2:
            flag = flags.FLAG_WARNING
        else:
            flag = categorizer_result.flag or flags.FLAG_OKAY

        metadata = {
            'claim-number': row.claim_number,
        }
        for existing_txn in existing_entries or []:
            if row.claim_number and \
                    row.claim_number == existing_txn.meta.get('claim-number'):
                metadata['__duplicate__'] = True

        return [
            data.Transaction(  # pylint: disable=not-callable
                data.new_metadata(file_name,
                                  row_number,
                                  metadata),
                parse_date_liberally(row.date),
                flag,
                categorizer_result.payee,
                categorizer_result.narration or row.description,
                self.tags | categorizer_result.tags,
                data.EMPTY_SET,  # links
                postings)
        ]

    def assert_is_row(self, row):
        assert isinstance(row, Row),\
            "Row must be an instance of row class"
