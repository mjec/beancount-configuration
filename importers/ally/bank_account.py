from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.importers.mixins import identifier, filing
from beancount.core import account as beancount_account, data, flags, number
from beancount.utils.date_utils import parse_date_liberally

from collections import namedtuple, OrderedDict

from ..categorizers import CategorizerResult

import csv

RowFields = OrderedDict()
RowFields['Date'] = 'date'
RowFields['Time'] = 'time'
RowFields['Amount'] = 'amount'
RowFields['Type'] = 'type'
RowFields['Description'] = 'description'

Row = namedtuple('Row', RowFields.values())


def assert_account_is_valid(account_name):
    assert beancount_account.is_valid(account_name),\
        "{} is not a valid account".format(account_name)


class Importer(identifier.IdentifyMixin, filing.FilingMixin, ImporterProtocol):
    '''
    Importer for Ally Bank account exports.
    '''

    matchers = [  # Ways we know this is an Amazon Items file
        ('mime', 'text/csv'),
        ('content', r',\s*'.join(RowFields.keys()))
    ]

    account = None
    categorizers = []
    currency = 'USD'
    tags = set()
    debug = False

    def __init__(
            self,
            account,
            *args,
            categorizers=[],
            currency='USD',
            tags=set(),
            prefix='Ally',
            debug=False,
            **kwargs):
        '''
        One required argument:
            account         the account to use for every transaction.
        Available keyword arguments:
            categorizers    a list of callables, taking one argument, the
                            narration for a transaction (a string). The
                            callable may return a CategorizerResult, which will
                            set fields on the transaction; or a string being
                            the name of an account against which to offset the
                            entire amount of the transaction; or None, in which
                            case it will be ignored. If more than one
                            categorizer returns a non-None result, the first
                            result will be used and the transaction will be
                            flagged.
            currency        the account currency (defaults to "USD").
            tags            a set of tags to add to every transaction.
            prefix          the filename prefix to use when beancount-file
                            moves files (defaults to "Ally").
            debug           if True, every row will be printed to stdout.
        Additional keyword arguments for IdentifyMixin and FilingMixin are
        permitted. Most significantly:
            matchers        a list of 2-tuples, where the first item is one of
                            "mime", "filename", or "content"; and the second is
                            a regular expression which, if matched, identifies
                            a file as one for which this importer should be
                            used.
        '''
        assert_account_is_valid(account)
        self.account = account
        self.categorizers = categorizers
        self.currency = currency
        self.tags.update(tags)
        self.debug = debug
        super().__init__(prefix=prefix, filing=account, *args, **kwargs)

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
        assert [f.strip() for f in reader.fieldnames] == list(
            RowFields.keys()), "Header mismatch!"
        for row_number, row_dict in enumerate(reader, 1):
            if not row_dict or list(row_dict)[0][0].startswith('#'):
                continue
            yield (
                row_number,
                Row(**{RowFields[k.strip()]: v for k, v in row_dict.items()})
            )

    def get_transaction(self, row: Row, file_name, row_number):
        postings, categorizer_result = self.get_postings(row)

        if categorizer_result is None:
            # Make a dummy result to avoid having NoneType errors
            categorizer_result = CategorizerResult(self.account)

        if len(postings) != 2:
            flag = flags.FLAG_WARNING
        else:
            flag = categorizer_result.flag or flags.FLAG_OKAY

        return data.Transaction(
            data.new_metadata(file_name, row_number, {
                'time': row.time,
            }),
            parse_date_liberally(row.date),
            flag,
            categorizer_result.payee,
            categorizer_result.narration or row.description,
            self.tags | categorizer_result.tags,
            data.EMPTY_SET,  # links
            postings)

    def get_postings(self, row: Row):
        postings = [
            data.Posting(
                account=self.account,
                units=self.get_amount(row),
                cost=None,
                price=None,
                flag=None,
                meta={}),
        ]

        final_result = None

        for result in [c(row.description) for c in self.categorizers]:
            if result is None:
                continue

            if final_result is not None:
                final_result = final_result._replace(flag=flags.FLAG_WARNING)
                continue

            if not isinstance(result, CategorizerResult):
                result = CategorizerResult(result)

            assert_account_is_valid(result.account)

            final_result = result

            postings.append(data.Posting(
                account=result.account,
                units=-self.get_amount(row),
                cost=None,
                price=None,
                flag=None,
                meta={}))

        return postings, final_result

    def get_amount(self, row: Row):
        return data.Amount(number.D(row.amount), self.currency)
