import abc

from collections import namedtuple, OrderedDict

from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.importers.mixins import identifier, filing
from beancount.core import data, flags, account as beancount_account
from beancount.core.number import D
from beancount.utils.date_utils import parse_date_liberally

from .mixins import CategorizerMixin, CsvMixin
from .categorizers import CategorizerResult


def make_row_class(fields):
    '''
    Takes a list of 2-tuples, where the first element is the column
    name in the CSV, and the second element is the field name on the
    final class. The amount, description and date fields must be
    defined.
    '''

    required_fields = ('amount', 'description', 'date',)

    fields_dict = OrderedDict(fields)

    for field in required_fields:
        assert field in fields_dict.values(),\
            f"Row must have a field called {field}"

    Row = namedtuple('Row', fields_dict.values())
    Row.row_fields_dict = fields_dict

    return Row


class BankAccountBase(
        CategorizerMixin,
        CsvMixin,
        identifier.IdentifyMixin,
        filing.FilingMixin,
        ImporterProtocol,
        abc.ABC):
    '''
    Abstract base class for bank account CSV importers. You must define
    the row_class property to be an object which represents a single
    row of the CSV.
    You must either define the account property or pass it into the
    constructor. Either way it must be a valid beancount account.
    '''

    row_class = None
    account = None
    currency = 'USD'
    prefix = None
    tags = set()
    debug = False
    invert_amounts = False

    @property
    def row_fields(self):
        if hasattr(self.row_class, 'row_fields_dict'):
            return self.row_class.row_fields_dict
        raise NotImplementedError

    def get_extra_metadata(self, row):
        self.assert_is_row(row)
        return {}

    def __init__(
            self,
            *args,
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
                            moves files.
            debug           if True, every row will be printed to stdout.
        Additional keyword arguments for CategorizerMixin, IdentifyMixin and
        FilingMixin are permitted. Most significantly:
            categorizers    a list of callables, taking one argument, the
                            csv row (as a Row namedtuple object). The callable
                            may return a CategorizerResult, which will set
                            fields on the transaction; or a string being the
                            name of an account against which to offset the
                            entire amount of the transaction; or None, in which
                            case it will be ignored. If more than one
                            categorizer returns a non-None result, the first
                            result will be used and the transaction will be
                            flagged.
            matchers        a list of 2-tuples, where the first item is one of
                            "mime", "filename", or "content"; and the second is
                            a regular expression which, if matched, identifies
                            a file as one for which this importer should be
                            used.
        '''
        assert self.row_class is not None,\
            "The row_class property must be defined."

        self.account = account or self.account
        self.currency = currency or self.currency
        self.tags.update(tags)

        if invert_amounts is not None:
            self.invert_amounts = invert_amounts

        if debug is not None:
            self.debug = debug

        assert beancount_account.is_valid(self.account),\
            "{} is not a valid account".format(self.account)

        super().__init__(
            *args,
            prefix=prefix or self.prefix,
            filing=self.account,
            row_fields=self.row_fields,
            row_class=self.row_class,
            **kwargs
        )

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
            parsed_date = parse_date_liberally(row.date)
            if max_date is None or parsed_date > max_date:
                max_date = parsed_date
        return max_date

    def get_transaction(self, row, file_name, row_number):
        self.assert_is_row(row)

        postings, categorizer_result = self.get_categorized_postings(row)

        if categorizer_result is None:
            # Make a dummy result to avoid having NoneType errors
            categorizer_result = CategorizerResult(self.account)

        if len(postings) != 2:
            flag = flags.FLAG_WARNING
        else:
            flag = categorizer_result.flag or flags.FLAG_OKAY

        return data.Transaction(  # pylint: disable=not-callable
            data.new_metadata(file_name, row_number,
                              self.get_extra_metadata(row)),
            parse_date_liberally(row.date),
            flag,
            categorizer_result.payee,
            categorizer_result.narration or row.description,
            self.tags | categorizer_result.tags,
            data.EMPTY_SET,  # links
            postings)

    def get_amount(self, row):
        self.assert_is_row(row)

        if self.invert_amounts:
            return data.Amount(-D(row.amount), self.currency)
        else:
            return data.Amount(D(row.amount), self.currency)

    def assert_is_row(self, row):
        assert isinstance(row, self.row_class),\
            "Row must be an instance of row class"
