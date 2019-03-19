import abc

from collections import namedtuple

from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.importers.mixins import identifier, filing
from beancount.core import data, flags, account as beancount_account
from beancount.core.number import D
from beancount.utils.date_utils import parse_date_liberally

from .mixins import CategorizerMixin, CsvMixin
from .categorizers import CategorizerResult


class RowBase(abc.ABC):
    required_fields = ('amount', 'description', 'date',)
    _namedtuple = None

    @property
    @abc.abstractstaticmethod
    def row_fields_dict() -> dict:
        raise NotImplementedError

    @classmethod
    def is_instance(cls, other):
        cls.ensure_namedtuple()
        return isinstance(other, cls._namedtuple)

    @classmethod
    def ensure_namedtuple(cls):
        if cls._namedtuple is None:
            for field in cls.required_fields:
                cls.assert_field_exists(field)
            cls._namedtuple = namedtuple(
                cls.__name__,
                cls.row_fields_dict.values())

    @classmethod
    def assert_field_exists(cls, field):
        assert field in cls.row_fields_dict.values(),\
            f"Row must have a field called {field}"

    def __new__(cls, *args, **kwargs):
        cls.ensure_namedtuple()
        return cls._namedtuple(*args, **kwargs)


class BankAccountBase(
        CategorizerMixin,
        CsvMixin,
        identifier.IdentifyMixin,
        filing.FilingMixin,
        ImporterProtocol,
        abc.ABC):

    prefix = None
    tags = set()
    debug = False
    invert_amounts = False

    @property
    @abc.abstractmethod
    def account(self):
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def currency(self):
        raise NotImplementedError

    @property
    def row_fields(self):
        if hasattr(self.row_class, 'row_fields_dict'):
            return self.row_class.row_fields_dict
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def row_class(self):
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
                            moves files (defaults to "Chase").
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
        assert issubclass(self.row_class, RowBase),\
            'row_class must be subclass of RowBase'

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
            parsed_date = parse_date_liberally(row.order_date)
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
        assert self.row_class.is_instance(row),\
            "Row must be an instance of row class"
