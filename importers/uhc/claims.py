from collections import namedtuple, OrderedDict

from beancount.utils.date_utils import parse_date_liberally
from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.importers.mixins import identifier, filing
from beancount.core import data, flags, account as beancount_account
from beancount.core.number import D

from ..mixins import CsvMixin


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

    currency = 'USD'
    prefix = 'UHC'
    tags = set()
    narration_format = 'UHC claim for {provider} services'\
        ' to {patient} on {visit_date:%b %d, %Y}'
    link_format = "uhc-claim-{claim_number}"
    reimbursement_account = "Income:Health-Insurance:Reimbursements"
    discount_account = "Income:Health-Insurance:Discounts"
    debug = False

    def __init__(
            self,
            *args,
            currency=None,
            reimbursement_account=None,
            discount_account=None,
            prefix=None,
            tags=set(),
            debug=False,
            **kwargs):
        '''
        Available keyword arguments:
            reimbursement_account   the income account to use for
                             reimbursements and filing (unless filing is
                             passed in as a keyword argument).
            discount_account the income account to use for discounts.
            currency         the currency for all transactions (defaults
                             to "USD").
            tags             a set of tags to add to every transaction.
            prefix           the filename prefix to use when beancount-file
                             moves files (defaults to "UHC").
            debug            if True, every row will be printed to stdout.
        Additional keyword arguments for IdentifyMixin and FilingMixin are
        permitted. Most significantly:
            matchers         a list of 2-tuples, where the first item is one of
                             "mime", "filename", or "content"; and the second
                             is a regular expression which, if matched,
                             identifies a file as one for which this importer
                             should be used.
        '''
        self.reimbursement_account = reimbursement_account \
            or self.reimbursement_account
        self.discount_account = discount_account or self.discount_account
        self.currency = currency or self.currency
        self.tags.update(tags)

        if debug is not None:
            self.debug = debug

        assert beancount_account.is_valid(self.reimbursement_account),\
            "{} is not a valid account".format(self.reimbursement_account)
        assert beancount_account.is_valid(self.discount_account),\
            "{} is not a valid account".format(self.discount_account)

        super().__init__(
            *args,
            prefix=prefix or self.prefix,
            filing=kwargs.get('filing', None) or self.reimbursement_account,
            row_fields=row_fields,
            row_class=Row,
            **kwargs
        )

    def extract(self, file, existing_entries=None):
        transactions = []

        for row_number, row in self.get_rows(file):
            if self.debug:
                print("Row #{}: {}".format(row_number, row))
            transaction = self.get_transaction(row, file.name, row_number)
            if transaction:
                transactions.append(transaction)

        transactions.sort(key=lambda txn: txn.date)

        return transactions

    def get_transaction(
            self,
            row: Row,
            file_name,
            row_number,
            existing_entries=None):

        flag = flags.FLAG_WARNING

        row = row._replace(**{
            'visit_date': parse_date_liberally(row.visit_date),
            'amount_billed': data.Amount(
                D(row.amount_billed.replace("$", "")), self.currency),
            'amount_deductible': data.Amount(
                D(row.amount_deductible.replace("$", "")), self.currency),
            'amount_plan_paid': data.Amount(
                D(row.amount_plan_paid.replace("$", "")), self.currency),
            'amount_plan_discount': data.Amount(
                D(row.amount_plan_discount.replace("$", "")), self.currency),
            'amount_responsible': data.Amount(
                D(row.amount_responsible.replace("$", "")), self.currency),
            'amount_paid_at_visit': data.Amount(
                D(row.amount_paid_at_visit.replace("$", "")), self.currency),
            'amount_owed': data.Amount(
                D(row.amount_owed.replace("$", "")), self.currency),
        })

        metadata = {
            'claim-number': row.claim_number,
            'claim-type': row.claim_type,
            'patient': row.patient,
            'provider': row.provider,
            'visit-date': row.visit_date,
        }

        postings = []
        if row.amount_plan_paid:
            postings.append(data.Posting(
                account=self.reimbursement_account,
                units=row.amount_plan_paid,
                cost=None,
                price=None,
                flag=None,
                meta={}))

        if row.amount_plan_discount:
            postings.append(data.Posting(
                account=self.discount_account,
                units=row.amount_plan_discount,
                cost=None,
                price=None,
                flag=None,
                meta={}))

        if not postings:
            return None

        return data.Transaction(  # pylint: disable=not-callable
            data.new_metadata(file_name,
                                  row_number,
                                  metadata),
            parse_date_liberally(row.date),
            flag,
            "United Healthcare",
            self.narration_format.format(**row._asdict()),
            self.tags,
            set([self.link_format.format(**row._asdict())]),
            postings)
