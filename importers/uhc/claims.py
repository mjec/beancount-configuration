import json

from copy import deepcopy

from beancount.utils.date_utils import parse_date_liberally
from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.importers.mixins import identifier, filing
from beancount.core import data, flags, account as beancount_account
from beancount.core.number import D


class Importer(identifier.IdentifyMixin,
               filing.FilingMixin,
               ImporterProtocol):
    '''
    Importer for UHC claims exports.
    '''

    prefix = 'UHC'
    tags = set()
    narration_format = 'UHC claim for {providerName} services'\
        ' to {patientName} on {visitDate:%b %d, %Y}'
    link_format = "uhc-claim-{claimId}"
    reimbursement_account = "Income:Health-Insurance:Reimbursements"
    discount_account = "Income:Health-Insurance:Discounts"
    debug = False

    def __init__(
            self,
            *args,
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
            **kwargs
        )

    def extract(self, file, existing_entries=None):
        transactions = []
        with open(file.name, "r") as fp:
            rows = json.load(fp)['claims']
        row_number = 0
        for row in rows:
            row_number += 1
            if self.debug:
                print("Row #{}: {}".format(row_number, row))
            transactions.extend(self.get_transactions_from_row(
                row, file.name, row_number, existing_entries))
        transactions.sort(key=lambda t: t.date)
        return transactions

    def get_transactions_from_row(
            self,
            row,
            file_name,
            row_number,
            existing_entries=None):

        flag = flags.FLAG_WARNING

        formatted_row = deepcopy(row)

        formatted_row['visitDate'] = parse_date_liberally(row['serviceDate'])

        formatted_row['patientName'] = '{} {}'.format(
            row['serviceRecipient']['firstName'],
            row['serviceRecipient']['lastName'])

        metadata = {
            'claim-number': row['claimId'],
            'claim-type': row['claimType'],
            'patient': formatted_row['patientName'],
            'provider': row['providerName'],
            'visit-date': formatted_row['visitDate'],
        }

        postings = []
        if row['balance']['healthPlanPays']['value'] != "0.00":
            postings.append(data.Posting(
                account=self.reimbursement_account,
                units=data.Amount(D(row['balance']['healthPlanPays']['value']),
                                  row['balance']['healthPlanPays']['iso4217']),
                cost=None,
                price=None,
                flag=None,
                meta={}))

        if row['balance']['healthPlanDiscount']['value'] != "0.00":
            postings.append(data.Posting(
                account=self.discount_account,
                units=data.Amount(
                    D(row['balance']['healthPlanDiscount']['value']),
                    row['balance']['healthPlanDiscount']['iso4217']),
                cost=None,
                price=None,
                flag=None,
                meta={}))

        if not postings or formatted_row['visitDate'] > parse_date_liberally('2019-12-31'):
            return []

        return [
            data.Transaction(  # pylint: disable=not-callable
                data.new_metadata(file_name,
                                  row_number,
                                  metadata),
                parse_date_liberally(row['processedDate']),
                flag,
                "United Healthcare",
                self.narration_format.format(**formatted_row),
                self.tags,
                set([self.link_format.format(**row)]),  # links
                postings)
        ]
