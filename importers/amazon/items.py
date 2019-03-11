#!/usr/bin/env python3

from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.importers.mixins import identifier, filing
from beancount.core import data, flags, number
from beancount.utils.date_utils import parse_date_liberally

from collections import namedtuple, OrderedDict

import csv

GIFT_CERTIFICATE_STRING = 'Gift Certificate/Card'
RECEIPT_URL = 'https://www.amazon.com/gp/your-account/order-details?orderID={}'

RowFields = OrderedDict()
RowFields['Order Date'] = 'order_date'
RowFields['Order ID'] = 'order_id'
RowFields['Title'] = 'title'
RowFields['Category'] = 'category'
RowFields['ASIN/ISBN'] = 'asin_or_isbn'
RowFields['UNSPSC Code'] = 'unspsc'
RowFields['Website'] = 'website'
RowFields['Release Date'] = 'release_date'
RowFields['Condition'] = 'condition'
RowFields['Seller'] = 'seller'
RowFields['Seller Credentials'] = 'seller_credentials'
RowFields['List Price Per Unit'] = 'list_price_per_unit'
RowFields['Purchase Price Per Unit'] = 'purchase_price_per_unit'
RowFields['Quantity'] = 'quantity'
RowFields['Payment Instrument Type'] = 'payment_instrument'
RowFields['Purchase Order Number'] = 'purchase_order_number'
RowFields['PO Line Number'] = 'po_line_number'
RowFields['Ordering Customer Email'] = 'ordering_customer_email'
RowFields['Shipment Date'] = 'shipment_date'
RowFields['Shipping Address Name'] = 'shipping_address_name'
RowFields['Shipping Address Street 1'] = 'shipping_address_street_1'
RowFields['Shipping Address Street 2'] = 'shipping_address_street_2'
RowFields['Shipping Address City'] = 'shipping_address_city'
RowFields['Shipping Address State'] = 'shipping_address_state'
RowFields['Shipping Address Zip'] = 'shipping_address_zip'
RowFields['Order Status'] = 'order_status'
RowFields['Carrier Name & Tracking Number'] = 'carrier_name_and_tracking'
RowFields['Item Subtotal'] = 'item_subtotal'
RowFields['Item Subtotal Tax'] = 'item_subtotal_tax'
RowFields['Item Total'] = 'item_total'
RowFields['Tax Exemption Applied'] = 'tax_exemption_applied'
RowFields['Tax Exemption Type'] = 'tax_exemption_type'
RowFields['Exemption Opt-Out'] = 'exemption_opt_out'
RowFields['Buyer Name'] = 'buyer_name'
RowFields['Currency'] = 'currency'
RowFields['Group Name'] = 'group_name'

Row = namedtuple('Row', RowFields.values())


class Importer(identifier.IdentifyMixin, filing.FilingMixin, ImporterProtocol):
    '''
    Importer for Amazon Items reports.
    Create a new "Items" report at https://www.amazon.com/gp/b2b/reports and
    this importer will convert those into transactions.
    '''

    matchers = [  # Ways we know this is an Amazon Items file
        ('mime', 'text/csv'),
        ('content', ','.join(RowFields.keys()))
    ]

    funding_sources = {}
    expense_account = None
    tags = {'amazon-purchase'}
    currency_symbols = {'$', '€', '£'}  # I know, very English-centric
    debug = False

    def __init__(
            self,
            *args,
            funding_sources={},
            expense_account=None,
            tags={},
            currency_symbols={},
            prefix='Amazon',
            debug=False,
            **kwargs):
        '''
        Create an importer for Amazon Items reports. Available keyword
        arguments:
            funding_source  a dict mapping the last four digits of a store card
                            (or some other card/funding source identifier) to
                            its matching liability account.
            expense_account the account to offset every transaction against (or
                            if None, unbalanced transactions will be printed).
            tags            a set of tags to add to every transaction.
            currency_symbols    a set of currency symbols to strip from input.
            prefix          the filename prefix to use when beancount-file
                            moves files (defaults to "Amazon").
            debug           if True, every row will be printed to stdout.
        Additional keyword arguments for IdentifyMixin and FilingMixin are
        permitted. Most significantly:
            filing          the name of the account used to set the storage
                            location for these files.
        '''
        self.funding_sources.update(funding_sources)
        self.expense_account = expense_account
        self.tags.update(tags)
        self.currency_symbols.update(currency_symbols)
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
        meta = data.new_metadata(file_name, row_number, {
            'receipt-url': RECEIPT_URL.format(row.order_id),
            'payment-method': row.payment_instrument,
        })
        postings = self.get_postings(row)
        if self.expense_account:
            postings.append(self.get_expense_posting(row))
        t = data.Transaction(
            meta,
            parse_date_liberally(row.order_date),
            flags.FLAG_WARNING if len(postings) == 0 else flags.FLAG_OKAY,
            row.seller.strip(),
            row.title.strip(),
            self.tags,
            data.EMPTY_SET,  # links
            postings)
        return t

    def get_postings(self, row: Row):
        postings = []
        if 'and' in row.payment_instrument:
            flag = flags.FLAG_WARNING
        else:
            flag = None
        for src, account in self.funding_sources.items():
            if src.isnumeric() and len(src) == 4:
                src = 'Amazon.com Store Card - {}'.format(src)
            if src in row.payment_instrument:
                postings.append(data.Posting(
                    account=account,
                    units=-self.get_amount(row),
                    cost=None,
                    price=None,
                    flag=flag,
                    meta={},
                ))
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
