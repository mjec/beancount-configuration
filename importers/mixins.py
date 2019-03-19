from beancount.core import account, data, flags
from .categorizers import CategorizerResult

import csv


class CategorizerMixin():
    categorizers = []

    def __init__(self, *args, categorizers=[], **kwargs):
        self.categorizers.extend(categorizers)
        super().__init__(*args, **kwargs)

    def get_categorized_postings(self, row):
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

        for result in [c(row) for c in self.categorizers]:
            if result is None:
                continue

            if final_result is not None:
                # Uh oh, more than one thing matched
                final_result = final_result._replace(flag=flags.FLAG_WARNING)
                break

            if not isinstance(result, CategorizerResult):
                result = CategorizerResult(account=result)

            assert account.is_valid(result.account),\
                "{} is not a valid account".format(result.account)

            final_result = result

            postings.append(data.Posting(
                account=result.account,
                units=-self.get_amount(row),
                cost=None,
                price=None,
                flag=None,
                meta={}))

        return postings, final_result


class CsvMixin():
    def __init__(self, *args, row_fields, row_class, **kwargs):
        self.csv_row_fields = row_fields
        self.csv_row_class = row_class

        if 'matchers' not in kwargs:
            kwargs['matchers'] = []

        kwargs['matchers'].extend([
            ('mime', 'text/csv'),
            ('content', '''["']?''' +
             r'''["']?,\s*["']?'''.join(row_fields.keys()) +
             '''["']?''')
        ])

        super().__init__(*args, **kwargs)

    def get_rows(self, file):
        reader = csv.DictReader(open(file.name))
        assert [f.strip() for f in reader.fieldnames] == list(
            self.csv_row_fields.keys()), "Header mismatch!"
        for row_number, row_dict in enumerate(reader, 1):
            if not row_dict or list(row_dict)[0][0].startswith('#'):
                continue
            yield (
                row_number,
                self.csv_row_class(**{
                    self.csv_row_fields[k.strip()]: v
                    for k, v in row_dict.items()
                })
            )
