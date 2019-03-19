from collections import namedtuple
from beancount.core import account
import re

CategorizerResult = namedtuple(
    'CategorizerResult',
    ['account', 'payee', 'narration', 'tags', 'flag'],
    defaults=[None, None, set(), None])


def merge_categorizer_results(left, right):
    '''Returns the union of left and right'''
    replacements = {}
    mergeable_fields = {
        'tags': lambda l, r: l.update(r),
    }

    for i, field in enumerate(CategorizerResult._fields):
        if field in mergeable_fields:
            left[i] = mergeable_fields[field](left, right)
            continue

        assert left[i] is None or right[i] is None or left[i] == right[i],\
            f"Cannot merge CategorizerResults with different {field} values"
        if left[i] is None:
            replacements[field] = right[i]

    return left._replace(**replacements)


CategorizerResult.merge = merge_categorizer_results


def C(
        pattern,
        account_name,
        field_name='description',
        row_must_have_field=True,
        **kwargs):
    '''
    Make a categorizer that returns the given account when the given regular
    expression is matched on the row's $field_name field (defaults to
    description). All CategorizerResult fields except account are valid keyword
    arguments.
    '''

    assert 'account' not in kwargs,\
        "'account' is not permitted as a keyword argument"

    assert account.is_valid(account_name),\
        "{} is not a valid account".format(account_name)

    def f(row):
        row = row._asdict()
        if row_must_have_field:
            assert field_name in row,\
                f'Row used for categorizer must have a {field_name} field'

        if re.search(pattern, row[field_name], re.IGNORECASE) is not None:
            return CategorizerResult(account=account_name, **kwargs)
        return None

    return f
