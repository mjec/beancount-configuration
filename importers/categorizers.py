from collections import namedtuple
from beancount.core import account
import re

CategorizerResult = namedtuple(
    'CategorizerResult',
    ['account', 'payee', 'narration', 'tags', 'flag'],
    defaults=[None, None, set(), None])


def C(pattern, account_name, **kwargs):
    '''
    Make a categorizer that returns the given account when the given regular
    expression is matched. All CategorizerResult fields except acocunt are
    valid keyword arguments.
    '''
    assert account.is_valid(account_name),\
        "{} is not a valid account".format(account_name)

    def f(description):
        if re.search(pattern, description, re.IGNORECASE) is not None:
            return CategorizerResult(account=account_name, **kwargs)
        return None

    return f


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
