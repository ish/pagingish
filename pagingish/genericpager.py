import itertools


class SkipLimitPager(object):
    """
    Generic efficient pager that simply calls a function, passing it item skip
    and limit.

    The func that is passed to the initialiser must have a signature of "def
    func(skip=None, limit=None)".

    Efficient simply means that the pager does not try to calculate any paging
    statistics, i.e. there is no need to calculated total items, total pages,
    etc.
    """

    def __init__(self, func):
        self.func = func

    def get(self, pagesize, pageref=None):
        # Decode the pageref.
        if pageref is not None:
            pagenum = int(pageref)
        else:
            pagenum = 0
        # Get a page of items with one extra to test if there's a next page.
        items = self.func(skip=pagenum*pagesize, limit=pagesize+1)
        # Calculate prev and next refs
        prev, next = None, None
        if pagenum:
            prev = unicode(pagenum-1)
        if len(items) > pagesize:
            next = unicode(pagenum+1)
        # Return result tuple.
        # Cope with broken slice operators, e.g. xappy/xapian.
        items = list(itertools.islice(items, pagesize))
        return {'prev': prev, 'items': items, 'next': next, 'stats': {'page_size': pagesize}}

