from pagingish import couchdbpager, genericpager, listpager
from restish import url

DEFAULT_PAGES_PER_SIDE = 2
DEFAULT_PAGE_SIZE = 25

class Paging(object):
    """
    Interface for web properties
    """

    def __init__(self, request, paging_args, base_path=None):
        if base_path:
            self.base_path = url.URL(base_path)
        else:
            self.base_path = request.path_qs
        self.request = request
        if paging_args is None:
            paging_args = {}
        self._prev_ref = paging_args.get('prev')
        self._items = paging_args.get('items',[])
        self._next_ref = paging_args.get('next')

        stats = paging_args.get('stats',{})
        self._page_number = stats.get('page_number',1)
        self._total_pages = stats.get('total_pages')
        self._item_count = stats.get('item_count')
        self._page_size = stats.get('page_size')

        self._range_left = None
        self._range_right = None
        self._range_center = None

    @property
    def items(self):
        """ The items """
        if self._items is None:
            return []
        return self._items

    @property
    def page_number(self):
        """ Current page number """
        return self._page_number

    @property
    def total_pages(self):
        """ Total numbr of pages """
        return self._total_pages

    @property
    def page_size(self):
        """ Number of items per full page """
        return self._page_size

    @property
    def item_count(self):

        """ Total number of items """
        return self._item_count

    @property
    def next(self):
        """ The next url """
        return self.base_path.replace_query('pageref',self._next_ref)
    
    @property
    def prev(self):
        """ The prev url """
        return self.base_path.replace_query('pageref',self._prev_ref)

    @property
    def has_prev(self):
        return self._prev_ref is not None

    @property
    def has_next(self):
        return self._next_ref is not None

    @property
    def range_left(self):
        return self._range_left

    @property
    def range_center(self):
        return self._range_center

    @property
    def range_right(self):
        return self._range_right

    @property
    def has_range(self):
        return self._range_left is not None and \
               self._range_center is not None and \
               self._range_right is not None


class RangePaging(Paging):

    def __init__(self, request, paging_args, pages_per_side=DEFAULT_PAGES_PER_SIDE, base_path=None):
        Paging.__init__(self, request, paging_args, base_path=base_path)
        range_left = [] 
        range_right = [] 
        range_center = None
        for page in xrange( self.page_number-(pages_per_side+1), self.page_number+(pages_per_side+1)+1 ):
            position = page-self.page_number
            is_start = False
            is_end = False

            # Is the position at the start or the end
            if position == -(pages_per_side+1):
                is_start = True
            if position  == (pages_per_side+1):
                is_end = True

            # is the current slot within the page range
            if page < 1:
                is_within_range = False
            elif page > self.total_pages:
                is_within_range = False
            else:
                is_within_range = True

            # If it's the start or end position and that position
            # is within the page range then add the 'dots
            if is_start and is_within_range:
                range_left.append( {'url': self.base_path.replace_query('pageref',1), 'label': '...'} )
                continue

            if is_end and is_within_range:
                range_right.append( {'url': self.base_path.replace_query('pageref',self.total_pages), 'label': '...'} )
                continue

            if is_within_range:
                if position == 0:
                    range_center = {'label': page}
                    continue
                range =  {'url': self.base_path.replace_query('pageref',page), 'label': page}
                if position < 0:
                    range_left.append(range)
                if position > 0:
                    range_right.append(range)

            self._range_left = range_left         
            self._range_center = range_center
            self._range_right = range_right


def paged_search(request, searcher, index, query, max_pagesize=None, prefix=None):
    paging_args = request_paging_args(request, max_pagesize=max_pagesize, prefix=prefix)
    def func(skip=None, limit=None):
        return searcher.search(index, query, skip=skip, max=limit)
    pager = genericpager.SkipLimitPager(func)
    return pager.get(**paging_args)


def paged_view(request, session, view, view_args, max_pagesize=None, prefix=None):
    paging_args = request_paging_args(request, max_pagesize=max_pagesize, prefix=prefix)
    pager = couchdbpager.Pager(session.view, view, view_args)
    return pager.get(**paging_args)


def paged_skiplimit_view(request, session, view, count_view, view_args, max_pagesize=None, prefix=None):
    paging_args = request_paging_args(request, max_pagesize=max_pagesize, prefix=prefix)
    pager = couchdbpager.SkipLimitPager(session.view, view, count_view, view_args)
    return pager.get(**paging_args)


def paged_list(request, l, max_pagesize=None, prefix=None):
    paging_args = request_paging_args(request, max_pagesize=max_pagesize, prefix=prefix)
    pager = listpager.Pager(l)
    return pager.get(**paging_args)


def request_paging_args(request, max_pagesize=None, prefix=None):
    pagesize = request_pagesize(request, max_pagesize=max_pagesize, prefix=prefix)
    pageref = request_pageref(request, prefix=prefix)
    args = {}
    if pagesize is not None:
        args['pagesize'] = pagesize
    if pageref is not None:
        args['pageref'] = pageref
    return args

    
def request_pagesize(request, max_pagesize=None, prefix=None):
    if max_pagesize is None:
        max_pagesize = DEFAULT_PAGE_SIZE
    try:
        pagesize = int(request.GET.get(paging_arg(prefix, 'pagesize'), max_pagesize))
    except:
        pagesize = max_pagesize
    if pagesize is None and max_pagesize is not None:
        pagesize = max_pagesize
    if pagesize is None:
        return None
    return min(max_pagesize, pagesize)

    
def request_pageref(request, prefix=None):
    return request.GET.get(paging_arg(prefix, 'pageref'))


def paging_arg(prefix, name):
    if prefix:
        return '%s-%s' % (prefix, name)
    return name


