from pagingish.couchdb_pager import CouchDBSkipLimitViewPager, CouchDBViewPager


class Paging(object):

    def __init__(self, request, docs, page_number=None, total_pages=None,
                 page_size=None, item_count=None, next_ref=None, prev_ref=None,
                 range_left=None, range_center=None, range_right=None):
        self.request = request
        self._docs = docs
        self._page_number = page_number
        self._total_pages = total_pages
        self._page_size = page_size
        self._item_count = item_count
        self._next_ref = next_ref
        self._prev_ref = prev_ref
        self._range_left = range_left
        self._range_center = range_center
        self._range_right = range_right

    @property
    def docs(self):
        """ The items """
        return self._docs

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
        return self.request.path_qs.replace_query('page_ref',self._next_ref)
    
    @property
    def prev(self):
        """ The prev url """
        return self.request.path_qs.replace_query('page_ref',self._prev_ref)

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


class CouchDBPaging(Paging):
    """
    if startkey is not used then we can use the view results to get the offset, counts OK
    """

    def __init__(self, view_func, view_name, default_page_size=10, **args):
        self.pager = CouchDBViewPager(view_func, view_name, **args)
        self.default_page_size = default_page_size


    def load_from_request(self, request):
        self.request = request
        page_ref = request.GET.get('page_ref')
        page_size = get_integer_from_request(request, 'page_size', self.default_page_size)
        self.load(page_ref, page_size)

    def load(self, page_ref=None, page_size=None):
        prev_ref, docs, next_ref, stats = self.pager.get(page_size, page_ref)
        page_number = None
        total_pages = None
        item_count = None
        range_left = []
        range_center = None
        range_right = []
        
        Paging.__init__(self, self.request, docs, page_number, total_pages, page_size, item_count, next_ref, prev_ref, range_left, range_center, range_right)
          


def get_integer_from_request(request, key, default):
    value = request.GET.get(key, default)
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


class CouchDBSkipLimitPaging(Paging):

    def __init__(self, view_func, view_name, count_view_name, default_page_size=10,pages_per_side=2, **args):
        assert 'limit' not in args
        assert 'skip' not in args
        self.pager = CouchDBSkipLimitViewPager(view_func, view_name, count_view_name, **args)
        self.pages_per_side = pages_per_side
        self.default_page_size = default_page_size
        self.args = args

    def load_from_request(self, request):
        self.request = request
        page_ref = get_integer_from_request(request, 'page_ref', 1)
        page_size = get_integer_from_request(request, 'page_size', self.default_page_size)
        self.load(page_ref, page_size)

    def load(self, page_number=None, page_size=None):
        prev_ref, docs, next_ref, stats = self.pager.get(page_size, page_number)
        
        range_left = [] 
        range_right = [] 
        for page in xrange( page_number-(self.pages_per_side+1), page_number+(self.pages_per_side+1)+1 ):
            position = page-page_number
            is_start = False
            is_end = False

            # Is the position at the start or the end
            if position == -(self.pages_per_side+1):
                is_start = True
            if position  == (self.pages_per_side+1):
                is_end = True

            # is the current slot within the page range
            if page < 1:
                is_within_range = False
            elif page > stats.total_pages:
                is_within_range = False
            else:
                is_within_range = True

            # If it's the start or end position and that position
            # is within the page range then add the 'dots
            if is_start and is_within_range:
                range_left.append( {'url': self.request.path.add_query('page_ref',1), 'label': '...'} )
                continue

            if is_end and is_within_range:
                range_right.append( {'url': self.request.path.add_query('page_ref',stats.total_pages), 'label': '...'} )
                continue

            if is_within_range:
                if position == 0:
                    range_center = {'label': page}
                    continue
                range =  {'url': self.request.path.add_query('page_ref',page), 'label': page}
                if position < 0:
                    range_left.append(range)
                if position > 0:
                    range_right.append(range)

        Paging.__init__(self, self.request, docs, page_number, stats.total_pages, page_size, stats.item_count, next_ref, prev_ref, range_left, range_center, range_right)

