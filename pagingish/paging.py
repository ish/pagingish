from __future__ import with_statement
import pdb

class Paging(object):

    def __init__(self, request, docs, page_number, total_pages, page_size, item_count, next_args, prev_args, range_left, range_center, range_right):
        self.request = request
        self._docs = docs
        self._page_number = page_number
        self._total_pages = total_pages
        self._page_size = page_size
        self._item_count = item_count
        self._next_args = next_args
        self._prev_args = prev_args
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
        print 'next args',self._next_args
        return self.request.path.add_queries(self._next_args)
    
    @property
    def prev(self):
        """ The prev url """
        print 'prev args',self._prev_args
        return self.request.path.add_queries(self._prev_args)

    @property
    def has_prev(self):
        return self._prev_args != []

    @property
    def has_next(self):
        return self._next_args != []

    @property
    def range_left(self):
        return self._range_left

    @property
    def range_center(self):
        return self._range_center

    @property
    def range_right(self):
        return self._range_right


def get_integer_from_request(request, key, default):
    value = request.GET.get(key, default)
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

class ListPaging(Paging):

    def __init__(self, request, data, default_page_size=10):

        page_number = get_integer_from_request(request, 'page_number', 1)
        page_size = get_integer_from_request(request, 'page_size', default_page_size)

        item_count = len(data)

        total_pages = item_count/page_size
        if item_count % page_size:
            total_pages += 1

        if page_number > total_pages:
            page_number = total_pages

        start_of_page = (page_number-1)*page_size
        end_of_page = start_of_page + page_size
        docs = data[start_of_page:end_of_page]

        next_args = []
        prev_args = []

        if page_number > 1:
            next_args.append(('page_number', str(page_number+1)))
        if page_number < total_pages:
            prev_args.append(('page_number', str(page_number-1)))

        Paging.__init__(self, request, docs, page_number, total_pages, page_size, item_count, next_args, prev_args)



class CouchDBPaging(Paging):
    """
    if startkey is not used then we can use the view results to get the offset, counts OK

    offset is ok if no startkey
    count is ok if no startkey and endkey
    need to use startkey&startkey_docid for 'start point' of each page (apart from first page)
    need to keep startkey available in query (orginal_startkey?)
    when going backwards use original startkey as endkey
    """

    def __init__(self, request, couchish_store, view, default_page_size=10, count_view=None, pages_per_side=0, startkey=None, endkey=None, **options):
        """
        XXX if query is already reversed we need to toggle our reverses
        """

        page_number = get_integer_from_request(request, 'page_number', None)
        startkey_docid = request.GET.get('startkey_docid')
        paging_startkey = request.GET.get('paging_startkey')
        page_size = get_integer_from_request(request, 'page_size', default_page_size)

        # If we have no startkey_docid, make first query using general startkey
        # If we do have startkey_docid, start from this
        o = {}
        o['include_docs'] = True
        o['limit'] = page_size+1
        if startkey_docid is None:
            if startkey is not None:
                o['startkey'] = startkey
        else:
            o['startkey'] = paging_startkey
            o['startkey_docid'] = startkey_docid
        if endkey is not None:
            o['endkey'] = endkey
        o.update(options)
        with couchish_store.session() as S:
            doc_rows = list(S.view(view, **o))

        

        if startkey_docid is None:
            rev_doc_rows = []
        else:
            o = {}
            o['limit'] = 1
            o['skip'] = page_size
            o['descending'] = True
            o['startkey'] = paging_startkey
            o['startkey_docid'] = startkey_docid
            o['endkey'] = startkey
            o.update(options)

            with couchish_store.session() as S:
                rev_doc_rows = list(S.view(view, **o))

        docs = [row.doc for row in doc_rows]

        next_args = []
        prev_args = []

        if len(rev_doc_rows) == 1:
            prev_args.append(('startkey_docid', rev_doc_rows[-1].id))
            prev_args.append(('paging_startkey', rev_doc_rows[-1].key))
            #if page_number is not None:
            #    prev_args.append( ('page_number', str(page_number-1)) )

        if len(doc_rows) == page_size+1:
            next_args.append(('startkey_docid', doc_rows[-1].id))
            next_args.append(('paging_startkey', doc_rows[-1].key))
            #if page_number is not None:
            #    next_args.append( ('page_number', str(page_number+1)) )

        item_count = None
        #if count_view is not None:
        #    with couchish_store.session() as S:
        #        result = S.view(self.count_view)
        #    item_count = list(result)[0].value

        total_pages = None
        #if item_count is not None:
        #    total_pages = item_count/page_size

        range_left = []
        range_right = []
        range_center = {'label': page_number}

        Paging.__init__(self, request, docs, page_number, total_pages, page_size, item_count, next_args, prev_args, range_left, range_center, range_right)
          

class CouchDBPagingSkipLimit(Paging):

    def __init__(self, request, couchish_store, view, count_view, default_page_size=10, pages_per_side=2, **options):
        

        page_number = get_integer_from_request(request, 'page_number', 1)
        page_size = get_integer_from_request(request, 'page_size', default_page_size)

        # Work out the total count if a view is available
        with couchish_store.session() as S:
            result = S.view(count_view, **options)
        item_count = list(result)[0].value

        total_pages = item_count/page_size

        if item_count % page_size:
            total_pages += 1
        if page_number > total_pages:
            page_number = total_pages
        
        skip = (page_number-1)*page_size
        with couchish_store.session() as S:
            docs = S.view(view, skip=skip, limit=page_size, include_docs=True)
        docs = [row.doc for row in docs]

        next_args = []
        prev_args = []

        if page_number < total_pages:
            next_args = [('page_number', str(page_number+1))]
        if page_number >1:
            prev_args = [('page_number', str(page_number-1))]


        range_left = [] 
        range_right = [] 
        for page in xrange( page_number-(pages_per_side+1), page_number+(pages_per_side+1)+1 ):
            position = page-page_number
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
            elif page > total_pages:
                is_within_range = False
            else:
                is_within_range = True

            # If it's the start or end position and that position
            # is within the page range then add the 'dots
            if is_start and is_within_range:
                range_left.append( {'url': request.path.add_query('page_number',1), 'label': '...'} )
                continue

            if is_end and is_within_range:
                range_right.append( {'url': request.path.add_query('page_number',total_pages), 'label': '...'} )
                continue

            if is_within_range:
                if position == 0:
                    range_center = {'label': page}
                    continue
                range =  {'url': request.path.add_query('page_number',page), 'label': page}
                if position < 0:
                    range_left.append(range)
                if position > 0:
                    range_right.append(range)

        Paging.__init__(self, request, docs, page_number, total_pages, page_size, item_count, next_args, prev_args, range_left, range_center, range_right)
