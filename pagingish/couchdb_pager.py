import couchdb
import simplejson as json


class CouchDBViewPager(object):

    def __init__(self, view_func, view_name, **args):
        # We can't allow these, we need them to control paging correctly.
        assert 'limit' not in args
        assert 'startkey_docid' not in args
        assert 'endkey_docid' not in args
        self.view_func = view_func
        self.view_name = view_name
        self.args = args

    def get(self, pagesize, pageref=None):

        # Decode the pageref
        pageref = _decode_ref(pageref)

        # Copy the args and replace/update with paging control args.
        args = dict(self.args)
        if pageref:
            if pageref['direction'] == 'prev':
                # if we're going in the opposite to normal direction, reverse
                # the data scan direction and use endkey instead of startkey
                args['descending'] = not self.args.get('descending', False)
                if 'startkey' in self.args:
                    args['endkey'] = self.args.get('startkey')


            if 'startkey' in pageref:
                # if we have a startkey/startkey_docid in the pageref, then use it 
                args['startkey'] = pageref['startkey']
                if 'startkey_docid' in pageref:
                    args['startkey_docid'] = pageref['startkey_docid']
            else:
                # else this is the special hack case where we are taking the
                # last page of data by reversing the normal search
                if 'endkey' in self.args:
                    args['startkey'] = self.args['endkey']
                if 'startkey' in self.args:
                    args['endkey'] = self.args['startkey']
                args['descending'] = not self.args.get('descending', False)

        # Try to get two extra rows to detect previous and next pages as
        # efficiently as possible.
        args['limit'] = pagesize+2

        # Fetch the rows from the view_name.
        rows = list(self.view_func(self.view_name, **args))

        # Assume to ref documents by default.
        prevref = None
        nextref = None

        # Find the ref document to move a page in the opposite direction to the
        # pageref's direction (default is 'next').
        if pageref and len(rows) >=2 and rows[0].id == pageref.get('startkey_docid'):
            # 2+ rows and the first is the pagref startkey_docid'

            # If we have a page reference document then we know we have
            # previous items. Strip this pageref doc from the rows once the
            # prev button has been built
            if pageref['direction'] == 'next':
                prevref = ref_from_doc('prev',rows[1])
            else:
                nextref = ref_from_doc('next',rows[1])
            rows = rows[1:]


        elif pageref and rows and rows[0].id == pageref.get('startkey_docid'):
            # only one row and it's the pageref startkey_docid'

            # Because we don't have any real items on this page, we can't use
            # the first item as the startkey_docid for the previous button
            #
            # To cope with this we have to send a custom response to tell the
            # pager that it needs to be clever and instead of doing a normal
            # page look up, do a reverse lookup from the end of the current
            # search
            if pageref['direction'] == 'next':
                prevref = {'direction': 'prev'}
            else:
                nextref = {'direction': 'next'}
            rows = rows[1:]


        elif pageref and rows and rows[0].id != pageref.get('startkey_docid'):
            # 1+ rows and the first is not the pageref's startkey_docid'

            # We need to query from the first item but backwards (removing any
            # endkey) to check if we have anything previous. If we do then add
            # a prev link
            if 'startkey' in pageref:
                revref = rows[0]
            else:
                revref = rows[-1]
            args = dict(self.args)
            # If we're scanning backwards then the endkey must be swapped for the start key
            if 'startkey' in args:
                args['endkey'] = args['startkey']

            args['startkey'] = revref.key
            args['startkey_docid'] = revref.id
            args['limit'] = 2
            args['descending'] = not args.get('descending', False)
            revrows = list(self.view_func(self.view_name, **args))
            if len(revrows) >= 2:
                if 'startkey' in pageref:
                    if pageref['direction'] == 'next':
                        prevref = ref_from_doc('prev',revref)
                    else:
                        nextref = ref_from_doc('next',revref)
                else:
                    if pageref['direction'] == 'prev':
                        prevref = ref_from_doc('prev',revref)
                    else:
                        nextref = ref_from_doc('next',revref)

        elif pageref and not rows:
            # no rows !!

            # No data at all, but there might still be a previous page. 
            # Scan in the reverse direction to get the first item (clearing any
            # endkey to make sure we don't cut off our own results)
            args = dict(self.args)
            args['startkey'] = pageref['startkey']
            args['startkey_docid'] = pageref['startkey_docid']
            if 'endkey' in args:
                del args['endkey']
            args['limit'] = 1
            args['descending'] = not args.get('descending', False)
            revrows = list(self.view_func(self.view_name, **args))
            # We have the same issue as in the 'only one row and pagref match'
            # case. We don't have a first item on the current page that we can
            # use as the start point for a previous page. Hence we do the hack
            # which tells the pager to use the last page of results in this
            # special case
            if len(revrows) >= 1:
                if pageref['direction'] == 'next':
                    prevref = {'direction': 'prev'}
                else:
                    nextref = {'direction': 'next'}

        # Find the ref document to move a page in the same direction as the
        # pageref's direction (default is 'next').
        if len(rows) > pagesize:
            if pageref and pageref['direction'] == 'prev':
                prevref = ref_from_doc('prev',rows[pagesize-1])
            else:
                nextref = ref_from_doc('next',rows[pagesize-1])


        # Discard any extra remaining rows and return the control set.
        rows = rows[:pagesize]

        # Reverse the remaining rows if we're going backwards.
        if pageref and pageref['direction'] == 'prev':
            rows = rows[::-1]

        return _encode_ref(prevref), rows, _encode_ref(nextref), {}

def ref_from_doc(dir,doc):
    return {'direction': dir, 'startkey': doc.key, 'startkey_docid': doc.id}


class CouchDBSkipLimitViewPager(object):

    def __init__(self, view_func, view_name, count_view_name, **args):
        # We can't allow these, we need them to control paging correctly.
        assert 'limit' not in args
        assert 'startkey_docid' not in args
        assert 'endkey_docid' not in args
        self.view_func = view_func
        self.view_name = view_name
        self.count_view_name = count_view_name
        self.args = args

    def get(self, pagesize, pageref=None):
        try:
            page_number = int(pageref)
        except (ValueError, TypeError):
            page_number = 1
        
        # Work out the total count of items 
        result = list(self.view_func(self.count_view_name))
        if len(result) == 0:
            item_count = 0
        else:
            item_count = result[0].value

        # Work out total number of pages. e.g. for page_size = 10, 0-10 items
        # is page 1, 11-20 items is page 2, etc. Cope with out of bounds
        # page_numbers
        total_pages = item_count/pagesize
        if item_count % pagesize:
            total_pages += 1
        if page_number > total_pages:
            page_number = total_pages
        if page_number < 1:
            page_number == 1
        
        skip = (page_number-1)*pagesize
        docs = list(self.view_func(self.view_name, skip=skip, limit=pagesize, **self.args))

        # next and prev pagerefs (None for not available)
        nextref = None
        prevref = None
        if page_number < total_pages:
            nextref = str(page_number+1)
        if page_number >1:
            prevref = str(page_number-1)

        stats = {'page_number': page_number,
                 'total_pages': total_pages,
                 'item_count': item_count}

        return prevref, docs, nextref, stats



def _encode_ref(ref):
    if ref is None:
        return None
    if 'startkey' not in ref:
        return json.dumps([ref['direction']])
    return json.dumps([ref['direction'], ref['startkey'], ref['startkey_docid']])


def _decode_ref(ref):
    if ref is None:
        return None
    return dict(zip(['direction', 'startkey', 'startkey_docid'], json.loads(ref)))



