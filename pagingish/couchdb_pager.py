import time
import couchdb
import simplejson as json

class Stats(object):
    pass

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

        S = Stats()

        # Decode the pageref
        pageref = _decode_ref(pageref)

        # Copy the args and replace/update with paging control args.
        args = dict(self.args)
        if pageref:
            if pageref['direction'] == 'prev':
                args['descending'] = not args.get('descending', False)
                if 'startkey' in args:
                    args['endkey'] = args.get('startkey')
            args['startkey'] = pageref['startkey']
            args['startkey_docid'] = pageref['startkey_docid']

        # Try to get two extra rows to detect previous and next pages as
        # efficiently as possible.
        args['limit'] = pagesize+2

        # Fetch the rows from the view_name.
        print pageref, self.view_name, args
        rows = list(self.view_func(self.view_name, **args))

        # Assume to ref documents by default.
        prevref = None
        nextref = None

        # Find the ref document to move a page in the opposite direction to the
        # pageref's direction (default is 'next').
        if pageref and len(rows) >=2 and rows[0].id == pageref['startkey_docid']:
            # Page reference document found
            if pageref['direction'] == 'next':
                prevref = rows[1]
            else:
                nextref = rows[1]
            rows = rows[1:]
        elif pageref and rows and rows[0].id != pageref['startkey_docid']:
            # Page ref document missing, so need to work out if there is a next
            # or prev page by calling the view in that "direction".
            raise NotImplementError()
        elif pageref and not rows:
            # No data at all, but might still be a previous page. Is this
            # really the same as the previous elif?
            raise NotImplementError()

        # Find the ref document to move a page in the same direction as the
        # pageref's direction (default is 'next').
        if len(rows) > pagesize:
            if pageref and pageref['direction'] == 'prev':
                prevref = rows[pagesize-1]
            else:
                nextref = rows[pagesize-1]

        # Turn ref docs into dicts.
        if prevref:
            prevref = {'direction': 'prev', 'startkey': prevref.key, 'startkey_docid': prevref.id}
        if nextref:
            nextref = {'direction': 'next', 'startkey': nextref.key, 'startkey_docid': nextref.id}

        # Discard any extra remaining rows and return the control set.
        rows = rows[:pagesize]

        # Reverse the remaining rows if we're going backwards.
        if pageref and pageref['direction'] == 'prev':
            rows = rows[::-1]

        return _encode_ref(prevref), rows, _encode_ref(nextref), S


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

        S = Stats()

        try:
            page_number = int(pageref)
        except (ValueError, TypeError):
            page_number = 1
        
        # Work out the total count if a view is available
        result = list(self.view_func(self.count_view_name))
        item_count = result[0].value

        total_pages = item_count/pagesize

        if item_count % pagesize:
            total_pages += 1
        if page_number > total_pages:
            page_number = total_pages
        
        skip = (page_number-1)*pagesize
        docs = list(self.view_func(self.view_name, skip=skip, limit=pagesize, **self.args))

        nextref = None
        prevref = None

        if page_number < total_pages:
            nextref = str(page_number+1)
        if page_number >1:
            prevref = str(page_number-1)

        S.page_number = page_number
        S.total_pages = total_pages
        S.item_count = item_count
        return prevref, docs, nextref, S



def _encode_ref(ref):
    if ref is None:
        return None
    return json.dumps([ref['direction'], ref['startkey'], ref['startkey_docid']])


def _decode_ref(ref):
    if ref is None:
        return None
    return dict(zip(['direction', 'startkey', 'startkey_docid'], json.loads(ref)))


if __name__ == '__main__':
    db = couchdb.Server()['test']
    #p = CouchDBSkipLimitViewPager(db.view, 'test/all','test/count')
    p = CouchDBSkipLimitViewPager(db.view, 'test/all','test/count', descending=True)
    #p = CouchDBViewPager(db.view, 'test/test')
    #p = CouchDBViewPager(db.view, 'test/test', descending=True)
    #db = couchdb.Server()['paging2']
    #p = CouchDBViewPager(db.view, 'test/test', startkey=["b"], endkey=["b", {}])
    #p = CouchDBViewPager(db.view, 'test/test', startkey=["b", {}], endkey=["b"], descending=True)
    next = None
    print "***** forwards"
    while True:
        prev, rows, next, stats = p.get(5, next)
        print "* prev:", prev
        print "* next:", next
        for row in rows:
            print "-", row
        print
        time.sleep(0.5)
        if not next:
            break
    print "***** backwards"
    while True:
        prev, rows, next, stats = p.get(5, prev)
        print "* prev:", prev
        print "* next:", next
        for row in rows:
            print "-", row
        print
        time.sleep(0.5)
        if not prev:
            break
    print "***** forward one page"
    prev, rows, next, stats = p.get(5, next)
    print "* prev:", prev
    print "* next:", next
    for row in rows:
        print "-", row

