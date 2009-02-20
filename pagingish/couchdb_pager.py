import time
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
                args['descending'] = not self.args.get('descending', False)
                if 'startkey' in self.args:
                    args['endkey'] = self.args.get('startkey')


            if 'startkey' in pageref:
                args['startkey'] = pageref['startkey']
                if 'startkey_docid' in pageref:
                    args['startkey_docid'] = pageref['startkey_docid']
            else:
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
            print '### 2+ rows and the first is the pagref startkey_docid'
            # Page reference document found
            if pageref['direction'] == 'next':
                prevref = ref_from_doc('prev',rows[1])
            else:
                nextref = ref_from_doc('next',rows[1])
            rows = rows[1:]
        elif pageref and rows and rows[0].id == pageref.get('startkey_docid'):
            print '### only one row and it\'s the pageref startkey_docid'
            if pageref['direction'] == 'next':
                prevref = {'direction': 'prev'}
            else:
                nextref = {'direction': 'next'}
            rows = rows[1:]
        elif pageref and rows and rows[0].id != pageref.get('startkey_docid'):
            # Page ref document missing, so need to work out if there is a next
            # or prev page by calling the view in that "direction".
            print '### 1+ and the first is not the pagref startkey_docid'
            args = dict(self.args)
            args['startkey'] = rows[0].key
            args['startkey_docid'] = rows[0].id
            if 'endkey' in args:
                del args['endkey']
            args['limit'] = 1
            args['descending'] = not args.get('descending', False)
            revrows = list(self.view_func(self.view_name, **args))
            print '(( got %s for revrows using %s args ))'%(revrows, args)
            if len(revrows) >= 1:
                if pageref['direction'] == 'next':
                    prevref = ref_from_doc('prev',rows[0])
                else:
                    nextref = ref_from_doc('next',rows[0])

        elif pageref and not rows:
            print '### no rows !!'
            # No data at all, but might still be a previous page. Scan in the reverse direction to get the first item
            args = dict(self.args)
            args['startkey'] = pageref['startkey']
            args['startkey_docid'] = pageref['startkey_docid']
            if 'endkey' in args:
                del args['endkey']
            args['limit'] = 1
            args['descending'] = not args.get('descending', False)
            revrows = list(self.view_func(self.view_name, **args))
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

