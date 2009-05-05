import base64
import simplejson as json


class Pager(object):

    @classmethod
    def jumpref(cls, startkey):
        """
        Create a pageref that will "jump" into the middle of a view but will
        not affect the view results or further paging. Simply Pass the
        resulting string as the pageref when calling get().

        This is primarily useful for providing an index into a view, e.g. an
        '0-9', 'A', 'B', ..., 'Z' index into a list of names although any value
        jump key can be used.
        """
        return _encode_list(['jump', _encode_key(startkey)])

    def __init__(self, view_func, view_name, view_args=None):
        if view_args is None:
            view_args = {}
        # We can't allow these, we need them to control paging correctly.
        assert 'limit' not in view_args
        assert 'startkey_docid' not in view_args
        assert 'endkey_docid' not in view_args
        self.view_func = view_func
        self.view_name = view_name
        self.view_args = view_args

    def get(self, pagesize, pageref=None):

        # Decode the pageref
        pageref = _decode_ref(pageref)

        # Turn a "jump" ref into a "next" ref by looking for the document
        # before (in whatever direction) the jump ref key and creating the
        # pageref from it.
        if pageref and pageref['type'] == 'jump':
            pageref = self._resolve_jump_ref(pageref)

        # Copy the view_args and replace/update with paging control view_args.
        view_args = dict(self.view_args)
        if pageref:
            if pageref['type'] == 'prev':
                # if we're going in the opposite to normal direction, reverse
                # the data scan direction and use endkey instead of startkey
                view_args['descending'] = not self.view_args.get('descending', False)
                if 'startkey' in self.view_args:
                    view_args['endkey'] = self.view_args.get('startkey')


            if 'startkey' in pageref:
                # if we have a startkey/startkey_docid in the pageref, then use it 
                view_args['startkey'] = pageref['startkey']
                if 'startkey_docid' in pageref:
                    view_args['startkey_docid'] = pageref['startkey_docid']
            else:
                # else this is the special hack case where we are taking the
                # last page of data by reversing the normal search
                if 'endkey' in self.view_args:
                    view_args['startkey'] = self.view_args['endkey']
                if 'startkey' in self.view_args:
                    view_args['endkey'] = self.view_args['startkey']
                view_args['descending'] = not self.view_args.get('descending', False)

        # Try to get two extra rows to detect previous and next pages as
        # efficiently as possible.
        view_args['limit'] = pagesize+2

        # Fetch the rows from the view_name.
        rows = list(self.view_func(self.view_name, **view_args))

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
            if pageref['type'] == 'next':
                prevref = _ref_from_doc('prev',rows[1])
            else:
                nextref = _ref_from_doc('next',rows[1])
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
            if pageref['type'] == 'next':
                prevref = {'type': 'prev'}
            else:
                nextref = {'type': 'next'}
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
            view_args = dict(self.view_args)
            # If we're scanning backwards then the endkey must be swapped for the start key
            if 'startkey' in view_args:
                view_args['endkey'] = view_args['startkey']

            view_args['startkey'] = revref.key
            view_args['startkey_docid'] = revref.id
            view_args['limit'] = 2
            view_args['descending'] = not view_args.get('descending', False)
            revrows = list(self.view_func(self.view_name, **view_args))
            if len(revrows) >= 2:
                if 'startkey' in pageref:
                    if pageref['type'] == 'next':
                        prevref = _ref_from_doc('prev',revref)
                    else:
                        nextref = _ref_from_doc('next',revref)
                else:
                    if pageref['type'] == 'prev':
                        prevref = _ref_from_doc('prev',revref)
                    else:
                        nextref = _ref_from_doc('next',revref)

        elif pageref and not rows:
            # no rows !!

            # No data at all, but there might still be a previous page. 
            # Scan in the reverse direction to get the first item (clearing any
            # endkey to make sure we don't cut off our own results)
            view_args = dict(self.view_args)
            view_args['startkey'] = pageref['startkey']
            view_args['startkey_docid'] = pageref['startkey_docid']
            if 'endkey' in view_args:
                del view_args['endkey']
            view_args['limit'] = 1
            view_args['descending'] = not view_args.get('descending', False)
            revrows = list(self.view_func(self.view_name, **view_args))
            # We have the same issue as in the 'only one row and pagref match'
            # case. We don't have a first item on the current page that we can
            # use as the start point for a previous page. Hence we do the hack
            # which tells the pager to use the last page of results in this
            # special case
            if len(revrows) >= 1:
                if pageref['type'] == 'next':
                    prevref = {'type': 'prev'}
                else:
                    nextref = {'type': 'next'}

        # Find the ref document to move a page in the same direction as the
        # pageref's direction (default is 'next').
        if len(rows) > pagesize:
            if pageref and pageref['type'] == 'prev':
                prevref = _ref_from_doc('prev',rows[pagesize-1])
            else:
                nextref = _ref_from_doc('next',rows[pagesize-1])


        # Discard any extra remaining rows and return the control set.
        rows = rows[:pagesize]

        # Reverse the remaining rows if we're going backwards.
        if pageref and pageref['type'] == 'prev':
            rows = rows[::-1]

        return {'prev': _encode_ref(prevref), 'items': rows, 'next': _encode_ref(nextref), 'stats': {'page_size':pagesize}}

    def _resolve_jump_ref(self, jumpref):
        """
        Turn a jumpref into either a "next" pageref or None.

        XXX The while loop can be removed once non-inclusive start keys are
        supported in CouchDB.
        """
        view_args = dict(self.view_args)
        view_args['descending'] = not view_args.get('descending', False)
        view_args['startkey'] = jumpref['startkey']
        view_args['limit'] = 5
        while True:
            rows = list(self.view_func(self.view_name, **view_args))
            if not rows or (len(rows) == 1 and rows[0].key == jumpref['startkey']):
                break
            for row in rows:
                if row.key != jumpref['startkey']:
                    return _ref_from_doc('next', row)
            view_args['startkey'] = row.key
            view_args['startkey_docid'] = row.id


def _ref_from_doc(dir,doc):
    return {'type': dir, 'startkey': doc.key, 'startkey_docid': doc.id}


def _encode_ref(ref):
    if ref is None:
        return None
    if 'startkey' not in ref:
        return _encode_list([ref['type']])
    return _encode_list([ref['type'], _encode_key(ref['startkey']), ref['startkey_docid']])


def _decode_ref(ref):
    if ref is None:
        return None
    ref = _decode_list(ref)
    if ref[0] == 'jump':
        return dict(zip(['type', 'startkey'], [ref[0], _decode_key(ref[1])]))
    else:
        d = dict(zip(['type', 'startkey', 'startkey_docid'], ref))
        if 'startkey' in d:
            d['startkey'] = _decode_key(d['startkey'])
        return d


def _encode_list(l, delimeter='|'):
    return delimeter.join(i.replace(delimeter, '\\'+delimeter) for i in l)


def _decode_list(s, delimeter='|'):
    result = []
    for bit in s.split('\\'+delimeter):
        if delimeter not in bit:
            if result:
                result[-1] = delimeter.join([result[-1], bit])
            else:
                result.append(bit)
        else:
            for idx, i in enumerate(bit.split(delimeter)):
                if idx == 0:
                    if result:
                        result[-1] = delimeter.join([result[-1], i])
                    else:
                        result.append(i)
                else:
                    result.append(i)
    return result


def _encode_key(key):
    return base64.b64encode(json.dumps(key))


def _decode_key(s):
    return json.loads(base64.b64decode(s))


class SkipLimitPager(object):

    def __init__(self, view_func, view_name, count_view_name, view_args=None):
        if view_args is None:
            view_args = {}
        # We can't allow these, we need them to control paging correctly.
        assert 'limit' not in view_args
        assert 'startkey_docid' not in view_args
        assert 'endkey_docid' not in view_args
        self.view_func = view_func
        self.view_name = view_name
        self.count_view_name = count_view_name
        self.view_args = view_args

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
        docs = list(self.view_func(self.view_name, skip=skip, limit=pagesize, **self.view_args))

        # next and prev pagerefs (None for not available)
        nextref = None
        prevref = None
        if page_number < total_pages:
            nextref = str(page_number+1)
        if page_number >1:
            prevref = str(page_number-1)

        stats = {'page_number': page_number,
                 'total_pages': total_pages,
                 'item_count': item_count,
                 'page_size': pagesize}

        return {'prev':prevref, 'items': docs, 'next': nextref, 'stats': stats}

