


def get_integer_from_request(request, key, default):
    value = request.GET.get(key, default)
    try:
        return int(value)
    except ValueError:
        return default

class ListPaging(object):

    def __init__(self, request, data, default_page_size=10):

        page_number = get_integer_from_request(request, 'page_number', 1)
        page_size = self.get_integer_from_request(request, 'page_size', default_page_size)

        item_count = len(data)

        page_count = item_count/page_size
        if item_count % page_size:
            page_count += 1

        if page_number > page_count:
            page_number = page_count

        start_of_page = (page_number-1)*page_size
        end_of_page = start_of_page + page_size
        docs = data[start_of_page:end_of_page]

        next = {'page_number': page_number+1}
        prev = {'page_number': page_number-1}

        self.item_count = item_count
        self.docs = docs
        self.next = next
        self.prev = prev

class CouchDBPaging(object):

    def __init__(self, request, couchish_store, view, default_page_size=10, count_view=None)

        page_number = get_integer_from_request(request, 'page_number', None)
        startkey_docid = request.GET.get('startkey_docid')
        page_size = self.get_integer_from_request(request, 'page_size', default_page_size)

        with couchish_store.session() as S:
            doc_rows = list(S.view(view, startkey_docid=startkey_docid, limit=page_size+1))
            revdoc_rows = list(S.view(view, startkey_docid=startkey_docid, limit=page_size+1, descending=True))

        docs = [row.doc for row in doc_rows]
        next = {'startkey_docid': doc_rows[-1].id}
        prev = {'startkey_docid': revdoc_rows[-1].id}
        if page_number is not None:
            next['page_number'] = page_number + 1
            prev['page_number'] = page_number - 1

        item_count = None
        if count_view is not None:
            with couchish_store.session() as S:
                result = S.view(self.count_view)
            item_count = list(result)[0].value

        if item_count is not None:
            total_pages =item_count / page_size

        self.item_count = item_count
        self.docs = docs
        self.next = next
        self.prev = prev

           
class CouchDBPagingSkipLimit(object):

    def __init__(self, request, couchish_store, view, count_view, default_page_size=10):

        page_number = get_integer_from_request(request, 'page_number', 1)
        page_size = self.get_integer_from_request(request, 'page_size', default_page_size)

        # Work out the total count if a view is available
        with couchish_store.session() as S:
            result = S.view(self.count_view)
        item_count = list(result)[0].value

        page_count = item_count/page_size

        if item_count % self.page_size:
            page_count += 1
        if page_number > page_count:
            page_number = page_count
        
        skip = (page_number-1)*page_size
        with couchish_store.session() as S:
            docs = S.view(view, skip=skip, limit=page_size)
        self.docs = [row.doc for row in docs]

        next = {'page_number': page_number+1}
        prev = {'page_number': page_number-1}

        self.item_count = item_count
        self.docs = docs
        self.next = next
        self.prev = prev




