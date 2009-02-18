from zope.interface import Interface
from nevow import inevow, context, rend, loaders, tags as T, url
from twisted.internet import defer
from crux import skin

class PagingDataMixin(object):
    def __init__(self, request, query, page_number, page_size):
        self.request = ctx
        self.query = query
        self.data = []
        self.page_number = int(page_number)
        self.page_size = int(page_size)
        self.page_count = 1

    def set_page_count(self, value):
        self._page_count = value
        if self._page_count <= 0:
            self._page_count = 1

    def get_page_count(self):
        return self._page_count

    page_count = property(get_page_count, set_page_count)

    def set_page_number(self, value):
        try:
            self._page_number = int(value)
        except ValueError:
            self._page_number = 1
        if self._page_number <= 0:
            self._page_number = 1

    def get_page_number(self):
        return self._page_number

    page_number = property(get_page_number, set_page_number)

    def results(self, ctx, data):
        return self.data

    def no_data(self):
        self.page_number = 1
        self.data = []
        self.page_count = 1
        return defer.succeed(self)

    def run_query(self):
        """Provide an implmentation of this"""
        raise NotImplemented("Provide implementation of run_query in PagingDataMixin")
        if self.query is None:
            return self.no_data()
        else:
            return []

def get_integer_from_request(request, key, default):
    value = request.GET.get(key, default)
    try:
        return int(value)
    except ValueError:
        return default

class ListPagingData(PagingDataMixin):

    def __init__(self, request, query, defaultPageSize=10):
        page = get_integer_from_request(request, 'page', 1)
        page_size = self.get_integer_from_request(request, 'page_size')
        PagingDataMixin.__init__(self, request, query, page, page_size)
        self.item_count = 0

    def run_query(self):
        self.item_count = len(self.query)

        page_count = self.item_count/self.page_size
        if self.item_count % self.page_size:
            page_count += 1
        self.page_count = page_count

        if self.page_number > self.page_count:
            self.page_number = self.page_count

        if self.item_count == 0:
            self.no_data()
            return defer.succeed(None)

        start_of_page = (self.page_number-1)*self.page_size
        end_of_page = start_of_page + self.page_size
        self.data = self.query[start_of_page:end_of_page]

        return defer.succeed(True)


class PagingControlsFragment(rend.Fragment):
    """This uses a IPagingData found on the context
       as a source of data. It generates links to the various
       pages in the list.

       It uses a 'page' query parameter to indicate the page of
       data required.
    """

    def __init__(self, template):
        self.docFactory = skin.loader(template, ignoreDocType=True)


    def render_paging(self, ctx, data):
        tag = ctx.tag

        paging_data = ctx.locate(IPagingData)

        if paging_data.page_count == 1:
            return ''
        
        previous_tag = inevow.IQ(tag).onePattern('previous')
        next_tag = inevow.IQ(tag).onePattern('next')

        tag.fillSlots('count','%d items'%paging_data.item_count)

        if paging_data.page_number > 1:
            previous_tag(href=url.URL.fromContext(ctx).replace('page', paging_data.page_number - 1))
            tag.fillSlots('previous',previous_tag)
        else:
            tag.fillSlots('previous','')

        if paging_data.page_number < paging_data.page_count:
            next_tag(href=url.URL.fromContext(ctx).replace('page', paging_data.page_number + 1))
            tag.fillSlots('next',next_tag)
        else:
            tag.fillSlots('next','')

        return tag

    def render_ranges(self,ctx,data):
        tag = ctx.tag
        paging_data = ctx.locate(IPagingData)
        current = inevow.IQ(tag).patternGenerator('range-current')
        not_current = inevow.IQ(tag).patternGenerator('range-not_current')
        page_count = paging_data.page_count
        page_size = paging_data.page_size
        page_number = paging_data.page_number

        page_per_side = 3

        paging_left = T.div(id='paging-range-left')
        paging_center = T.div(id='paging-range-center')
        paging_right = T.div(id='paging-range-right')

        for page in range( page_number-(page_per_side+1), page_number+(page_per_side+1)+1 ):
            position = page-page_number
            start_position = False
            end_position = False

            # Is the position at the start or the end
            if position == -(page_per_side+1):
                start_position = True
            if position  == (page_per_side+1):
                end_position = True

            # is the current slot within the page range
            if page < 1:
                withinRange = False
            elif page > page_count:
                withinRange = False
            else:
                withinRange = True

            # If it's the start or end position and that position
            # is within the page range then add the 'dots
            if start_position and withinRange:
                paging_left[ ' ', T.a(href=url.URL.fromContext(ctx).replace('page', 1))['...'], ' ' ]
                continue
            if end_position and withinRange:
                paging_right[ ' ', T.a(href=url.URL.fromContext(ctx).replace('page', page_count))['...'], ' ' ]
                continue

            if withinRange:

                if position < 0:
                    pattern = not_current()
                    pattern.fillSlots('rangestring',page)
                    pattern.fillSlots('rangeurl',url.URL.fromContext(ctx).replace('page', page))
                    paging_left[ pattern ]

                if position > 0:
                    pattern = not_current()
                    pattern.fillSlots('rangestring',page)
                    pattern.fillSlots('rangeurl',url.URL.fromContext(ctx).replace('page', page))
                    paging_right[ pattern ]

                if position == 0:
                    pattern = current()
                    pattern.fillSlots('rangestring',page)
                    paging_center[ pattern ]

        return tag[ paging_left, paging_center, paging_right ]

