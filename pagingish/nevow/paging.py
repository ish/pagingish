from zope.interface import Interface
from nevow import inevow, context, rend, loaders, tags as T, url
from twisted.internet import defer
from crux import skin

class IPagingData(Interface):
    """Marker interface for paging info"""

class PagingDataMixin(object):
    def __init__(self, ctx, query, pageNo, pageSize):
        self.ctx = ctx
        self.query = query
        self.data = []
        self.pageNo = int(pageNo)
        self.pageSize = int(pageSize)
        self.pageCount = 1

    def setPageCount(self, value):
        self._pageCount = value
        if self._pageCount <= 0:
            self._pageCount = 1

    def getPageCount(self):
        return self._pageCount

    pageCount = property(getPageCount, setPageCount)

    def setPageNo(self, value):
        try:
            self._pageNo = int(value)
        except ValueError:
            self._pageNo = 1
        if self._pageNo <= 0:
            self._pageNo = 1

    def getPageNo(self):
        return self._pageNo

    pageNo = property(getPageNo, setPageNo)

    def doQuery(self):
        if self.query is None:
            return self.noData()
        else:
            return self.runQuery()

    def results(self, ctx, data):
        return self.data

    def noData(self):
        self.pageNo = 1
        self.data = []
        self.pageCount = 1
        return defer.succeed(self)

    def runQuery(self):
        """Provide an implmentation of this"""
        return defer.fail(self)

class ListPagingData(PagingDataMixin):
    """Assumes that page and pageSize are query parameters.
       Also that the list to page over is passed as the query parameter.
       This will remember itself as an IPagingData on the page context.

       It will always report one page of data, even if there is no data in the
       list (paging works better, it's difficult to report 'page 0').
    """

    def __init__(self, ctx, query, defaultPageSize=10):
        # This assumes that page and pageSize are query parameters
        pageNo = self.getInt(inevow.IRequest(ctx).args.get('page', ['1'])[0], 1)
        pageSize = self.getInt(inevow.IRequest(ctx).args.get('pageSize', [str(defaultPageSize)])[0], defaultPageSize)
        PagingDataMixin.__init__(self, ctx, query, pageNo, pageSize)
        self.rememberOnPageContext()
        self.itemCount = 0

    def getInt(self, str, default):
        try:
            return int(str)
        except:
            return default

    def rememberOnPageContext(self):
        pageContext = self.findPageContext()
        pageContext.remember(self, IPagingData)

    def findPageContext(self):
        ctx = self.ctx

        lastPageCtx = ctx

        while ctx:
            if isinstance(ctx, context.PageContext):
                lastPageCtx = ctx
            ctx = ctx.parent

        return lastPageCtx

    def runQuery(self):
        # data is in query

        self.itemCount = len(self.query)

        pageCount = self.itemCount/self.pageSize
        if self.itemCount % self.pageSize:
            pageCount += 1
        self.pageCount = pageCount

        if self.pageNo > self.pageCount:
            self.pageNo = self.pageCount

        if self.itemCount == 0:
            self.noData()
            return defer.succeed(None)

        startOfPage = (self.pageNo-1)*self.pageSize
        endOfPage = startOfPage + self.pageSize
        self.data = self.query[startOfPage:endOfPage]

        return defer.succeed(True)


class DeferedListPagingData(ListPagingData):
    """As ListPagingData but the query parameter in place of a list,
       is expected to be a deferred that will return a list of data.
    """

    def runQuery(self):
        # data is in query

        def gotData(data):
            origQuery = self.query
            self.query = data

            def resetQuery(rv, origQuery):
                self.query = origQuery
                return rv

            d = ListPagingData.runQuery(self)
            d.addBoth(resetQuery, origQuery)
            return d

        d = self.query
        d.addCallback(gotData)
        return d

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

        pagingData = ctx.locate(IPagingData)

        if pagingData.pageCount == 1:
            return ''
        
        previousTag = inevow.IQ(tag).onePattern('previous')
        nextTag = inevow.IQ(tag).onePattern('next')

        tag.fillSlots('count','%d items'%pagingData.itemCount)

        if pagingData.pageNo > 1:
            previousTag(href=url.URL.fromContext(ctx).replace('page', pagingData.pageNo - 1))
            tag.fillSlots('previous',previousTag)
        else:
            tag.fillSlots('previous','')

        if pagingData.pageNo < pagingData.pageCount:
            nextTag(href=url.URL.fromContext(ctx).replace('page', pagingData.pageNo + 1))
            tag.fillSlots('next',nextTag)
        else:
            tag.fillSlots('next','')

        return tag

    def render_ranges(self,ctx,data):
        tag = ctx.tag
        pagingData = ctx.locate(IPagingData)
        current = inevow.IQ(tag).patternGenerator('range-current')
        notcurrent = inevow.IQ(tag).patternGenerator('range-notcurrent')
        pageCount = pagingData.pageCount
        pageSize = pagingData.pageSize
        pageNumber = pagingData.pageNo

        pagesPerSide = 3

        pagingLeft = T.div(id='paging-range-left')
        pagingCenter = T.div(id='paging-range-center')
        pagingRight = T.div(id='paging-range-right')

        for page in range( pageNumber-(pagesPerSide+1), pageNumber+(pagesPerSide+1)+1 ):
            position = page-pageNumber
            startPosition = False
            endPosition = False

            # Is the position at the start or the end
            if position == -(pagesPerSide+1):
                startPosition = True
            if position  == (pagesPerSide+1):
                endPosition = True

            # is the current slot within the page range
            if page < 1:
                withinRange = False
            elif page > pageCount:
                withinRange = False
            else:
                withinRange = True

            # If it's the start or end position and that position
            # is within the page range then add the 'dots
            if startPosition and withinRange:
                pagingLeft[ ' ', T.a(href=url.URL.fromContext(ctx).replace('page', 1))['...'], ' ' ]
                continue
            if endPosition and withinRange:
                pagingRight[ ' ', T.a(href=url.URL.fromContext(ctx).replace('page', pageCount))['...'], ' ' ]
                continue

            if withinRange:

                if position < 0:
                    pattern = notcurrent()
                    pattern.fillSlots('rangestring',page)
                    pattern.fillSlots('rangeurl',url.URL.fromContext(ctx).replace('page', page))
                    pagingLeft[ pattern ]

                if position > 0:
                    pattern = notcurrent()
                    pattern.fillSlots('rangestring',page)
                    pattern.fillSlots('rangeurl',url.URL.fromContext(ctx).replace('page', page))
                    pagingRight[ pattern ]

                if position == 0:
                    pattern = current()
                    pattern.fillSlots('rangestring',page)
                    pagingCenter[ pattern ]

        return tag[ pagingLeft, pagingCenter, pagingRight ]

