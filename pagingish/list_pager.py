import simplejson as json

class ListPager(object):

    def __init__(self, data):
        # We can't allow these, we need them to control paging correctly.
        self.data = list(data)

    def get(self, pagesize, pageref=None):

        # Decode the pageref
        if pageref is None:
            pageref = 1
        # Get some stats
        item_count = len(self.data)
        total_pages = ((item_count-1)//pagesize) +1

        # Discard any extra remaining rows and return the control set.
        if (pageref-1) < 1:
            prevref = None
        else:
            prevref = pageref-1

        if (pageref+1) > total_pages:
            nextref = None
        else:
            nextref = pageref+1

        # Get stats
        stats = {'item_count': item_count, 'total_pages': total_pages}

        start = pagesize*(pageref-1)
        end = pagesize*pageref

        return prevref, self.data[start:end], nextref, stats 





