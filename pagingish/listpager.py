class Pager(object):

    def __init__(self, data):
        self.data = list(data)

    def get(self, pagesize, pageref=None):
        # Decode the pageref, defaulting to page 1
        
        if pageref is None:
            pageref = 1
        else:
            pageref = int(pageref)

        # Get some stats
        item_count = len(self.data)
        total_pages = ((item_count-1)//pagesize) +1

        # Discard any extra remaining rows and return the control set.
        if (pageref-1) < 1:
            prevref = None
        else:
            prevref = unicode(pageref-1)

        if (pageref+1) > total_pages:
            nextref = None
        else:
            nextref = unicode(pageref+1)

        # Get stats
        stats = {'page_number': pageref, 'item_count': item_count, 'total_pages': total_pages, 'page_size': pagesize}

        start = pagesize*(pageref-1)
        end = pagesize*pageref

        return {'prev': prevref, 'items': self.data[start:end], 'next': nextref, 'stats': stats}

