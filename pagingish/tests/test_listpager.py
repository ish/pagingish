from unittest import TestCase

from pagingish.listpager import Pager


def sequence_generator(num):
    return xrange(num)


def assert_page(page, prev, rows, next, stats, expecteds):
    if page > len(expecteds):
        expected = expecteds[-1]
    else:
        expected = expecteds[page-1]


    actual = rows
    print 'page', page
    print 'prev', prev
    print 'next', next
    print 'rows', rows
    print 'expected', expected
    print 'actual', actual
    assert expected == actual
    if page >= len(expecteds):
        assert next is None
    else:
        assert next is not None

    # if the page is <= 1 *or* the page too big and the actuall length of the data is 1. This last may happen if enough items are removed after the previous paging to reduce the number of pages to 1
    if page <= 1 or (page > len(expecteds) and len(expecteds) == 1):
        assert prev is None
    else:
        assert prev is not None

# Test sub sets (i.e. passing startkey endkey)
# zero data
# one item data
# one page?




# 10 items (n per page)
e5pp_10t = [ [0,1,2,3,4], [5,6,7,8,9] ]
e4pp_10t = [ [0,1,2,3], [4,5,6,7], [8,9] ]

def t(d):
    # tuplify paging dict (func sig changed and rewriting everything was going to be painful)
    return  d['prev'], d['items'], d['next'], d['stats']

class TestPager_10items(TestCase):

    def setUp(self):
        self.data = sequence_generator(10)

    def test_roundtrip_5pp(self):
        p = Pager(self.data)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = t(p.get(5, None))
        assert_page(1,prev, rows, next, stats, e5pp_10t)
        prev, rows, next, stats = t(p.get(5, next))
        assert_page(2,prev, rows, next, stats, e5pp_10t)
        prev, rows, next, stats = t(p.get(5, prev))
        assert_page(1,prev, rows, next, stats, e5pp_10t)


    def test_roundtrip_4pp(self):
        p = Pager(self.data)
        # Go forward and back the whole way, 4 per page
        prev, rows, next, stats = t(p.get(4, None))
        assert_page(1,prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = t(p.get(4, next))
        assert_page(2, prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = t(p.get(4, next))
        assert_page(3, prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = t(p.get(4, prev))
        assert_page(2, prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = t(p.get(4, prev))
        assert_page(1, prev, rows, next, stats, e4pp_10t)

    def test_upone_downone_4pp(self):
        p = Pager(self.data)
        # got forward and back one page, 4 per page
        prev, rows, next, stats = t(p.get(4, None))
        assert_page(1, prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = t(p.get(4, next))
        assert_page(2, prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = t(p.get(4, prev))
        assert_page(1, prev, rows, next, stats, e4pp_10t)

    def test_prev_at_start_4pp(self):
        p = Pager(self.data)
        # got forward and back one page, 4 per page
        prev, rows, next, stats = t(p.get(4, None))
        assert_page(1, prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = t(p.get(4, next))
        assert_page(2, prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = t(p.get(4, prev))
        assert_page(1, prev, rows, next, stats, e4pp_10t)
        # Check that using prev again doesn't break
        prev, rows, next, stats = t(p.get(4, prev))
        assert_page(1, prev, rows, next, stats, e4pp_10t)

# 5 items (n per page)
e5pp_5t = [ [0,1,2,3,4] ]
e4pp_5t = [ [0,1,2,3], [4] ]
e6pp_5t = [ [0,1,2,3,4] ]
        
class TestPager_5items(TestCase):

    def setUp(self):
        self.data = sequence_generator(5)


    def test_roundtrip_5pp(self):
        p = Pager(self.data)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = t(p.get(5, None))
        assert_page(1, prev, rows, next, stats, e5pp_5t)

    def test_roundtrip_4pp(self):
        p = Pager(self.data)
        # Go forward and back the whole way, 4 per page
        prev, rows, next, stats = t(p.get(4, None))
        assert_page(1, prev, rows, next, stats, e4pp_5t)
        prev, rows, next, stats = t(p.get(4, next))
        assert_page(2, prev, rows, next, stats, e4pp_5t)
        prev, rows, next, stats = t(p.get(4, prev))
        assert_page(1, prev, rows, next, stats, e4pp_5t)

    def test_prev_at_start_4pp(self):
        p = Pager(self.data)
        print '-----------data',self.data
        # Check that using prev again doesn't break
        prev, rows, next, stats = t(p.get(4, None))
        prev, rows, next, stats = t(p.get(4, next))
        prev, rows, next, stats = t(p.get(4, prev))
        prev, rows, next, stats = t(p.get(4, prev))
        assert_page(1, prev, rows, next, stats, e4pp_5t)
        
    def test_next_at_end_4pp(self):
        p = Pager(self.data)
        # Check that using prev again doesn't break
        prev, rows, next, stats = t(p.get(4, None))
        prev, rows, next, stats = t(p.get(4, next))
        prev, rows, next, stats = t(p.get(4, next))
        assert_page(1, prev, rows, next, stats, e4pp_5t)
       
    def test_roundtrip_6pp(self):
        p = Pager(self.data)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = t(p.get(6, None))
        assert_page(1, prev, rows, next, stats, e6pp_5t)
        prev, rows, next, stats = t(p.get(6, next))
        assert_page(1, prev, rows, next, stats, e6pp_5t)
        prev, rows, next, stats = t(p.get(6, prev))
        assert_page(1, prev, rows, next, stats, e6pp_5t)

    def test_prev_at_start_6pp(self):
        p = Pager(self.data)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = t(p.get(6, None))
        assert_page(1, prev, rows, next, stats, e6pp_5t)
        prev, rows, next, stats = t(p.get(6, prev))
        assert_page(1, prev, rows, next, stats, e6pp_5t)

    def test_next_at_end_6pp(self):
        p = Pager(self.data)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = t(p.get(6, None))
        assert_page(1, prev, rows, next, stats, e6pp_5t)
        prev, rows, next, stats = t(p.get(6, next))
        assert_page(1, prev, rows, next, stats, e6pp_5t)

# 5 items (n per page)
e5pp_1t = [ [0] ]

class TestPager_1items(TestCase):

    def setUp(self):
        self.data = sequence_generator(1)

    def test_roundtrip_5pp(self):
        p = Pager(self.data)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = t(p.get(5, None))
        assert_page(1, prev, rows, next, stats, e5pp_1t)

    def test_next_at_end_5pp(self):
        p = Pager(self.data)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = t(p.get(5, None))
        prev, rows, next, stats = t(p.get(5, next))
        assert_page(1, prev, rows, next, stats, e5pp_1t)

    def test_prev_at_start_5pp(self):
        p = Pager(self.data)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = t(p.get(5, None))
        prev, rows, next, stats = t(p.get(5, prev))
        assert_page(1, prev, rows, next, stats, e5pp_1t)

# 5 items (n per page)
e5pp_0t = [ [] ]

class TestPager_0items(TestCase):

    def setUp(self):
        self.data = sequence_generator(0)

    def test_roundtrip_5pp(self):
        p = Pager(self.data)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = t(p.get(5, None))
        assert_page(1, prev, rows, next, stats, e5pp_0t)

    def test_next_at_end_5pp(self):
        p = Pager(self.data)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = t(p.get(5, None))
        prev, rows, next, stats = t(p.get(5, next))
        assert_page(1, prev, rows, next, stats, e5pp_0t)

    def test_prev_at_start_5pp(self):
        p = Pager(self.data)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = t(p.get(5, None))
        prev, rows, next, stats = t(p.get(5, prev))
        assert_page(1, prev, rows, next, stats, e5pp_0t)

