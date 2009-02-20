from unittest import TestCase
from createdata import create_items
import couchdb
from couchdb.design import ViewDefinition

from pagingish.couchdb_pager import CouchDBViewPager

dbname = 'test-paging'
model_type = 'test'

def create_view(db, design_doc, name, map_fun, reduce_fun=None):
    view = ViewDefinition(design_doc, name, map_fun, reduce_fun)
    view.get_doc(db)
    view.sync(db)

def get_pagenumber(pagesize, itemcount):
    if item_count == 0:
        return 1
    return 1+ (itemcount-1)/pagesize

def sequence_generator(num):
    for n in xrange(num):
        data = {'_id': 'id-%s'%n,
                'model_type': model_type,
                'num': n}
        yield data




def assert_page(page, prev, rows, next, stats, expecteds):
    if page > len(expecteds):
        expected = expecteds[-1]
    else:
        expected = expecteds[page-1]


    actual = [r.key for r in rows]
    print '----------------------'
    print 'expecteds', expecteds
    print 'page', page
    print 'prev', prev
    print 'next', next
    print 'rows', rows
    print 'expected', expected
    print 'actual', actual
    assert expected == actual
    if page >= len(expecteds) or (page < len(expecteds) and len(expecteds) == 1):
        print 'assert next is None'
        assert next is None
    else:
        print 'assert next is not None'
        assert next is not None

    # if the page is <= 1 *or* the page too big and the actuall length of the data is 1. This last may happen if enough items are removed after the previous paging to reduce the number of pages to 1
    if page <= 1 or (page > len(expecteds) and len(expecteds) == 1):
        print 'assert prev is None'
        assert prev is None
    else:
        print 'assert prev is not None'
        assert prev is not None

# Test sub sets (i.e. passing startkey endkey)
# zero data
# one item data
# one page?




# 10 items (n per page)
e5pp_10t = [ [0,1,2,3,4], [5,6,7,8,9] ]
e4pp_10t = [ [0,1,2,3], [4,5,6,7], [8,9] ]

class TestCouchDBPager_10items(TestCase):

    def setUp(self):
        self.db = create_items(dbname,force_create=True, model_type=model_type, items=sequence_generator(10))
        map_fun = 'function(doc) { if (doc.model_type == "%s") { emit(doc.num, 1); } }'%model_type
        reduce_fun = 'function(keys, values) { return sum(values) }'
        create_view(self.db, model_type,'all',map_fun)
        create_view(self.db, model_type,'count',map_fun, reduce_fun)

    def test_roundtrip_5pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = p.get(5, None)
        assert_page(1,prev, rows, next, stats, e5pp_10t)
        prev, rows, next, stats = p.get(5, next)
        assert_page(2,prev, rows, next, stats, e5pp_10t)
        prev, rows, next, stats = p.get(5, prev)
        assert_page(1,prev, rows, next, stats, e5pp_10t)


    def test_roundtrip_4pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # Go forward and back the whole way, 4 per page
        prev, rows, next, stats = p.get(4, None)
        assert_page(1,prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = p.get(4, next)
        assert_page(2, prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = p.get(4, next)
        assert_page(3, prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = p.get(4, prev)
        assert_page(2, prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = p.get(4, prev)
        assert_page(1, prev, rows, next, stats, e4pp_10t)

    def test_upone_downone_4pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # got forward and back one page, 4 per page
        prev, rows, next, stats = p.get(4, None)
        assert_page(1, prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = p.get(4, next)
        assert_page(2, prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = p.get(4, prev)
        assert_page(1, prev, rows, next, stats, e4pp_10t)

    def test_prev_at_start_4pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # got forward and back one page, 4 per page
        prev, rows, next, stats = p.get(4, None)
        assert_page(1, prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = p.get(4, next)
        assert_page(2, prev, rows, next, stats, e4pp_10t)
        prev, rows, next, stats = p.get(4, prev)
        assert_page(1, prev, rows, next, stats, e4pp_10t)
        # Check that using prev again doesn't break
        prev, rows, next, stats = p.get(4, prev)
        assert_page(1, prev, rows, next, stats, e4pp_10t)

# 5 items (n per page)
e5pp_5t = [ [0,1,2,3,4] ]
e4pp_5t = [ [0,1,2,3], [4] ]
e6pp_5t = [ [0,1,2,3,4] ]
        
class TestCouchDBPager_5items(TestCase):

    def setUp(self):
        self.db = create_items(dbname,force_create=True, model_type=model_type, items=sequence_generator(5))
        map_fun = 'function(doc) { if (doc.model_type == "%s") { emit(doc.num, 1); } }'%model_type
        reduce_fun = 'function(keys, values) { return sum(values) }'
        create_view(self.db, model_type,'all',map_fun)
        create_view(self.db, model_type,'count',map_fun, reduce_fun)

    def test_roundtrip_5pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = p.get(5, None)
        assert_page(1, prev, rows, next, stats, e5pp_5t)

    def test_roundtrip_4pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # Go forward and back the whole way, 4 per page
        prev, rows, next, stats = p.get(4, None)
        assert_page(1, prev, rows, next, stats, e4pp_5t)
        prev, rows, next, stats = p.get(4, next)
        assert_page(2, prev, rows, next, stats, e4pp_5t)
        prev, rows, next, stats = p.get(4, prev)
        assert_page(1, prev, rows, next, stats, e4pp_5t)

    def test_prev_at_start_4pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # Check that using prev again doesn't break
        prev, rows, next, stats = p.get(4, None)
        prev, rows, next, stats = p.get(4, next)
        prev, rows, next, stats = p.get(4, prev)
        prev, rows, next, stats = p.get(4, prev)
        assert_page(1, prev, rows, next, stats, e4pp_5t)
        
    def test_next_at_end_4pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # Check that using prev again doesn't break
        prev, rows, next, stats = p.get(4, None)
        prev, rows, next, stats = p.get(4, next)
        prev, rows, next, stats = p.get(4, next)
        assert_page(1, prev, rows, next, stats, e4pp_5t)
       
    def test_roundtrip_6pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = p.get(6, None)
        assert_page(1, prev, rows, next, stats, e6pp_5t)
        prev, rows, next, stats = p.get(6, next)
        assert_page(1, prev, rows, next, stats, e6pp_5t)
        prev, rows, next, stats = p.get(6, prev)
        assert_page(1, prev, rows, next, stats, e6pp_5t)

    def test_prev_at_start_6pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = p.get(6, None)
        assert_page(1, prev, rows, next, stats, e6pp_5t)
        prev, rows, next, stats = p.get(6, prev)
        assert_page(1, prev, rows, next, stats, e6pp_5t)

    def test_next_at_end_6pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = p.get(6, None)
        assert_page(1, prev, rows, next, stats, e6pp_5t)
        prev, rows, next, stats = p.get(6, next)
        assert_page(1, prev, rows, next, stats, e6pp_5t)

# 5 items (n per page)
e5pp_1t = [ [0] ]

class TestCouchDBPager_1items(TestCase):

    def setUp(self):
        self.db = create_items(dbname,force_create=True, model_type=model_type, items=sequence_generator(1))
        map_fun = 'function(doc) { if (doc.model_type == "%s") { emit(doc.num, 1); } }'%model_type
        reduce_fun = 'function(keys, values) { return sum(values) }'
        create_view(self.db, model_type,'all',map_fun)
        create_view(self.db, model_type,'count',map_fun, reduce_fun)

    def test_roundtrip_5pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = p.get(5, None)
        assert_page(1, prev, rows, next, stats, e5pp_1t)

    def test_next_at_end_5pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = p.get(5, None)
        prev, rows, next, stats = p.get(5, next)
        assert_page(1, prev, rows, next, stats, e5pp_1t)

    def test_prev_at_start_5pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = p.get(5, None)
        prev, rows, next, stats = p.get(5, prev)
        assert_page(1, prev, rows, next, stats, e5pp_1t)

# 5 items (n per page)
e5pp_0t = [ [] ]

class TestCouchDBPager_0items(TestCase):

    def setUp(self):
        self.db = create_items(dbname,force_create=True, model_type=model_type, items=sequence_generator(0))
        map_fun = 'function(doc) { if (doc.model_type == "%s") { emit(doc.num, 1); } }'%model_type
        reduce_fun = 'function(keys, values) { return sum(values) }'
        create_view(self.db, model_type,'all',map_fun)
        create_view(self.db, model_type,'count',map_fun, reduce_fun)

    def test_roundtrip_5pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = p.get(5, None)
        assert_page(1, prev, rows, next, stats, e5pp_0t)

    def test_next_at_end_5pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = p.get(5, None)
        prev, rows, next, stats = p.get(5, next)
        assert_page(1, prev, rows, next, stats, e5pp_0t)

    def test_prev_at_start_5pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        # Go forward and back the whole way, 5 per page
        prev, rows, next, stats = p.get(5, None)
        prev, rows, next, stats = p.get(5, prev)
        assert_page(1, prev, rows, next, stats, e5pp_0t)

# 10 items (n per page)

class TestCouchDBPager_alterlist_10items(TestCase):

    def setUp(self):
        self.db = create_items(dbname,force_create=True, model_type=model_type, items=sequence_generator(10))
        map_fun = 'function(doc) { if (doc.model_type == "%s") { emit(doc.num, 1); } }'%model_type
        reduce_fun = 'function(keys, values) { return sum(values) }'
        create_view(self.db, model_type,'all',map_fun)
        create_view(self.db, model_type,'count',map_fun, reduce_fun)

    def test_remove_prevref(self):
        e5pp_10t_before = [ [0,1,2,3,4], [5,6,7,8,9] ]
        e5pp_10t_after = [ [0,1,2,3], [5,6,7,8,9] ]
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        prev, rows, next, stats = p.get(5, None)
        assert_page(1, prev, rows, next, stats, e5pp_10t_before)
        del self.db['id-4']
        prev, rows, next, stats = p.get(5, next)
        assert_page(2, prev, rows, next, stats, e5pp_10t_after)

    def test_remove_prevref_reverse(self):
        e5pp_10t_before = [ [9,8,7,6,5], [4,3,2,1,0] ]
        e5pp_10t_after = [ [9,8,7,6], [4,3,2,1,0] ]
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type, descending=True)
        prev, rows, next, stats = p.get(5, None)
        assert_page(1, prev, rows, next, stats, e5pp_10t_before)
        del self.db['id-5']
        prev, rows, next, stats = p.get(5, next)
        assert_page(2, prev, rows, next, stats, e5pp_10t_after)

    def test_remove_whole_next_page(self):
        e5pp_10t_before = [ [0,1,2,3,4], [5,6,7,8,9] ]
        e5pp_10t_after = [ [0,1,2,3,4], [] ]
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        prev, rows, next, stats = p.get(5, None)
        assert_page(1, prev, rows, next, stats, e5pp_10t_before)
        for i in xrange(5,10):
            del self.db['id-%s'%i]
        prev, rows, next, stats = p.get(5, next)
        assert_page(2, prev, rows, next, stats, e5pp_10t_after)
        prev, rows, next, stats = p.get(5, prev)
        assert_page(1, prev, rows, next, stats, e5pp_10t_after)

    def test_remove_whole_next_page_reverse(self):
        e5pp_10t_before = [ [9,8,7,6,5], [4,3,2,1,0] ]
        e5pp_10t_after = [ [9,8,7,6,5], [] ]
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type, descending=True)
        prev, rows, next, stats = p.get(5, None)
        assert_page(1, prev, rows, next, stats, e5pp_10t_before)
        for i in xrange(0,5):
            print 'deleting id-%s'%i
            del self.db['id-%s'%i]
        prev, rows, next, stats = p.get(5, next)
        assert_page(2, prev, rows, next, stats, e5pp_10t_after)
        prev, rows, next, stats = p.get(5, prev)
        assert_page(1, prev, rows, next, stats, e5pp_10t_after)
        prev, rows, next, stats = p.get(5, next)
        assert_page(2, prev, rows, next, stats, e5pp_10t_after)


    def test_remove_alldata(self):
        e5pp_10t_before = [ [0,1,2,3,4], [5,6,7,8,9] ]
        e5pp_10t_after = [ [] ]
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        prev, rows, next, stats = p.get(5, None)
        assert_page(1, prev, rows, next, stats, e5pp_10t_before)
        for i in xrange(0,10):
            print 'deleting id-%s'%i
            del self.db['id-%s'%i]
        prev, rows, next, stats = p.get(5, next)
        assert_page(2, prev, rows, next, stats, e5pp_10t_after)
        prev, rows, next, stats = p.get(5, prev)
        assert_page(1, prev, rows, next, stats, e5pp_10t_after)

    def test_remove_alldata_reversed(self):
        e5pp_10t_before = [ [9,8,7,6,5], [4,3,2,1,0] ]
        e5pp_10t_after = [ [] ]
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type, descending=True)
        prev, rows, next, stats = p.get(5, None)
        assert_page(1, prev, rows, next, stats, e5pp_10t_before)
        for i in xrange(0,10):
            print 'deleting id-%s'%i
            del self.db['id-%s'%i]
        prev, rows, next, stats = p.get(5, next)
        assert_page(2, prev, rows, next, stats, e5pp_10t_after)

# 15 items (n per page)

class TestCouchDBPager_alterlist_15items(TestCase):

    def setUp(self):
        self.db = create_items(dbname,force_create=True, model_type=model_type, items=sequence_generator(15))
        map_fun = 'function(doc) { if (doc.model_type == "%s") { emit(doc.num, 1); } }'%model_type
        reduce_fun = 'function(keys, values) { return sum(values) }'
        create_view(self.db, model_type,'all',map_fun)
        create_view(self.db, model_type,'count',map_fun, reduce_fun)

    def test_remove_prevref(self):
        e5pp_15t_before = [ [0,1,2,3,4], [5,6,7,8,9], [10,11,12,13,14] ]
        e5pp_15t_after = [ [0,1,2,3], [5,6,7,8,9], [10,11,12,13,14] ]
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        prev, rows, next, stats = p.get(5, None)
        assert_page(1, prev, rows, next, stats, e5pp_15t_before)
        del self.db['id-4']
        prev, rows, next, stats = p.get(5, next)
        assert_page(2, prev, rows, next, stats, e5pp_15t_after)


    def test_remove_prevrefandfirst(self):
        e5pp_15t_before = [ [0,1,2,3,4], [5,6,7,8,9], [10,11,12,13,14] ]
        e5pp_15t_after = [ [0,1,2,3], [6,7,8,9,10], [11,12,13,14] ]
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        prev, rows, next, stats = p.get(5, None)
        assert_page(1, prev, rows, next, stats, e5pp_15t_before)
        del self.db['id-4']
        del self.db['id-5']
        prev, rows, next, stats = p.get(5, next)
        assert_page(2, prev, rows, next, stats, e5pp_15t_after)

    def test_remove_prevrefandfirst_thenprev(self):
        e5pp_15t_before = [ [0,1,2,3,4], [5,6,7,8,9], [10,11,12,13,14] ]
        e5pp_15t_after = [ [0,1,2,3], [6,7,8,9,10], [11,12,13,14] ]
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type)
        prev, rows, next, stats = p.get(5, None)
        assert_page(1, prev, rows, next, stats, e5pp_15t_before)
        del self.db['id-4']
        del self.db['id-5']
        prev, rows, next, stats = p.get(5, next)
        assert_page(2, prev, rows, next, stats, e5pp_15t_after)
        prev, rows, next, stats = p.get(5, prev)
        assert_page(1, prev, rows, next, stats, e5pp_15t_after)


# 10 items (n per page)

class TestCouchDBPager_10items_withstartend(TestCase):

    def setUp(self):
        self.db = create_items(dbname,force_create=True, model_type=model_type, items=sequence_generator(20))
        map_fun = 'function(doc) { if (doc.model_type == "%s") { emit(doc.num, 1); } }'%model_type
        reduce_fun = 'function(keys, values) { return sum(values) }'
        create_view(self.db, model_type,'all',map_fun)
        create_view(self.db, model_type,'count',map_fun, reduce_fun)

    def test_roundtrip_5pp(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type, startkey=5, endkey=14)
        expecteds = [ [5,6,7,8,9],[10,11,12,13,14] ]
        prev, rows, next, stats = p.get(5, None)
        assert_page(1,prev, rows, next, stats, expecteds)
        prev, rows, next, stats = p.get(5, next)
        assert_page(2,prev, rows, next, stats, expecteds)
        prev, rows, next, stats = p.get(5, prev)
        assert_page(1,prev, rows, next, stats, expecteds)

    def test_roundtrip_5pp_reversed(self):
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type, startkey=14, endkey=5, descending=True)
        expecteds = [ [14,13,12,11,10],[9,8,7,6,5] ]
        prev, rows, next, stats = p.get(5, None)
        assert_page(1,prev, rows, next, stats, expecteds)
        prev, rows, next, stats = p.get(5, next)
        assert_page(2,prev, rows, next, stats, expecteds)
        prev, rows, next, stats = p.get(5, prev)
        assert_page(1,prev, rows, next, stats, expecteds)


# 15 items (n per page)

class TestCouchDBPager_alterlist_15items_withstartend(TestCase):

    def setUp(self):
        self.db = create_items(dbname,force_create=True, model_type=model_type, items=sequence_generator(15))
        map_fun = 'function(doc) { if (doc.model_type == "%s") { emit(doc.num, 1); } }'%model_type
        reduce_fun = 'function(keys, values) { return sum(values) }'
        create_view(self.db, model_type,'all',map_fun)
        create_view(self.db, model_type,'count',map_fun, reduce_fun)

    def test_remove_prevref(self):
        e5pp_15t_before = [ [2,3,4,5,6], [7,8,9,10,11], [12] ]
        e5pp_15t_after = [ [2,3,4,5], [7,8,9,10,11], [12] ]
        p = CouchDBViewPager(self.db.view, '%s/all'%model_type, startkey=2, endkey=12)
        prev, rows, next, stats = p.get(5, None)
        assert_page(1, prev, rows, next, stats, e5pp_15t_before)
        del self.db['id-6']
        prev, rows, next, stats = p.get(5, next)
        assert_page(2, prev, rows, next, stats, e5pp_15t_after)


