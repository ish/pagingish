from subprocess import call
import simplejson as json
import couchdb

dbname = 'cms'

S = couchdb.Server('http://localhost:5984')
#del S[dbname]
if 'test' not in S:
    db = S.create(dbname)
else:
    db = S[dbname]

def put(id, data):
    command = ["curl","-X","PUT","http://localhost:5984/%s/%s"%(dbname,id),"-H","Content-Type: application/json","-d",json.dumps(data)]
    retcode = call( command )

chars = 'abcdefghijklmnopqrst'
for n in xrange(20):
    put('xs%s'%n, {'model_type':'page','url':chars[n%10], 'title':chars[n].title()})

