import sqlite3
import csv
from pprint import pprint

sqlite_file = 'sanjose.db'

conn = sqlite3.connect(sqlite_file)

cur = conn.cursor()

cur.execute('''DROP TABLE IF EXISTS nodes;''')
conn.commit()

cur.execute('''
            CREATE TABLE nodes(id INTEGER PRIMARY KEY, user TEXT, uid INTEGER, version TEXT,lat REAL,lon REAL,timestamp DATE,changeset INTEGER) 
	    ''')

conn.commit()

with open('nodes.csv','rb') as fin:
    dr = csv.DictReader(fin)
    to_db = [(i['id'],i['user'].decode("utf-8"),i['uid'],i['version'],i['lat'],i['lon'],i['timestamp'],i['changeset']) for i in dr]

cur.executemany("INSERT INTO nodes(id,user,uid,version,lat,lon,timestamp,changeset) VALUES(?,?,?,?,?,?,?,?);",to_db)

conn.commit()

cur.execute('''
            CREATE TABLE nodes_tags(id INTEGER, key TEXT, value TEXT, type TEXT) 
	    ''')

conn.commit()

with open('nodes_tags.csv','rb') as fin:
    dr = csv.DictReader(fin)
    to_db = [(i['id'],i['key'],i['value'].decode("utf-8"),i['type']) for i in dr]

cur.executemany("INSERT INTO nodes_tags(id,key,value,type) VALUES(?,?,?,?);",to_db)

conn.commit()

cur.execute('''
            CREATE TABLE ways(id INTEGER PRIMARY KEY, user TEXT,uid INTEGER,version TEXT,timestamp DATE,changeset INTEGER) 
	    ''')

conn.commit()

with open('ways.csv','rb') as fin:
    dr = csv.DictReader(fin)
    to_db = [(i['id'],i['user'].decode("utf-8"),i['uid'],i['version'],i['timestamp'],i['changeset']) for i in dr]

cur.executemany("INSERT INTO ways(id,user,uid,version,timestamp,changeset) VALUES(?,?,?,?,?,?);",to_db)

conn.commit()

cur.execute('''
            CREATE TABLE ways_tags(id INTEGER, key TEXT, value TEXT, type TEXT) 
	    ''')

conn.commit()

with open('ways_tags.csv','rb') as fin:
    dr = csv.DictReader(fin)
    to_db = [(i['id'],i['key'],i['value'].decode("utf-8"),i['type']) for i in dr]

cur.executemany("INSERT INTO ways_tags(id,key,value,type) VALUES(?,?,?,?);",to_db)

conn.commit()

cur.execute('''
            CREATE TABLE ways_nodes(id INTEGER, node_id INTEGER, position INTEGER) 
	    ''')

conn.commit()

with open('ways_nodes.csv','rb') as fin:
    dr = csv.DictReader(fin)
    to_db = [(i['id'],i['node_id'],i['position']) for i in dr]

cur.executemany("INSERT INTO ways_nodes(id,node_id,position) VALUES(?,?,?);",to_db)

conn.commit()


cur.execute('SELECT * FROM nodes')
all_rows = cur.fetchall()
print ('1):')
pprint(all_rows)

conn.close()
