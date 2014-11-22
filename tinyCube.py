# The TinyCube module that receives query from the user client and returns "approximate" answer (As a proof of concept,
# will return accurate answer for simplicity)

# Global Library
import MySQLdb
import os.path

# Local library
import queryRewriter as rewriter
import trainer as t
from Cube import Cube


# Constants
_host_name = "localhost";
_user_name = "root";
_db_name = "tinyCube";
_relation_name = "TestRelation";
_cmd_train = "TRAIN";

# Files
_default_workload = "./config/workload.dat";

# Global variable
relations = [];
aggregates = [];
attributes = dict();
attr_partition = dict();
cube_partition = dict();
cubes = dict();

def tinyCube(query):
	# Check if query is for Training
	check = query.split(' ');
	if check[0] == _cmd_train:
		train_set = [];
		filename = check[1];
		if os.path.exists(filename):
			f = open(filename, 'r');
			for line in f:
				line = line.replace("\n","");
				train_set.append(line);
		else:
			f = open(_default_workload, 'r');
			for line in f:
				line = line.replace("\n","");
				train_set.append(line);

		t.train(train_set,False,cube_partition,attr_partition);

		#print(attr_partition);
		#print(cube_partition);
		
		# Construct Cubes
		for key,value in cube_partition.iteritems():
			cubes[key] = Cube(key,attr_partition[key],cube_partition[key]);
						
	# Answer query
	else:
		#print('sf');
		sql_query = check[1:];			 
		relation = sql_query[sql_query.index("FROM")+1];
		aggregate = sql_query[sql_query.index("SELECT")+1];
		predicate = sql_query[sql_query.index("WHERE")+1:];
		mycube_key = aggregate+'@'+relation;
		mycube = cubes[mycube_key];
		mycube.answerQuery(predicate,cursor);

# Connect to DBMS (MySQL)
db = MySQLdb.connect(host=_host_name,user=_user_name,db=_db_name);
cursor = db.cursor()

# Find all relations and attributes within a database
cursor.execute("SHOW TABLES");
data = cursor.fetchone();
for relation in data:
	relations.append(relation);
	cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '"+relation+"'");
	data = cursor.fetchall();
	att_list = [];
	for att in data:
			att_list.append(att[0]);
	attributes[relation] = att_list;

tinyCube('TRAIN ./config/default_workload.dat');
tinyCube('ANSWER SELECT SUM(A) FROM T WHERE B >= 15');
