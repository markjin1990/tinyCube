# The TinyCube module that receives query from the user client and returns "approximate" answer (As a proof of concept,
# will return accurate answer for simplicity)

# Global Library
import mysql.connector
import os.path

# Local library
import queryRewriter as rewriter
from trainer import Trainer
from Cube import Cube
import resource

# Constants
_host_name = "localhost";
_user_name = "root";
_db_name = "tpch";
_cmd_train = "TRAIN";
_cmd_answer = "ANSWER";
_cmd_getNumOfCache = "CACHE#";

# Files
_default_workload = "./config/workload.dat";

# Global variable
relations = [];
aggregates = [];
attributes = dict();
attr_types = dict();
attr_partition = dict();
cube_partition = dict();
cubes = dict();

def tinyCube(query,ifTinyCube,ifRepartition,idx=0):
	# Check if query is for Training
	check = query.split(' ');
	if check[0] == _cmd_train:
		train_set = [];
		filename = check[1];
		if os.path.exists(filename):
			f = open(filename, 'r');
			for line in f:
				line = line.replace("\n","");
				line = line.replace(";","");
				train_set.append(line);
		else:
			f = open(_default_workload, 'r');
			for line in f:
				line = line.replace("\n","");
				line = line.replace(";","");
				train_set.append(line);

		t = Trainer(attr_types);
		t.train(train_set,ifTinyCube,cube_partition,attr_partition);

		print cube_partition
		#print attr_partition
		
		# Construct Cubes
		for key,value in cube_partition.iteritems():
			cubes[key] = Cube(key,ifRepartition,attr_partition[key],cube_partition[key],attr_types);
						
	# Answer query
	elif check[0] == _cmd_answer:
		#print('sf');
		sql_query = check[1:];			 
		relation = sql_query[sql_query.index("FROM")+1];
		aggregate = sql_query[sql_query.index("SELECT")+1];
		predicate = sql_query[sql_query.index("WHERE")+1:];
		mycube_key = aggregate+'@'+relation;
		mycube = cubes[mycube_key];
		# This is the final answer
		mycube.answerQuery(predicate);

	elif check[0] == _cmd_getNumOfCache:
		#print('sf');
		sql_query = check[1:];			 
		relation = sql_query[sql_query.index("FROM")+1];
		aggregate = sql_query[sql_query.index("SELECT")+1];
		predicate = sql_query[sql_query.index("WHERE")+1:];
		mycube_key = aggregate+'@'+relation;
		mycube = cubes[mycube_key];
		if mycube:
			print str(idx)+','+str(mycube.getNumofGrids())+","+memUse();
		else:
			print "error"

def memUse(point=""):
    usage=resource.getrusage(resource.RUSAGE_SELF)
    return '''%s'''%((usage[2]*resource.getpagesize())/1000000.0 )

# Connect to DBMS (MySQL)
db = mysql.connector.connect(host=_host_name,user=_user_name,db=_db_name);
cursor = db.cursor();


# Turn off caching in MySQL for the purpose of experiment
cursor.execute("SET GLOBAL query_cache_type = OFF");
cursor.execute("SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");

# Find all relations and attributes within a database
cursor.execute("SHOW TABLES");
data = cursor.fetchall();
for relation in data:
	relations.append(relation[0]);
	msg = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '"+str(relation[0])+"'";
	cursor.execute(msg);
	data = cursor.fetchall();
	att_list = [];
	for att in data:
		att_list.append(str(att[0]));
		key = str(relation[0]+'.'+att[0]);
		attr_types[key] = str(att[1]);
	attributes[relation] = att_list;

	#print attr_types;


tinyCube('TRAIN ./qgen/1_out.sql',True,False,0);
test_set = [];
filename = "./qgen/2_out.sql";
if os.path.exists(filename):
	f = open(filename, 'r');
	for line in f:
		line = line.replace("\n","");
		line = line.replace(";","");
		test_set.append(line);
else:
	f = open(_default_workload, 'r');
	for line in f:
		line = line.replace("\n","");
		line = line.replace(";","");
		test_set.append(line);

final_query = "";
final_idx = 0;
for idx,query in enumerate(test_set):
	tinyCube('ANSWER '+query,True,False,idx);
	if idx%100 == 0:
		tinyCube('CACHE# '+query,True,False,idx);
	final_query = query;
	final_idx = idx;
tinyCube('CACHE# '+final_query,True,False,final_idx);


