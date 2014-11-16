# The TinyCube module that receives query from the user client and returns "approximate" answer (As a proof of concept,
# will return accurate answer for simplicity)

# Global Library
import MySQLdb
import os.path

# Local library
import queryRewriter as rewriter
import trainer as t

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
partitions = dict();

def tinyCube(query):
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

		t.train(partitions,train_set);
	print partitions;

  # Do the query rewrite
	#query_set = rewriter.queryRewriter(query);
	#for newquery in query_set:
		#print(newquery);


tinyCube('TRAIN ./config/default_workload.dat');
