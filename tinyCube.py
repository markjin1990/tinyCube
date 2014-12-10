# The TinyCube module that receives query from the user client and returns "approximate" answer (As a proof of concept,
# will return accurate answer for simplicity)

# Global Library
import mysql.connector
import os.path
import resource
import socket
import sys,getopt
import ConfigParser
sys.path.insert(0, './src/')
sys.path.insert(0, './config/')
sys.path.insert(0, './qgen/')

config = ConfigParser.RawConfigParser()
config.read('./config/TinyCube.cfg')

# Local library
import queryRewriter as rewriter
from trainer import Trainer
from Cube import Cube

# Constants
#_host_name = "localhost";
#_user_name = "root";
#_db_name = "tpch";

_host_name = config.get('Database', 'host');
_user_name = config.get('Database', 'username');
_db_name = config.get('Database', 'dbname');

_cmd_train = "TRAIN";
_cmd_answer = "ANSWER";
_cmd_getNumOfCache = "CACHE#";

_port = config.getint('TinyCube server', 'port')
_server_host = config.get('TinyCube server', 'host')
_backlog = 5 
_size = 1024 



class TinyCube:
	# Global variable
	relations = [];
	aggregates = [];
	attributes = dict();
	attr_types = dict();
	attr_partition = dict();
	cube_partition = dict();
	cubes = dict();
	ifTinyCube = True;
	ifRepartition = True;

	def __init__(self,mode,ifRepartition):
		if mode == "COSMOS":
			self.ifTinyCube = False;
		elif mode == "TinyCube":
			self.ifTinyCube = True;

		self.ifRepartition = ifRepartition;
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
			self.relations.append(relation[0]);
			msg = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '"+str(relation[0])+"'";
			cursor.execute(msg);
			data = cursor.fetchall();
			att_list = [];
			for att in data:
				att_list.append(str(att[0]));
				key = str(relation[0]+'.'+att[0]);
				self.attr_types[key] = str(att[1]);
			self.attributes[relation] = att_list;

	def handleRequest(self,query):
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
				return "Training File "+filename + " Not Found"

			t = Trainer(self.attr_types);
			t.train(train_set,self.ifTinyCube,self.cube_partition,self.attr_partition);

			#print self.cube_partition
			#print attr_partition
			
			# Construct Cubes
			for key,value in self.cube_partition.iteritems():
				self.cubes[key] = Cube(key,self.ifRepartition,self.attr_partition[key],self.cube_partition[key],self.attr_types);
				mycube = self.cubes[key];
				print "TinyCube["+key+ "] is created. It has "+str(mycube.getNumOfCubes())+" TinyCubes"
				print "TinyCube["+key+"] has "+str(mycube.getNumofGrids())+" cached results\n";

			return "Training Done"
							
		# Answer query
		elif check[0] == _cmd_answer:
			#print('sf');
			sql_query = check[1:];			 
			relation = sql_query[sql_query.index("FROM")+1];
			aggregate = sql_query[sql_query.index("SELECT")+1];
			predicate = sql_query[sql_query.index("WHERE")+1:];
			mycube_key = aggregate+'@'+relation;
			mycube = self.cubes[mycube_key];
			
			
			# This is the final answer
			retmsg = "Result: "+str(mycube.answerQuery(predicate));
			print retmsg;
			print "TinyCube["+mycube_key+"] has "+str(mycube.getNumofGrids())+" cached results\n";
			return retmsg;

		elif check[0] == _cmd_getNumOfCache:
			#print('sf');
			sql_query = check[1:];			 
			relation = sql_query[sql_query.index("FROM")+1];
			aggregate = sql_query[sql_query.index("SELECT")+1];
			predicate = sql_query[sql_query.index("WHERE")+1:];
			mycube_key = aggregate+'@'+relation;
			mycube = self.cubes[mycube_key];
			num = mycube.getNumofGrids();
			print "TinyCube["+key+"] has "+str(num)+" cached results\n";
			return str(num);

	def memUse(self,point=""):
	    usage=resource.getrusage(resource.RUSAGE_SELF)
	    return '''%s'''%((usage[2]*resource.getpagesize())/1000000.0 )




def main(argv):
	mode = ''
	outputfile = ''
	ifRepartition = False;
	# Check command line arguments
	try:
		opts, args = getopt.getopt(argv,"hrm:",["mode=","repartition"])
	except getopt.GetoptError:
		print 'test.py -m <mode> [-r]'
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print 'test.py -m <mode> [-r]'
			sys.exit()
		elif opt in ('-r',"repartition"):
			ifRepartition = True;
		elif opt in ("-m", "--mode"):
			mode = arg

	# Create TinyCube object
	myTinyCube = TinyCube(mode,ifRepartition);
	print "TinyCube is created"

	# Start TinyCube server
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((_server_host,_port)) 
	s.listen(_backlog) 
	while 1: 
		client, address = s.accept() 
		request = client.recv(_size) 
		if request: 
			if request == "Exit":
				client.close();
			else:
				client.send(myTinyCube.handleRequest(request));

if __name__ == "__main__":
    main(sys.argv[1:])


