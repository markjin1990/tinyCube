# Class Cube
# For each aggregate, e.g. SUM(A), we construct a big Cube which contains a set
# of small tinyCubes

import trainer as t
import time
import threading
import Queue
import mysql.connector

_host_name = "localhost";
_user_name = "root";
_db_name = "tinyCube";
_worker_num = 10;

class Cube:
	aggregate = "";
	operation = "";
	relation = "";
	dimensions = [];
	attr_partition = dict();
	temp_attr_partition = dict();
	attr_group = dict();
	tinyCubes = [];
	if_repartition = False;

	temp_work = [];
	q = Queue.Queue();

	def __init__(self,agg,if_repartition,attr_group,init_partition):
		self.aggregate = agg;
		info = agg.split("@");
		self.operation = info[0];
		self.relation = info[1];
		self.if_repartition = if_repartition;

		self.attr_group = attr_group;
		self.attr_partition = init_partition;
		self.dimensions = self.attr_group.keys();
		# Initialize tinycubes
		tinycubes_size = len(set(self.attr_group.values()));
		for i in range(0,tinycubes_size):
			self.tinyCubes.append(dict());
		
	def ifAggregate(self,aggregate):
		if self.aggregate == aggregate:
			return True;
		else:
			return False;

	def ifRepartition(self):
		return self.if_repartition;
		
	def checkAggregate(self):
		return self.aggregate;

	def getNumOfCubes(Self):
		return len(tinyCubes);

	def getNumofGrids(Self):
		num = 0;
		for cube in tinyCubes:
			num += len(cube);
		return num;

	def repartition(self):
		return 0;

	def computeFinalAnswer(self,answers):
		return 0;

	def checkIfGridInOldPartition(self,grid_key,tinycube_id):
		value_set = grid_key.split("&");
		attr_list = [];
		
		for key,value in self.attr_group.iteritems():
			if value == tinycube_id:
				attr_list.append(key);
			
		for i,value in enumerate(value_set):
			# If the predicate value is like "10,23"
			if "," in value:
				vset = value.split(",");
				this_attr = attr_list[i];
				value1 = vset[0];
				value2 = vset[1];
				# If this value is not in the origial partition return False
				# Same as following False
				if value1 not in self.attr_partition[this_attr]:
					return False;
				if value2 not in self.attr_partition[this_attr]:
					return False;
			else:
				value = t.convert_to_num(value);
				this_attr = attr_list[i];
				if value not in self.attr_partition[this_attr]:
					return False;
		# No one returns False, meaning this grid belongs to old partition
		return True;


	def generateQuery(self,tinycube_id,key_set):
		query_set = [];
		attr_list = [];
		
		for key,value in self.attr_group.iteritems():
			if value == tinycube_id:
				attr_list.append(key);

		for key in key_set:
			query = 'SELECT ' + self.operation + ' FROM ' + self.relation + ' WHERE';
			value_set = key.split("&");
			
			for i,value in enumerate(value_set):
				# If the predicate value is like "10,23"
				if "," in value:
					vset = value.split(",");
					this_attr = attr_list[i];
					if i > 0:
						query += " AND";
					query += " (" + this_attr + " >= " + str(vset[0]) + " AND " + this_attr + " < " + str(vset[1]) +")";

				# If the predicate value is like "10"
				else:
					value = t.convert_to_num(value);
					this_attr = attr_list[i];
					this_partition = self.attr_partition[this_attr];
					if i > 0:
						query += " AND";

					# If this is the first value in partition of attribut, say 10 in partition of 
					# attribut A: [10,15,23]. Then we have predicate A < 10, as we defined before
					if this_partition.index(value) == 0:
						query += " " + this_attr + " < " + str(value);
					# Other wise it must be the last value 23. We have predicate A >= 23
					else:
						query += " " + this_attr + " >= " + str(value);

			query_set.append(query);

		return query_set;

	def askDBworker(self,key_set,tinycube_id,lock):
	
		lock.acquire();
		newdb = mysql.connector.connect(host=_host_name,user=_user_name,db=_db_name);
		cursor = newdb.cursor();
		index = len(self.temp_work)-1;
		while index >= 0:

			subquery = self.temp_work.pop();
			lock.release();
			cursor.execute(subquery);

			query_result = cursor.fetchall();
			query_val = query_result[0][0];
			if (self.if_repartition == True) or (self.if_repartition == False and self.checkIfGridInOldPartition(key_set[index],tinycube_id)):
					lock.acquire();
					self.tinyCubes[tinycube_id][key_set[index]] = query_val;
					lock.release();
					print "Cached " + key_set[index] + " " + str(query_val);
			self.q.put(query_val);
			lock.acquire();
			index = len(self.temp_work)-1;

		lock.release();
		cursor.close();
		return;



	def askDatabase(self,key_set,tinycube_id):
		'''
		query_set = self.generateQuery(tinycube_id,key_set);
		operation = ';'.join(query_set);
		result_set = [];
		for index,result in enumerate(cursor.execute(operation, multi=True)):
			query_result = result.fetchall();
			query_val = query_result[0][0];
			# Save the partial aggregate in the tinycube grid if we want repartition
			# or (we don't allow repartition and this grids belongs to old grid) 
			if (self.if_repartition == True) or (self.if_repartition == False and self.checkIfGridInOldPartition(key_set[index],tinycube_id)):
				self.tinyCubes[tinycube_id][key_set[index]] = query_val;
				print "Cached " + key_set[index] + " " + str(query_val);
			result_set.append(query_val);
		return result_set;
		'''
		
		query_set = self.generateQuery(tinycube_id,key_set);
		self.temp_work = list(query_set);
		lock = threading.Lock();


		result_set = [];
		threads = []
		for i in range(0,_worker_num):
			t = threading.Thread(target=self.askDBworker,args=(key_set,tinycube_id,lock))
			threads.append(t)
			t.start()

		# Wait for all threads return	
		for i in range(0,_worker_num):
			threads[i].join();

		# Get all partial aggregates
		while not self.q.empty():
			result_set.append(self.q.get());

		return result_set;

	def findPartialAnswer(self,grids):
		if not grids:
			return 0;

		cached_partial_result = [];
		unknown_partial_aggr = [];
		unknown_partial_aggr_result = [];
		tinycube_id = int(grids[0]);
		tinyCube = self.tinyCubes[tinycube_id];
		for key in grids[1:]:
			# Check which cube it belongs
			if key in tinyCube.keys():
				print "Find Cached answer on " + key;
				cached_partial_result.append(tinyCube[key]);
			else:
				unknown_partial_aggr.append(key);

		if unknown_partial_aggr:
			unknown_partial_aggr_result = self.askDatabase(unknown_partial_aggr,tinycube_id);

		return cached_partial_result+unknown_partial_aggr_result;

	# Given predicate, produce a set of girds according to the partition
	# on the tinyCube.
	def findGrids(self,predicate):
		grids = [];
		# Check if the predicate attributes are in the same tinyCube
		temp_set = set();
		for key in predicate.keys():
			temp_set.add(self.attr_group[key]);

		# If All Attributes are in the same tinyCube
		# (Otherwise, they are in the different tinyCubes, 
		# which means no previous predicate can be levaraged. 
		# We choose to return empty grid in this case)
		if len(temp_set) == 1:
			tinycube_id = temp_set.pop();
			# Append this cube id information in girds to benefit
			# findPartialAnswer function
			tinycube_attr = [];
			# Get all attribute within this tinyCube
			for key,value in self.attr_group.iteritems():
				if value == tinycube_id:
					tinycube_attr.append(key);

			# Initialize temp_attr_partition
			if self.if_repartition:
				self.temp_attr_partition = self.attr_partition.copy(); 

			for att in tinycube_attr:
				grid_dim = [];
				temp_part = list(self.attr_partition[att]);
				# If the range in this dimension is specified by the predicate
				if att in predicate.keys():
					pred = predicate[att];
					# A >= x
					if pred[0] == ">=":
						value = pred[1];
						# If the value is new, then insert it in the temp_dim before 
						# finding grids.
						if value not in temp_part:
							temp_part.append(value);
							temp_part.sort();

						index = temp_part.index(value);
						size = len(temp_part);
						for i in range(index,size-1):
							grid_dim.append(temp_part[i]+','+temp_part[i+1]);
						grid_dim.append(temp_part[size-1]);

					# A < x
					elif pred[0] == "<":
						value = pred[1];
						# If the value is new, then insert it in the temp_dim before 
						# finding grids.
						if value not in temp_part:
							temp_part.append(value);
							temp_part.sort();

						index = temp_part.index(value);
						grid_dim.append(temp_part[0]);

						for i in range(0,index):
							grid_dim.append(str(temp_part[i])+','+str(temp_part[i+1]));

					# x<=A< y
					else:
						value1 = pred[0];
						value2 = pred[1];
						# If the value is new, then insert it in the temp_dim before 
						# finding grids.
						if value1 not in temp_part:
							temp_part.append(value1);
							temp_part.sort();
						if value2 not in temp_part:
							temp_part.append(value2);
							temp_part.sort();

						index1 = temp_part.index(value1);
						index2 = temp_part.index(value2);

						for i in range(index1,index2):
							grid_dim.append(str(temp_part[i])+','+str(temp_part[i+1]));


				# If the range in this dimension is NOT specified by the predicate
				else:
					size = len(temp_part);
					grid_dim.append(temp_part[0]);
					grid_dim.append(temp_part[size-1]);
					for i in range(1,size-2):
						grid_dim.append(temp_part[i]+','+temp_part[i+1]);

			  # if the grids list is empty
				if not grids:
					grids = grid_dim;
				# else add a new dimension for all the grids
				# Thus the number of grids increase exponentially
				else:
					new_grids = list();
					for grid in grids:
						for item in grid_dim:
							new_grids.append(str(grid)+"&"+str(item));
					grids = new_grids;

				# If repartition, update temp_attr_partition so that attr_partition will be updated
				# after this query processing
				if self.if_repartition:
					self.temp_attr_partition[att] = temp_part;

			# Inset tinycube id at the beginning of the 
			# grids to benefit findPartialAggregates()
			grids.insert(0,tinycube_id);

			

		# Attributes are not in same tinyCube which we will send the query directly 
		# to database; 
		else:
			print "ERROR";
		return grids;
					        


	# Put predicate into dictionary
	# [A,>=,5,AND,B,<,10] => [A:[>=,5], B:[<,10]]
	def rewritePredicate(self,predicate):
		new_pred = dict();
		size = len(predicate);
		
		i = 0;
		while i < size:
			values = [];
			attr = predicate[i];
			i += 1;
		
			values.append(predicate[i]);
			i += 1;
			values.append(t.convert_to_num(predicate[i]));
			i += 2;
			# Attribute has not appeared
			if attr not in new_pred.keys():
				new_pred[attr] = values;
			# Attribute has appeared, which means it must be like a =< A < b
			else:
				tempval =  new_pred[attr];
				val1 = tempval[1];
				val2 = values[1];
				newval = [val1,val2];
				newval.sort();
				new_pred[attr] = newval;
		return new_pred;


	# Get predicate like [A,>=,5,AND,B,<,10]
 	def answerQuery(self,predicate):
		# get all grids in the cube that is within the prdicates
		print("Start");
		#print predicate;
		current_milli_time = lambda: int(round(time.time() * 1000));
		time1 = current_milli_time();

		grids = self.findGrids(self.rewritePredicate(predicate));
		
		print "grids" + str(grids);
		
		# find all partial aggregates
		answers = self.findPartialAnswer(grids);
		ret = sum(answers);
		time2 = current_milli_time();

		print "Lantency" + str(time2-time1);
    # Compute final answer (depend on actual aggregates)
		# answer = computeFinalAnswer(answers);

		# return final answer
		#return sum(answer);
		print("END");

		# In the end, if repartition, then update the partition information
		if self.if_repartition:
			self.attr_partition = self.temp_attr_partition.copy();
			self.temp_attr_partition.clear();
			print self.attr_partition

		return ret;

	


