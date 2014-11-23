# Class Cube
# For each aggregate, e.g. SUM(A), we construct a big Cube which contains a set
# of small tinyCubes

import trainer as t

class Cube:
	aggregate = "";
	operation = "";
	relation = "";
	dimensions = [];
	attr_partition = dict();
	attr_group = dict();
	tinyCubes = [];

	def __init__(self,agg,attr_group,init_partition):
		self.aggregate = agg;
		info = agg.split("@");
		self.operation = info[0];
		self.relation = info[1];

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

	def generateQuery(self,tinycube_id,key_set):
		query_set = [];
		attr_list = [];
		
		for key,value in self.attr_group.iteritems():
			if value == tinycube_id:
				attr_list.append(key);

		for key in key_set:
			query = 'SELECT ' + self.operation + ' FROM ' + self.relation + ' WHERE';
			value_set = key.split("&");
			i = 0;
			for value in value_set:
				# If the predicate value is like "10,23"
				if "," in value:
					vset = value.split(",");
					this_attr = attr_list[i];
					if i > 0:
						query += " AND";
					query += " (" + this_attr + " >= " + vset[0] + " AND " + this_attr + " < " + vset[1] +")";
					i += 1;
				# If the predicate value is like "10"
				else:

					this_attr = attr_list[i];
					this_partition = self.attr_partition[this_attr];
					if i > 0:
						query += " AND";

					# If this is the first value in partition of attribut, say 10 in partition of 
					# attribut A: [10,15,23]. Then we have predicate A < 10, as we defined before
					if this_partition.index(value) == 0:
						query += " " + this_attr + " < " + value;
					# Other wise it must be the last value 23. We have predicate A >= 23
					else:
						query += " " + this_attr + " >= " + value;
					i += 1;

			query_set.append(query);

		return query_set;

	def askDatabase(self,key_set,tinycube_id,cursor):
		query_set = self.generateQuery(tinycube_id,key_set);
		result_set = [];
		for query in query_set:
			cursor.execute(query);
			result_set.append(cursor.fetchall());
		return result_set;

	def findPartialAnswer(self,grids,cursor):
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
				cached_partial_result.append(tinyCube[key]);
			else:
				unknown_partial_aggr.append(key);

		if unknown_partial_aggr:
			unknown_partial_aggr_result = self.askDatabase(unknown_partial_aggr,tinycube_id,cursor);

		return cached_partial_result+unknown_partial_aggr_result;

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


			for att in tinycube_attr:
				grid_dim = [];
				temp_part = self.attr_partition[att];
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

					# A BETWEEN x AND y (x <= A < y)
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
				else:
					new_grids = list();
					for grid in grids:
						for item in grid_dim:
							new_grids.append(str(grid)+"&"+str(item));
					grids = new_grids;

			# Inset tinycube id at the beginning of the 
			# grids to benefit findPartialAggregates()
			grids.insert(0,tinycube_id);

		# Attributes are not in same tinyCube which we will send the query directly 
		# to database; 
		else:
			print "ERROR";
		return grids;
					        
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



	# Get predicate like A:10,20, B: >= 10 
 	def answerQuery(self,predicate,cursor):
		# get all grids in the cube that is within the prdicates
		print("Start");
		#print predicate;
		grids = self.findGrids(self.rewritePredicate(predicate));
		
		print "grids" + str(grids);
		
		# find all partial aggregates
		answers = self.findPartialAnswer(grids,cursor);

    # Compute final answer (depend on actual aggregates)
		# answer = computeFinalAnswer(answers);

		# return final answer
		#return sum(answer);
		print("END");

	


