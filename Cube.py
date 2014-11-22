# Class Cube
# For each aggregate, e.g. SUM(A), we construct a big Cube which contains a set
# of small tinyCubes

class Cube:
	aggregate = "";
	dimensions = [];
	partitions = dict();
	cache = dict();
	num_cache = 0;

	def __init__(self,agg,attr_part,init_partition):
		self.aggregate = agg;
		self.partitions = init_partition;
		self.dimensions = init_partition.keys();
		
	def ifAggregate(self,aggregate):
		if self.aggregate == aggregate:
			return True;
		else:
			return False;
		
	def checkAggregate(self):
		return self.aggregate;

	def repartition(self):
		return 0;

	def computeFinalAnswer(self,answers):
		return 0;


	def findPartialAnswer(self,grids,cursor):
		return 0;

	def findGrids(self,predicate):
		grids = [];
		for att in self.dimensions:
			grid_dim = [];
			temp_part = partitions[att];
			# If the range in this dimension is specified by the predicate
			if att in predicate.keys():
				pred = predicate[att];
				# A >= x
				if pred[0] == ">=":
					value = pred[1];
					# If the value is new, then insert it in the temp_dim before 
					# finding grids.
					if value not in temp_part:
						temp_part.append(value).sort();

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
						temp_part.append(value).sort();

					index = temp_part.index(value);
					grid_dim.append(temp_part[0]);
					for i in range(1,index-1):
						grid_dim.append(temp_part[i]+','+temp_part[i+1]);

				# A BETWEEN x AND y (x <= A < y)
				else:
					value1 = pred[0];
					value2 = pred[1];
					# If the value is new, then insert it in the temp_dim before 
					# finding grids.
					if value1 not in temp_part:
						temp_part.append(value1).sort();
					if value2 not in temp_part:
						temp_part.append(value2).sort();

					index1 = temp_part.index(value1);
					index2 = temp_part.index(value2);
					for i in range(index1,index2-1):
						grid_dim.append(temp_part[i]+','+temp_part[i+1]);

			# If the range in this dimension is NOT specified by the predicate
			else:
				size = len(temp_part);
				grid_dim.append(temp_part[0]);
				grid_dim.append(temp_part[size-1]);
				for i in range(1,size-2):
					grid_dim.append(temp_part[i]+','+temp_part[i+1]);

		  # if the grids list is empty
			if not grids:
				grids = grid_dim();
			else:
				new_grids = list();
				for grid in grids:
					for item in grid_dim:
						new_grids.append(grid+'&'+item);
				grids = new_grids;
		return grids;
					        

	# Get predicate like A:10,20, B: >= 10 
 	def answerQuery(self,predicate,cursor):
		# get all grids in the cube that is within the prdicates
		print("Start");
		print predicate;
		grids = self.findGrids(predicate);
		print("END");
		# find all partial aggregates
		#answers = findPartialAnswer(grids,cursor);

    # Compute final answer (depend on actual aggregates)
		#answer = computeFinalAnswer(answers);

		# return final answer
		#return sum(answer);

	


