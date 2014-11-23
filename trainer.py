# Train from train_set to partition the data space

# Return [Relation, [attribute_1:[value1,value2], 
# attribute_2:[value1,value2],...]]

def convert_to_num(s):
	try:
		return int(s)
	except ValueError:
		return float(s)

def partAttr(train_set,attr_partition,ifTinyCube):
	query_attr_set = [];
	
	# Get all predicate attributes for each query
	# ['SUM(A)@T',[B,C,E]]
	for query in train_set:
		attr_set = [];
		query_set = query.split(" ");

		relation = query_set[query_set.index("FROM")+1];
		aggregate = query_set[query_set.index("SELECT")+1];
	
		index = query_set.index("WHERE");
		query_set = query_set[index+1:];
		while len(query_set) > 0:
			att = query_set.pop(0);
			attr_set.append(att);
			op = query_set.pop(0);
			
			if op == '=' or op == '>=' or op == '>' or op == '<=' or op == '<':
				value = query_set.pop(0);
			elif op == "BETWEEN":
				value1 = query_set.pop(0);
				query_set.pop(0);
				value2 = query_set.pop(0);
	
			# Get rid of "AND"
			if query_set:
				query_set.pop(0);
		
		query_attr_set.append([aggregate+'@'+relation,attr_set]);
	
	print(query_attr_set);
		
	if not ifTinyCube:
		for query_attr in query_attr_set:
			key = query_attr[0];
			attr = query_attr[1];
			if key in attr_partition.keys():
				attr_dict = attr_partition[key];
				for att in query_attr[1]:
					attr_dict[att] = 0;
					
				attr_partition[key] = attr_dict;
			else:
				attr_dict = dict();
				for att in query_attr[1]:
					attr_dict[att] = 0;
					
				attr_partition[key] = attr_dict;

def train_parser(query):
	query_set = query.split(" ");

	relation = query_set[query_set.index("FROM")+1];
	aggregate = query_set[query_set.index("SELECT")+1];

	index = query_set.index("WHERE");
	query_set = query_set[index+1:];
	att_dict = dict();
	while len(query_set) > 0:
		values = [];
		att = query_set.pop(0);
		op = query_set.pop(0);
		
		if op == '=' or op == '>=' or op == '>' or op == '<=' or op == '<':
			value = query_set.pop(0);
			values.append(convert_to_num(value));

		# Get rid of "AND"
		if query_set:
			query_set.pop(0);

		# If the attribute has a value list, then merge it with the new one
		if att in att_dict.keys():
			att_dict[att] = values + att_dict[att];
		else:
			att_dict[att] = values;
	
	return [aggregate+'@'+relation,att_dict];		


def train(train_set,ifTinyCube,cube_partitions,attr_partition):
	# Partition the attributes according to workload 
	# (Put them in same partition if ifTinyCube is false)
	partAttr(train_set,attr_partition,ifTinyCube);
	
	#print(attr_partition);
	
			
	for query in train_set:
		ret = train_parser(query);

		print "parser"+str(ret);
		cubeKey = ret[0];
		att_dict = ret[1];
		#print att_dict;
		
		
		# If this operation in this relation is already partitioned, merge new partition with old
		if cubeKey in cube_partitions:
			att_dict_cube = cube_partitions[cubeKey];
			for key in att_dict.iterkeys():
				# If this attribute is already partitioned, merge the new with old
				if key in att_dict_cube:
					att_dict_cube[key] = list(set(att_dict_cube[key]+att_dict[key]));
					att_dict_cube[key].sort();
				# If this attribute is not partitioned before, then assign the new partition
				else:
					att_dict_cube[key] = att_dict[key];
			cube_partitions[cubeKey] = att_dict_cube;
		# If this is the new partition to this operation in this relation, create new partition	
		else:
			att_dict_cube = dict();
			for key in att_dict.iterkeys():
				att_dict_cube[key] = att_dict[key];
			cube_partitions[cubeKey] = att_dict_cube;
		
	

train_parser("SELECT SUM(A) FROM T WHERE A >= 10 AND B BETWEEN 0 AND 20 AND C = 10");

