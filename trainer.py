# Train from train_set to partition the data space

# Return [Relation, [attribute_1:[value1,value2], 
# attribute_2:[value1,value2],...]]
def train_parser(query):
	query_set = query.split(" ");
	relation = query_set[query_set.index("FROM")+1];
	index = query_set.index("WHERE");
	query_set = query_set[index+1:];
	att_dict = dict();
	while len(query_set) > 0:
		values = [];
		att = query_set.pop(0);
		op = query_set.pop(0);
		
		if op == '=' or op == '>=' or op == '>' or op == '<=' or op == '<':
			value = query_set.pop(0);
			values.append(value);
		elif op == "BETWEEN":
			value1 = query_set.pop(0);
			values.append(value1);
			query_set.pop(0);
			value2 = query_set.pop(0);
			values.append(value2);

		# Get rid of "AND"
		if query_set:
			query_set.pop(0);

		att_dict[att] = values;
	
	return [relation,att_dict];		


def train(partitions,train_set):
	for query in train_set:
		ret = train_parser(query);
		relation = ret[0];
		att_dict = ret[1];
		print att_dict;
		if relation in partitions:
			att_dict_cube = partitions[relation];
			for key in att_dict:
				if key in att_dict_cube:
					att_dict_cube[key] = list(set(att_dict_cube[key]+att_dict[key])).sort();
				else:
					att_dict_cube[key] = att_dict[key].sort();
			partitions[relation] = att_dict_cube;
		else:
			att_dict_cube = dict();
			for key in att_dict:
				att_dict_cube[key] = att_dict[key].sort();
			partitions[relation] = att_dict_cube;
		print att_dict_cube;

train_parser("SELECT SUM(A) FROM T WHERE A >= 10 AND B BETWEEN 0 AND 20 AND C = 10");

