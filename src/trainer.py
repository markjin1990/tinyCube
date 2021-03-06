# Train from train_set to partition the data space

# Return [Relation, [attribute_1:[value1,value2], 
# attribute_2:[value1,value2],...]]

from matrix import Matrix
import numpy as np
import scipy as sp
from sklearn.cluster import KMeans
import time

class Trainer():
	attr_types = dict();

	def __init__(self,types):
		self.attr_types = types;

	def if_num(self,value):
		try:
			float(value);
			return True;
		except ValueError:
			return False;

	def convert_to_num(self,s):
		try:
			return int(s)
		except ValueError:
			return float(s)

	def getSetOFfTwo(self,mylist):
		if len(mylist) == 1:
			return [];
		mylist = list(set(mylist));
		setoftwo = [];
		for i1,value1 in enumerate(mylist):
			for i2,value2 in enumerate(mylist):
				if i1>i2:
					setoftwo.append([value1,value2]);
		return setoftwo;



	def spectralClustering(self,query_attr_set):
		
		# Get a set of aggregates mentioned in query_attr_set. E.g. ["SUM(A)@T","SUM(B)@T"]
		aggr_list = [];
		# For each aggregate, we have a list of predicate attribute groups, which will be used
		# for clustering later
		aggr_predicate_attr_list = [];

		#print query_attr_set

		for item in query_attr_set:
			if item[0] in aggr_list:
				i = aggr_list.index(item[0]);
				aggr_predicate_attr_list[i].append(item[1]);
			else:
				aggr_list.append(item[0]);
				aggr_predicate_attr_list.append([item[1]]);

		#print aggr_predicate_attr_list;

		# This is running one spectral clustering for each unique aggregation 
		group_info = dict();
		for index,predicate_attr_set in enumerate(aggr_predicate_attr_list):
			cluster_num = 2;


			#print predicate_attr_set;
			attr_set = list();
			for sub_attr_set in predicate_attr_set:
				attr_set.extend(sub_attr_set);
			#print attr_set;
			attr_set = list(set(attr_set));

			# Create a similarity graph
			attr_num = len(attr_set);
			SimGraph = Matrix(attr_num,attr_num);

			# print attr_set

			# Update the similarity graph
			for sub_attr_set in predicate_attr_set:
				retlist = self.getSetOFfTwo(sub_attr_set);
				if retlist:
					for pair in retlist:
						index1 = attr_set.index(pair[0]);
						index2 = attr_set.index(pair[1]);
						SimGraph.addOne(index1,index2);
						#SimGraph.addOne(index1,index1);
						SimGraph.addOne(index2,index1);
						#SimGraph.addOne(index2,index2);
			

			# Get Graph Laplacian
			ret = np.linalg.eigh(SimGraph.L());
			
			# Compute eigenvectors and eigenvalues of Laplacian
			egval = ret[0];
			egvec = ret[1];


			while True:
				# new test data
				data = [];
				for i in range(0,attr_num):
					newdat = [];
					for j in range(0,cluster_num):
						newdat.append(egvec[i][j]);
					data.append(newdat);



				# K-means to find cluster
				est = KMeans(k=cluster_num);
				est.fit(data);
				labels = est.labels_;
				


				# Create sub grouping map
				subGroupMap = dict();
				for i in range(0,attr_num):
					subGroupMap[attr_set[i]] = labels[i];

				# Create final grouping decision for this aggregation
				group_info[aggr_list[index]] = subGroupMap;

				mismatch_num = 0;
				for one_query_attr_set in predicate_attr_set:
					temp_set = set()
					for myattr in one_query_attr_set:
						temp_set.add(subGroupMap[myattr]);
					# If the predicate attributes of this query can not be found in one TinyCube
					if len(temp_set)>1:
						mismatch_num += 1;
				mismatch_ratio = float(mismatch_num) / float(len(predicate_attr_set));
				if mismatch_ratio > 0.1:
					# This many cluster causing too many mismatches, we set cluster_num -1 as final number of clusters
					cluster_num -= 1;
					break;
				else:
					# Try more clusters
					cluster_num += 1


			# Final Cluster Assignment
			data = [];
			for i in range(0,attr_num):
				newdat = [];
				for j in range(0,cluster_num):
					newdat.append(egvec[i][j]);
				data.append(newdat);



			# K-means to find cluster
			est = KMeans(k=cluster_num);
			est.fit(data);
			labels = est.labels_;
			


			# Create sub grouping map
			subGroupMap = dict();
			for i in range(0,attr_num):
				subGroupMap[attr_set[i]] = labels[i];

			# Create final grouping decision for this aggregation
			group_info[aggr_list[index]] = subGroupMap;


		return group_info;

	def groupAttr(self,train_set,attr_partition,ifTinyCube):
		query_attr_set = [];

		# Get all predicate attributes for each query
		# ['SUM(A)@T',[B,C,E]]
		for query in train_set:
			attr_set = [];
			query_set = query.split(" ");
			#print query_set;

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

			# The the set of attributes for each query
			attr_set = list(set(attr_set));
			
			# We get ["SUM(A)@T",[B,C]]
			# 			 ["SUM(A)@T",[B,E]]
			# 			 ["SUM(A)@T",[D,F]]
			query_attr_set.append([aggregate+'@'+relation,attr_set]);
		
		#print(query_attr_set);
		
		# We have a huge Cube instead of several small tinyCubes
		if not ifTinyCube:
			for query_attr in query_attr_set:
				key = query_attr[0];
				attr = query_attr[1];
				# If the aggregate is in the attribution grouping, add atrribute->0
				if key in attr_partition.keys():
					attr_dict = attr_partition[key];
					for att in query_attr[1]:
						attr_dict[att] = 0;
						
					attr_partition[key] = attr_dict;
				# If the aggregate is not in the attribution grouping, put atrribute->0
				else:
					attr_dict = dict();
					for att in query_attr[1]:
						attr_dict[att] = 0;
						
					attr_partition[key] = attr_dict;
			print attr_partition
		# We enable tinyCubes
		else:
			for key,value in self.spectralClustering(query_attr_set).iteritems():
				attr_partition[key] = value;


	def train_parser(self,query):
		#print self.attr_types
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
				# Save as number if the value is a number
				if self.if_num(value):
					values.append(self.convert_to_num(value));
				# If date, convert it to epoch
				elif self.attr_types[relation+'.'+att] == "date":
					pattern = '%Y-%m-%d'
					value = int(time.mktime(time.strptime(value, pattern)))
					values.append(value);
				# Else it is just string, do nothing
				else:
					values.append(value);

			# Get rid of "AND"
			if query_set:
				query_set.pop(0);

			# If the attribute has a value list, then merge it with the new one
			if att in att_dict.keys():
				att_dict[att] = values + att_dict[att];
			else:
				att_dict[att] = values;
		
		return [aggregate+'@'+relation,att_dict];		


	def train(self,train_set,ifTinyCube,cube_partitions,attr_partition):
		# Partition the attributes according to workload 
		# (Put them in same partition if ifTinyCube is false)
		self.groupAttr(train_set,attr_partition,ifTinyCube);

		#print(attr_partition);
				
		for query in train_set:
			ret = self.train_parser(query);

			#print "parser"+str(ret);
			cubeKey = ret[0];
			att_dict = ret[1];
			
			
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
		


