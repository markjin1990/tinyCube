import mysql.connector
import random
import math

# Constant
_db_name_default = "tinyCube";
_r_name_default = "KDD";
_noise_ratio_default = 0.05;
_group_size_default = "5";
_filename_default = "query.dat";

_host_name = "localhost";
_user_name = "root";

_similarity_default = 0.1;
_num_col_default = 10;
_num_group_default = 2;

_operation_default = "SUM";
_aggr_attr_default = "src_bytes";



class QGen:
	
	db_name = "";
	r_name = "";
	noise_ratio = 0;
	group_size = 0;
	hostname = "";
	username = "";
	attr_list = [];
	min_list = [];
	max_list = [];


	def __init__(self,db_name=_db_name_default,r_name=_r_name_default,noise_ratio=_noise_ratio_default,group_size=_group_size_default): 
		self.db_name = db_name;
		self.r_name = r_name;
		self.noise_ratio = noise_ratio;
		self.group_size = group_size;

		self.hostname = _host_name;
		self.username = _user_name;

		db = mysql.connector.connect(host=self.hostname,user=self.username,db=self.db_name);
		cursor = db.cursor();
		cursor.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '"+self.r_name+"'");
		data = cursor.fetchall();

		for att in data:
			if att[1] == 'int' or att[1] == 'float':
				self.attr_list.append(att[0]);

	def genQuery(self,num_query,num_col=_num_col_default,num_group=_num_group_default,operation=_operation_default,aggr_attr=_aggr_attr_default):
		# Error handling
		if num_col > len(self.attr_list) or num_group > num_col:
			return;

		# Randomly choose num_col attributes
		attr_list_temp = list(self.attr_list);
		random.shuffle(attr_list_temp);
		selected_attr_list = attr_list_temp[:num_col];

		# Number of attributes in each group
		n = int(math.ceil(num_col/num_group));

		# Randomly group attributes into num_group groups
		group_list = [];
		random.shuffle(selected_attr_list);
		for i in range(0,num_group):
			if i<num_group-1:
				group_list.append(selected_attr_list[i*n:(i+1)*n]);
			else:
				group_list.append(selected_attr_list[i*n:]);

		print group_list;

		# Number of queries in each group
		n = int(math.ceil(num_query/num_group));
		query_list = [];
		attr_value = dict();

		for i in range(0,num_group):
			# number of random values sampled in each attributes
			num_sample = int(1/_similarity_default);
			db = mysql.connector.connect(host=self.hostname,user=self.username,db=self.db_name);
			cursor = db.cursor();
			msg = "SELECT "+",".join(group_list[i])+" FROM KDD ORDER BY RAND() LIMIT "+str(num_sample);
			print msg;
			cursor.execute(msg);
			data = cursor.fetchall();


			# number of attributes in this group
			num_attr = len(group_list[i]);
			for j in range(0,num_attr):
				attr_name = group_list[i][j];
				value_list = [];
				for k in range(0,num_sample):
					value_list.append(data[k][j]);

				# Remove duplicats
				value_list = list(set(value_list));

				# Remove max/min
				if value_list:
					value_list.remove(max(value_list));
				
				if value_list:
					value_list.remove(min(value_list));
				
				if value_list:
					attr_value[attr_name] = value_list;
			
			#Generate Queries
			



	def genQueryToFile(self,num_query,filename=_filename_default):
		queries = self.genQuery(num_query);

def main():
	qgen = QGen();
	qgen.genQuery(10);


if __name__ == "__main__":
    main()