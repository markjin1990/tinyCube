import mysql.connector
import os.path
import random
import math

# Constant
_db_name_default = "tinyCube";
_template_name_default = "1.sql";
_host_name = "localhost";
_user_name = "root";


class QGen:
	
	db_name = "";
	template_file = "";
	query_num = [];
	query_list = [];


	def __init__(self,db_name=_db_name_default,template_name=_template_name_default):
		self.template_file = template_name;
		self.db_name = db_name;
		train_set = [];
		if os.path.exists(self.template_file):
			f = open(self.template_file, 'r');
			c = f.read(1);
			msg= "";
			while c:
				if c != ';':
					if c != '\n':
						msg += c;
				else: 
					train_set.append(msg);
					print msg
					msg = "";
				c = f.read(1);

		for item in train_set:
			idx = item.index(' ');
			self.query_num.append(int(item[:idx]));
			self.query_list.append(item[idx+1:]);


		print self.query_list;
		print self.query_num;


	def replaceVar(self,relation,attribute,num):
		db = mysql.connector.connect(host=_host_name,user=_user_name,db=self.db_name);
		cursor = db.cursor();

	def genOneQuery(self,query_seg,num):
		idx = query_list.index("FROM");
		relation_name = query_list[idx+1];
		

	def genQuery(self):
		retQuerySet = [];
		for idx,query in enumerate(self.query_list):
			query_seg = query.split(' ');
			genQuerySet = genOneQuery(query_seg,query_num[idx]);
			retQuerySet = retQuerySet + genQuerySet;

		return retQuerySet;

	def genQueryToFile(self,output_file):
		return;
			



	def genQueryToFile(self):
		queries = self.genQuery();

def main():
	qgen = QGen();
	#qgen.genQuery(10);


if __name__ == "__main__":
    main()
