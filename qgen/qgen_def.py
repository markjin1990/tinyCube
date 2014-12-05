import mysql.connector
import os.path
import random
import math
import re
import datetime

# Constant
_db_name_default = "tpch";
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

	def resolveCalc(self,computation,ifDate):
		if "+" in computation or "-" in computation or "*" in computation or "/" in computation:
			if ifDate:
				tmp = re.split('\+|\-|\*|\/|(|)',computation);
				var_tag = tmp[0];
				op_val = tmp[1];
				unit = tmp[2];
				if "+" in computation:
					if unit == "day":
						d = datetime.datetime.strptime(var_tag, '%Y-%m-%d') + datetime.timedelta(days=self.convert_to_num(op_val))
						return d.strftime('%Y-%m-%d');
					else:
						print unit + " is not supported"
						return var_tag;
				elif "-" in computation:
					if unit == "day":
						d = datetime.datetime.strptime(var_tag, '%Y-%m-%d') + datetime.timedelta(days=self.convert_to_num("-"+op_val));
						return d.strftime('%Y-%m-%d');				
					else:
						print unit + " is not supported"
						return var_tag;
			else:
				tmp = re.split('\+|\-|\*|\/|(|)',computation);
				val1 = self.convert_to_num(tmp[0]);
				val2 = self.convert_to_num(tmp[1]);
				if "+" in computation:
					return val1 + val2;
				elif "-" in computation:
					return val1 - val2;
				elif "*" in computation:
					return val1 * val2;
				elif "/" in computation:
					return val1 / val2;

		else:
			return computation;


	def replaceVar(self,relation,attribute,num):
		db = mysql.connector.connect(host=_host_name,user=_user_name,db=self.db_name);
		cursor = db.cursor();
		msg = "SELECT "+attribute+" FROM "+relation+" ORDER BY RAND() LIMIT "+str(num);
		print msg
		cursor.execute(msg);
		data = cursor.fetchall();
		ret_list = [];
		for m in data:
			ret_list.append(m[0]);
		return ret_list;



	def genOneQuery(self,query_seg,num):
		idx = query_seg.index("FROM");
		relation_name = query_seg[idx+1];
		var_dict = dict();
		date_var = set();
		ret_query = [];
		rep_idx = [];

		for idx,seg in enumerate(query_seg):
			if seg[0] == '$':
				rep_idx.append(idx);
				seg = seg[1:];
				# If this is a number
				if self.if_num(seg):
					if seg not in var_dict.keys():
						if query_seg[idx-1] == "date":
							date_var.add(seg)
							var_dict[seg] = self.replaceVar(relation_name,query_seg[idx-3],num);
						else:
							var_dict[seg] = self.replaceVar(relation_name,query_seg[idx-2],num);
				
				# If this is like 1+2
				elif "(" not in seg and ")" not in seg:	
					var_tag = "";
					op = "";
					op_val = "";
					if "+" in seg:
						op = "+";
						tmp = seg.split('+');
						var_tag = tmp[0];
						op_val = tmp[1];
					elif "-" in seg:
						op = "-";
						tmp = seg.split('-');
						var_tag = tmp[0];
						op_val = tmp[1];
					elif "*" in seg:
						op = "*";
						tmp = seg.split('*');
						var_tag = tmp[0];
						op_val = tmp[1];
					elif "/" in seg:
						op = "/";
						tmp = seg.split('/');
						var_tag = tmp[0];
						op_val = tmp[1];
					if var_tag not in var_dict.keys():
						var_dict[seg] = self.replaceVar(relation_name,query_seg[idx-2],num);

				# if this is like a+b(year)
				else:
					var_tag = "";
					op = "";
					if "+" in seg:
						op = "+";
					elif "-" in seg:
						op = "-";
					elif "*" in seg:
						op = "*";
					elif "/" in seg:
						op = "/";
					tmp = re.split('\+|\-|\*|\/|(|)',seg);
					var_tag = tmp[0];
					op_val = tmp[1];
					unit = tmp[2];
					if var_tag not in var_dict.keys():
						date_var.add(var_tag);
						var_dict[seg] = self.replaceVar(relation_name,query_seg[idx-3],num);

		# Create new queries
		for i in range(0,num):
			new_query_seg = list(query_seg);
			for idx in rep_idx:
				p = re.match("\$[0-9]+", new_query_seg[idx]);
				if p:
					var_tag = p.group()[1:];
					val_list = var_dict[var_tag];
					orig_str = new_query_seg[idx];
					raw_data = orig_str.replace(str(p.group(0)),str(val_list[i]));
					if var_tag in date_var:
						new_query_seg[idx] = self.resolveCalc(raw_data,True);
					else:
						new_query_seg[idx] = self.resolveCalc(raw_data,False);
			ret_query.append(" ".join(new_query_seg));

		return ret_query;


	def genQuery(self):
		retQuerySet = [];
		for idx,query in enumerate(self.query_list):
			query_seg = query.split(' ');
			genQuerySet = self.genOneQuery(query_seg,self.query_num[idx]);
			retQuerySet.extend(genQuerySet);

		return retQuerySet;

	def genQueryToFile(self,output_file):
		return;
			



	def genQueryToFile(self):
		queries = self.genQuery();

def main():
	qgen = QGen();
	print qgen.genQuery();


if __name__ == "__main__":
    main()
