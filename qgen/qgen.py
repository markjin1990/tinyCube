import mysql.connector

# Constant
_db_name_default = "tinyCube";
_r_name_default = "A";
_noise_ratio_default = "0.05";
_filename_default = "query.dat";

_username = "root";
_password = "mark1990";


class QGen:
	
	db_name = "";
	r_name = "";
	noise_ratio = 0;

	def __init__(self,db_name=_db_name_default,r_name=_r_name_default,noise_ratio=_noise_ratio_default): 
		self.db_name = db_name;
		self.r_name = r_name;
		self.noise_ratio = noise_ratio;

	def genQuery(self,num_query):

	def genQueryToFile(self,num_query,filename=_filename_default):
		queries = self.genQuery(num_query,filename);

def main():
	qgen = QGen();	   


if __name__ == "__main__":
    main()