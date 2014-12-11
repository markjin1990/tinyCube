This is TinyCube, a lightweight partial aggregate results caching engine. 
The code is written for Python 2.7 and MySQL 5.5.

How to configure:

	In ./config/TinyCube.config, configure the database, server, and client set up.
	For Database,
		you configure what is the host, username and database name.
	For TinyCube server,
		you configure what is the host and port you want to run the TinyCube server on.

	For TinyCube client,
		you configure what is the server's host and port.

How to use it:

	0. Make sure you have your MySQL server running in the backend, and have the test relation ready.

	1. Start TinyCube server:
		python tinyCube.py -m [TinyCube/COSMOS] [-r]
		-m: mode, you could choose COSMOS or TinyCube
		-r: optional, is turning on "Repartition"
	
	2. Start TinyCube client:
		 python TinyCube_client.py
	
	3. Training
	   After initilization:
	   It prompts:
	   	"TRAIN or ANSWER (type t/a):"
	   Type "t":
	   
	   It then prompts:
	   	"Enter that training filename:"
	   Type your training query file name to train.

	4. Working
	   After initilization:
	   It prompts:
	   	"TRAIN or ANSWER (type t/a):"
	   Type "a":
	   
	   It then prompts:
	   	"Enter your query: "
	   Type your query to get answer.

	5. How to generate training queries?
		Use ./qgen/qgen.py, see ./qgen/README

Dataset:
	I used tpc-h dataset to test it. See http://www.tpc.org/tpch/ for details about tpc-h.
	Try to use dataset smaller than 1G if you are running on personal computer.

Note:
This is just a simple prototype of TinyCube for proof of concept.
It can not support complex queries that have "join", "group by", "having", etc., in them.
For predicate comparison operator, it supports ">=" and "<" for numbers and dates, and "=" for strings.
For aggregate operation, it supports "SUM" and "COUNT".
A typical query is like "SELECT SUM(A) FROM T WHERE A > a_1 AND B <= b1".

Also be careful in using "COSMOS" as the mode because it is SLOW!!!

Source Code Files:
	tinyCube.py: TinyCube server source file

	TinyCube_client.py: TinyCube client for testing the server

	src/Cube.py: Cube class, used for all operations within TinyCube including answering queries, partition computation, caching and retrieving partial answers and communicating with Databases

	src/matrix.py: some matrix computation used by src/trainer.py

	src/trainer.py: create clustering and partitioning in training phase

	src/queryRewriter.py: rewrite query string into a structure that is useable by other scripts.

	qgen/qgen.py: automatically generating training and testing queries from user-defined query template

	config/config.py: generate configuration file. But TinyCube.cfg configuration file can be edited throught any text editor without using config.py

