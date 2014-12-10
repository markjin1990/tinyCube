'''
qgen
Author: Zhongjun Jin
Usage:
	python TinyCube_client.py
'''
import socket 

host = 'localhost' 
port = 5000 
size = 1024 
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
s.connect((host,port))
msg = "";
mode = raw_input("TRAIN or ANSWER?(type t/a): ");
print mode
if mode == "t" or mode == "TRAIN":
	msg += "TRAIN ";
	msg += raw_input("Enter that training filename: ")
elif mode == "a" or mode == "ANSWER":
	msg += "ANSWER ";
	msg += raw_input("Enter your query: ")
s.send(msg)
data = s.recv(size)
s.close() 
print 'Received:', data
