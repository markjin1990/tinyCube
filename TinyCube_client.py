'''
qgen
Author: Zhongjun Jin
Usage:
	python TinyCube_client.py
'''
import socket 
import sys
import ConfigParser

sys.path.insert(0, './src/')
sys.path.insert(0, './config/')
sys.path.insert(0, './qgen/')

config = ConfigParser.RawConfigParser()
config.read('./config/TinyCube.cfg')

host = config.get('TinyCube client', 'host');
port = config.getint('TinyCube client', 'port');
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
	while ";" in msg:
		msg = msg.replace(";", "")
s.send(msg)
data = s.recv(size)
s.close() 
print 'Received:', data
