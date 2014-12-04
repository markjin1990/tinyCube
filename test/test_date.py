import mysql.connector

import time
from datetime import date, datetime, timedelta

_host_name = "localhost";
_user_name = "root";
_db_name = "tpch";
_relation_name = "lineitem";

db = mysql.connector.connect(host=_host_name,user=_user_name,db=_db_name);
cursor = db.cursor();
msg = "SELECT l_shipdate FROM " +_relation_name +" limit 1";
print msg;
cursor.execute(msg);
data = cursor.fetchone();
date_time = str(data[0]);

pattern = '%Y-%m-%d';
epoch = int(time.mktime(time.strptime(date_time, pattern)))
print epoch