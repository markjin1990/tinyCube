10 SELECT sum(l_quantity) FROM lineitem WHERE l_shipdate >= date $1 and l_shipdate < date $1+300(day) and l_discount >= $2 and l_discount < $2+0.3;
15 SELECT sum(l_quantity) FROM lineitem WHERE l_extendedprice >= $3 and l_extendedprice < $3+30000 and l_quantity >= $4 and l_quantity < $4+100;
