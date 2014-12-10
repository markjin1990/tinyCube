25 SELECT sum(l_quantity) FROM lineitem WHERE l_shipdate >= date $1 and l_shipdate < date $1+30(day) and l_receiptdate >= date $1 and l_receiptdate < date $1+7(day);
25 SELECT sum(l_quantity) FROM lineitem WHERE l_extendedprice >= $2 and l_extendedprice < $2+3000 and l_discount >= $3 and l_discount < $3+0.03;
