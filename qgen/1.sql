10 SELECT sum(count) FROM LINEITEMWHERE WHERE orderdate >= date $1 and orderdate < date $1 + 2 year and discount >= $2 and discount < $2+0.03;
15 SELECT sum(count) FROM LINEITEMWHERE WHERE extendedprice >= $3 and extendedprice < $3+30000 and quantity >= $4 and quantity < $4+30;
