import re
data = "$10=10";
p = re.match("\$[0-9]+", data);
print p
