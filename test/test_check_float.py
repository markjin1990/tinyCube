def isfloat(value):
	try:
		float(value)
		return True
	except ValueError:
		return False


print isfloat(5);
print isfloat(5.5);
print isfloat("wgwegw");
