import numpy as np

class Matrix:
	matrix = [];
	col = 0;
	row = 0;

	def __init__(self,row,col,matrix=[]):
		self.row = row;
		self.col = col;
		if not matrix:
			for i in range(0,self.row):
				newrow = [];
				for j in range(0,self.col):
					newrow.append(0);
				self.matrix.append(newrow);
		else:
			self.matrix = matrix;

	def getMatrix(self):
		return self.matrix;

	def addOne(self,row,col):
		self.matrix[row][col] += 1;

	def update(self,row,col,value):
		self.matrix[row][col] = value;

	def getValue(self,row,col):
		return self.matrix[row][col];

	def toString(self):
		return self.matrix;