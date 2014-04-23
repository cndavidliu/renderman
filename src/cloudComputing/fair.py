"""
code used to do fairing
"""
from pyspark import SparkContext
from operator import add
import os
import types
sc=SparkContext("local","Test Job")
test_lambda=0.5

def getfloat(s):
	coordinate=[]
	for k in xrange(2,5):
		coordinate.append(float(s[k]))
	return coordinate

def fchange(s):
	numbers=s.split()
	cors=[]
	for i in xrange(1,4):
		cors.append(getfloat(vertexlist[int(numbers[i])].split()))
	ans=[]
	for i in xrange(0,3):
		num=[]
		for j in xrange(0,3):
			num.append(test_lambda*cors[i][j]+(1-test_lambda)*(cors[i-1][j]+cors[i-2][j]))
		num.append(1)
		ans.append((int(numbers[i+1]),num))
	return ans

def fadd(a,b):
	c=[]
	for i in xrange(len(a)):
		c.append([i]+b[i])
	return c

def ffinal(s):
	#print s
	lines=s.split()
	#print lines
	global test_lambda
	global newmap
	#print lines[0]
	data=newmap[int(lines[0])]
	ans=lines[0]+' v'
	length=len(data)
	for i in xrange(len(data)-1):
		ans+=' '+str(data[i]/data[length-1])
	return ans

def fend(s):
	head=0
	while s[head]!=' ':
		head+=1
	return s[head+1:]

def test():
	objfile='data/new.obj'
	objdata=sc.textFile(objfile).cache()
	vertexdata=objdata.filter(lambda s : 'v ' in s)
	facedata=objdata.filter(lambda s : 'f ' in s)
	times=0

	while times<5:
		vertexlist=vertexdata.collect()
		newdata=facedata.map(fchange)
		newdata=sc.parallelize(newdata.reduce(add))
		newdata=newdata.reduceByKey(fadd)
		vertexmap=newdata.collectAsMap()
		vertexdata=vertexdata.map(ffinal)
		times+=1

	vertexdata=vertexdata.map(fend)
	result=vertexdata.union(fdata)
	result.saveAsTextFile('result')

if __name__=='__main__':
	test()
