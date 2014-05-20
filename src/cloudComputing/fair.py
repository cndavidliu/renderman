'''
code used to do fairing
'''
from pyspark import SparkContext, SparkConf
from operator import add
import os
import types
import sys

from config import sparkUrl, hdfsFolder

jobName = sys.argv[1]
jobCores = sys.argv[2]
jobMem = sys.argv[3] + 'g'
objFileName = sys.argv[4]
objLambda = float(sys.argv[5])
repeatTimes = int(sys.argv[6])

config = (SparkConf()
         .setMaster(sparkUrl)
         .setAppName(jobName)
         .set("spark.executor.memory", jobMem)
         .set("spark.cores.max", jobCores)
         .set("spark.scheduler.mode", "FIFO"))


sc = SparkContext(conf = config)

vertexmap = None
vertexlist = None

def getfloat(s):
	coordinate=[]
	for k in xrange(2, 5):
		coordinate.append(float(s[k]))
	return coordinate

def fchange(s):
	numbers = s.split()
	cors = []
	global vertexlist
	for i in xrange(1,4):
		cors.append(getfloat(vertexlist[int(numbers[i]) - 1].split()))
	ans = []
	global objLambda
	for i in xrange(0, 3):
		num=[]
		for j in xrange(0, 3):
			num.append(objLambda * cors[i][j] + (1 - objLambda) *(cors[i - 1][j] + cors[i - 2][j]))
		num.append(1)
		ans.append((int(numbers[i + 1]), num))
	return ans

def fadd(a,b):
	c=[]
	for i in xrange(len(a)):
		c.append(a[i] + b[i])
	return c

def ffinal(s):
	#print s
	lines = s.split()
	#print lines
	global vertexmap
	#print lines[0]
	data = vertexmap[int(lines[0])]
	ans = lines[0] + ' v'
	length = len(data)
	for i in xrange(len(data)-1):
		ans += ' ' + str(data[i]/data[length-1])
	return ans

def fend(s):
	head = 0
	while s[head] != ' ':
		head += 1
	return s[head+1:]

def main():
	global objFileName, vertexmap, vertexlist, repeatTimes
	objdata = sc.textFile(objFileName).cache()
	vertexdata = objdata.filter(lambda s : 'v ' in s)
	facedata = objdata.filter(lambda s : 'f ' in s)
	times=0

	while times < repeatTimes:
		vertexlist = vertexdata.collect()
		newdata = facedata.map(fchange)
		newdata = sc.parallelize(newdata.reduce(add))
		newdata = newdata.reduceByKey(fadd)
		vertexmap = newdata.collectAsMap()
		vertexdata = vertexdata.map(ffinal)
		times += 1

	vertexdata = vertexdata.map(fend)
	result = vertexdata.union(facedata)
	result.saveAsTextFile(hdfsFolder + jobName)

if __name__ == '__main__':
	main()