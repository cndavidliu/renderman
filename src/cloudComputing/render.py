#! /usr/bin/env python
import sys
import os
import time
import config

'''
generate hadoop streamming command
'''

jobName = sys.argv[1]
mapTaskCount = int(sys.argv[2])
width = sys.argv[3]
height = sys.argv[4]
srcFile = sys.argv[5]

logFile = config.logFolder + jobName + '.log'
redirectCommand = '>' + logFile + ' 2>&1'

#hdfs ops
os.system('hadoop fs -mkdir ' + jobName + ' ' + redirectCommand)
os.system('hadoop fs -mkdir ' + jobName + '/img ' + redirectCommand)

#init hadoop input file
os.system("mkdir " + config.hdfsFolder + jobName + '-input ' + redirectCommand)
handleFilePath = config.hdfsFolder + jobName + '-input/' + config.handleFileName
handleFile = open(handleFilePath, 'w')
coordinates = []
for i in xrange(0, mapTaskCount + 1):
	coordinates.append(i * 1.0 / mapTaskCount)
for i in xrange(0, mapTaskCount):
	print >> handleFile, coordinates[i], coordinates[i+1], '0.0', '1.0', i, ' '
handleFile.close()

#put srcFile to hdfs
os.system('hadoop fs -put ' + handleFilePath + ' ' + jobName + ' ' + redirectCommand)
os.system('hadoop fs -put ' + srcFile + ' ' + jobName + '/ ' + redirectCommand)
'''
hadoopCommand = ("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.0.4.jar \
-D mapred.map.tasks=%d -D mapred.reduce.tasks=1 \
-input %s/%s -output %s/output \
-file %s -file %s -mapper %s -file %s -reducer %s \
-cmdenv jobName=%s -cmdenv width=%s \
-cmdenv height=%s -cmdenv mapTaskCount=%d %s" ) % \
(mapTaskCount, jobName, config.handleFileName, jobName, config.configFilePath, config.mapperFilePath, config.mapperFilePath, \
	config.reducerFilePath, config.reducerFilePath, jobName, width, height, mapTaskCount, redirectCommand)
'''

hadoopCommand = ("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.0.4.jar \
-D mapred.map.tasks=%d -D mapred.reduce.tasks=1 \
-input %s/%s -output %s/output \
-file %s -file %s -mapper '%s %s %s %s' -file %s -reducer '%s %s %s %s %d' %s") % \
(mapTaskCount, jobName, config.handleFileName, jobName, config.configFilePath, config.mapperFilePath, config.mapperFilePath, \
	jobName, width, height, config.reducerFilePath, config.reducerFilePath, jobName, width, height, mapTaskCount, redirectCommand)

#print hadoopCommand
os.system("rm -rf " + config.hdfsFolder + jobName + '-input/ ' + redirectCommand)
os.system(hadoopCommand)