#! /usr/bin/env python
import sys
import os
import time
import config

jobName = sys.argv[1]
mapTaskCount = int(sys.argv[2])
width = sys.argv[3]
height = sys.argv[4]

srcFile = config.serverFolder + jobName + config.fileSuffix

#hdfs ops
os.system('hadoop fs -mkdir ' + jobName)
os.system('hadoop fs -mkdir ' + jobName + '/img')

#init hadoop input file
os.system("mkdir " + config.hdfsFolder + jobName + '-input')
handleFilePath = config.hdfsFolder + jobName + '-input/' + config.handleFileName
handleFile = open(handleFilePath, 'w')
coordinates = []
for i in xrange(0, mapTaskCount + 1):
	coordinates.append(i * 1.0 / mapTaskCount)
for i in xrange(0, mapTaskCount):
	print >> handleFile, coordinates[i], coordinates[i+1], '0.0', '1.0', i, ' '
handleFile.close()

#put srcFile to hdfs
os.system('hadoop fs -put ' + handleFilePath + ' ' + jobName)
os.system('hadoop fs -put ' + srcFile + ' ' + jobName + '/')
'''
hadoopCommand = ("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.0.4.jar \
-D mapred.map.tasks=%d -D mapred.reduce.tasks=1 \
-input %s/%s -output %s/output \
-file %s -file %s -mapper %s -file %s -reducer %s \
-cmdenv jobName=%s -cmdenv width=%s \
-cmdenv height=%s -cmdenv mapTaskCount=%d" ) % \
(mapTaskCount, jobName, config.handleFileName, jobName, config.configFilePath, config.mapperFilePath, config.mapperFilePath, \
	config.reducerFilePath, config.reducerFilePath, jobName, width, height, mapTaskCount)
'''

hadoopCommand = ("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.0.4.jar \
-D mapred.map.tasks=%d -D mapred.reduce.tasks=1 \
-input %s/%s -output %s/output \
-file %s -file %s -mapper '%s %s %s %s' -file %s -reducer '%s %s %s %s %d'") % \
(mapTaskCount, jobName, config.handleFileName, jobName, config.configFilePath, config.mapperFilePath, config.mapperFilePath, \
	jobName, width, height, config.reducerFilePath, config.reducerFilePath, jobName, width, height, mapTaskCount)

print hadoopCommand
os.system("rm -rf " + config.hdfsFolder + jobName + '-input/')
os.system(hadoopCommand)