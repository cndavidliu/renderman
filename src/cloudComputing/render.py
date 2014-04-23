"""
code used to generate hadoop command for rendering
Usage:
python render.py
the map task count you can choose only can be square number
"""

import sys
import os
import time
import config

jobName = sys.argv[1]
mapTaskCount = int(sys.argv[2])
width = int(sys.argv[3])
height = int(sys.argv[4])

srcFile = jobName + config.fileSuffix

#hdfs ops
os.system('hadoop fs -mkdir ' + jobName)
os.system('hadoop fs -mkdir' + jobName + '/img')

#init hadoop input file
handleFilePath = config.prefixFilePath + jobName + '/' + config.handleFileName
handleFile = open(handleFilePath, 'w')
coordinates = []
for i in xrange(0, mapTaskCount + 1):
	coordinates.append(i * 1.0 / mapTaskCount)
for i in xrange(0, mapTaskCount):
	print >> handleFile, coordinates[i], coordinates[i+1], '0.0', '1.0', i, ' '
handleFile.close()

#put srcFile to hdfs
os.system('hadoop fs -put' + handleFilePath + ' ' + jobName)
os.system('hadoop fs -put' + srcFile + ' ' + job_name + '/')

hadoopCommand = "$HADOOP_HOME/bin/hadoop jar " \
		+ "$HADOOP_HOME/contrib/streaming/hadoop-streaming-1.0.4.jar "\
		+ "-D mapred.map.tasks=" + mapTaskCount + " " \
		+ "-input " + jobName + "/" + config.handleFileName + " " + "-output " + jobName + "/output "\
		+"-file " + config.mapperFilePath + " "\
		+ "-mapper " + config.mapperFilePath + " "\
		+ "-file " + config.reducerFilePath + " "\
		+ "-reducer " + config.reducerFilePath + " "\
		+ "-cmdenv " + "jobName = jobName "\
		+ "-cmdenv " + "width = width "\
		+ "-cmdenv " + "height = height"

print hadoopCommand
os.system(hadoopCommand)