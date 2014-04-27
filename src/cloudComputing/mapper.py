#! /usr/bin/env python
import sys
import os
from config import fileSuffix, hdfsFolder

'''
map python code used to handle rendering map 
'''
'''
jobName = os.environ["jobName"]
width = os.environ["width"]
height = os.environ["height"]
'''

jobName = sys.argv[1]
width = sys.argv[2]
height = sys.argv[3]

srcFile = jobName + fileSuffix

#srcFile = jobName + '.pov'
rediretCommand = " >" + hdfsFolder + jobName + "/commandInfo 2>&1"

#using zip to instead of making dir
os.system("mkdir " + hdfsFolder + jobName + ">/dev/null 2>&1")
os.system("$HADOOP_HOME/bin/hadoop fs -get " + jobName + "/" + srcFile + " " + hdfsFolder \
	+ jobName + "/" + srcFile + rediretCommand)

for line in sys.stdin:
	if line == '':
		continue
	handleInfo = line.split(' ')
	outputFileName = 'output_00' + handleInfo[4] + '.png'

	povrayCommand = "/usr/local/bin/povray +I" + hdfsFolder + jobName + "/" +srcFile + " +O" + hdfsFolder + jobName + "/output_00" + handleInfo[4] \
		+ " +SR" + str(handleInfo[0]) + " +ER" + str(handleInfo[1]) \
		+ " +SC" + str(handleInfo[2]) + " +EC" + str(handleInfo[3]) \
		+ " +H" + str(height) + " +W" + str(width) + rediretCommand

	os.system(povrayCommand)
	os.system("$HADOOP_HOME/bin/hadoop fs -put " + hdfsFolder + jobName + "/" + outputFileName + " " + jobName + "/img" + rediretCommand)

	print outputFileName

os.system("rm -rf " + hdfsFolder + jobName  + " >/dev/null 2>&1")