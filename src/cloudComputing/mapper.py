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
povFileName = sys.argv[2]
width = sys.argv[3]
height = sys.argv[4]

srcFile = jobName + fileSuffix

#srcFile = jobName + '.pov'
redirectFile = hdfsFolder  + jobName + "/commandInfo"
redirectCommand = " >>" + redirectFile + " 2>>" + redirectFile

#using zip to instead of making dir
if not os.path.exists(hdfsFolder + jobName):
	os.system("mkdir " + hdfsFolder + jobName + ">/dev/null 2>&1")
	os.system("$HADOOP_HOME/bin/hadoop fs -get " + jobName + "/" + srcFile + " " + hdfsFolder \
		+ redirectCommand)
	os.system("unzip " + hdfsFolder + srcFile + " -d " +  hdfsFolder + jobName + redirectCommand )
	os.system("rm -rf " + hdfsFolder + srcFile + " >/dev/null 2>&1")

for line in sys.stdin:
	if line == '':
		continue
	handleInfo = line.split(' ')
	outputFileName = 'output_00' + handleInfo[4] + '.png'

	povrayCommand = "/usr/local/bin/povray +I" + povFileName + ".pov" + " +O" + hdfsFolder + jobName + "/output_00" + handleInfo[4] \
		+ " +SR" + str(handleInfo[0]) + " +ER" + str(handleInfo[1]) \
		+ " +SC" + str(handleInfo[2]) + " +EC" + str(handleInfo[3]) \
		+ " +H" + str(height) + " +W" + str(width) + redirectCommand

	os.chdir(hdfsFolder + jobName)
	os.system(povrayCommand)
	os.system("$HADOOP_HOME/bin/hadoop fs -put " + hdfsFolder + jobName + "/" + outputFileName + " " + jobName + "/img" + redirectCommand)

	print outputFileName

#os.system("rm -rf " + hdfsFolder + jobName  + " >/dev/null 2>&1")