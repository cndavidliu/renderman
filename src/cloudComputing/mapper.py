"""
map python code used to handle rendering map 
"""

import sys
import os
import config

jobName = os.environ["jobName"]
width = os.environ["width"]
height = os.environ["height"]

"""
jobName = sys.argv[1]
width = sys.argv[2]
height = sys.argv[3]
"""

srcFile = jobName + config.fileSuffix

#using zip to instead of making dir
os.system("rm -rf " + jobName + "/" ">commandInfo 2>&1")
os.system("mkdir " + jobName + ">commandInfo 2>&1")
os.system("hadoop fs -get " + jobName + "/" + srcFile + " ./" \
	+ jobName + "/" + srcFile + ">commadInfo 2>&1")

for line in sys.stdin:
	if line == '':
		continue
	handleInfo = line.split(' ')
	outputFileName = 'output_00' + handleInfo[4] + '.png'

	povrayCommand = "povray +I" + jobName + "/" +srcFile + "+O" + jobName + "/output_00" + handleInfo[4] \
		+ " +SR" + str(handleInfo[0]) + " +ER" + str(handleInfo[1]) \
		+ " +SC" + str(handleInfo[2]) + " +EC" + str(handleInfo[3]) \
		+ " +H" + str(height) + " +W" + str(width) + " >commandInfo 2>&1"

	os.system(povrayCommand)
	os.system("hadoop fs -put ./" + jobName + "/" + outputFileName + " " + jobName + "/img >commandInfo 2>&1" )
	print outputFileName

os.system("rm -f commandInfo >commandInfo 2>&1")