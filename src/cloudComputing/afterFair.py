"""
do some handles after fair
"""
import os
import sys

from config import hdfsFolder, fairFileSuffix, hdfsLogFolder

jobName = sys.argv[1]

logFile = hdfsLogFolder + jobName + '.log'
redirectCommand = '>>' + logFile + ' 2>>' + logFile

resultFolder = hdfsFolder + jobName + '/'
objFile = resultFolder + jobName + fairFileSuffix

files = os.listdir(resultFolder)
files.sort()

for resultFile in files:
	if resultFile[0:4] == 'part':
		os.system('cat ' + resultFolder + resultFile + ' >>' + objFile)

os.system('hadoop fs -mkdir ' + jobName + redirectCommand)
os.system('hadoop fs -put ' + objFile + ' ' + jobName + redirectCommand)
os.system('rm -rf ' + resultFolder)