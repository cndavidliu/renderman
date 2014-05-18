"""
Test for log config
"""

import logging
import logging.config
import os

from ..src.jobManager.config import systemLogConf, systemLogFile

logging.config.fileConfig(systemLogConf)
logger = logging.getLogger("system")

def logTest():
	logger.debug("THis is the debug information.")
	logger.info("This is the info information.")
	logger.warn("This is the warn information")
	logger.error("This is the error information")
	logger.error("This is the string insert %s - %d", 'test', 1)

def exceptionTest():
	testList = []
	try:
		print testList[2]
	except BaseException, e:
		logger.warn(e)

def getLogContent():
	logFile = open(systemLogFile, 'r')
	contents = logFile.readlines()
	logFile.close()
	for content in contents:
		print content

if __name__ == '__main__':
	logTest()
	exceptionTest()
	getLogContent()
	os.system("rm -f " + systemLogFile)