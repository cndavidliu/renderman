from ..src import models
from ..src.jobManager import dbManager, jobManager
from .clean import cleanDatabase
from time import sleep
from ..src.cloudComputing import config
import os

maxStore = 3
threadCount = 2	
failedCount = 2
retryCount = 2
manager = None
user = models.User('davis', 'davis', 'test@gmail.com')

testJobName = 'testJob'

def initDb():
	cleanDatabase()
	dbManager.init()
	global manager
	manager = jobManager.JobManager(maxStore, threadCount, failedCount, retryCount)
	dbManager.insert(user)

def initFiles():
	global testJobName
	for i in xrange(maxStore):
		os.system('cp ' + config.serverFolder + testJobName + config.fileSuffix \
			+ ' ' + config.serverFolder + testJobName + '-' + str(i + 1) + config.fileSuffix)

def submitJobTest():
	global maxStore, user, testJobName
	for i in xrange(maxStore):
		job = models.Job(testJobName, '/home/mfkiller/code/spark_cloud/static/testJob-' + str(i) + '.pov', 0)
		jobConfig = models.Config()
		jobConfig.setRenderConfig(2, 640, 960)
		job.user = user
		manager.submitJob(job, jobConfig)
	print 'check insert:', len(dbManager.selectJob(maxStore)) == maxStore

def handlelJobTest():
	manager.start()
	print manager.dispatcher.running

def cleanFiles():
	global testJobName
	for i in xrange(maxStore):
		os.system('rm -f ' + config.serverFolder + testJobName + '-' + str(i + 1) + config.fileSuffix)
		#os.system('rm -f ' + config.logFolder + testJobName + str(i + 1) + config.fileSuffix)
		os.system('hadoop fs -rmr ' + testJobName + '-' + str(i + 1) +'/')

if __name__ == '__main__':
	initDb()
	initFiles()
	submitJobTest()
	handlelJobTest()
	sleep(200)
	cleanFiles()