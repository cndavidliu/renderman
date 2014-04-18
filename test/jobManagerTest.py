from ..src import models
from ..src.jobManager import dbManager, jobManager
from .clean import cleanDatabase
from time import sleep

maxStore = 10
threadCount = 3	
failedCount = 2
retryCount = 2
manager = None
user = models.User('davis', 'davis', 'test@gmail.com')

def initDb():
	cleanDatabase()
	dbManager.init()
	global manager
	manager = jobManager.JobManager(maxStore, threadCount, failedCount, retryCount)
	dbManager.insert(user)

def submitJobTest():
	global maxStore, user
	for i in xrange(maxStore):
		job = models.Job('testJob', '/home/mfkiller/test.rc', -1)
		job.user = user
		manager.submitJob(job)
	print 'check insert:', len(dbManager.selectJob(maxStore)) == maxStore

def handlelJobTest():
	manager.start()
	print manager.dispatcher.running


if __name__ == '__main__':
	initDb()
	submitJobTest()
	handlelJobTest()
	sleep(10)