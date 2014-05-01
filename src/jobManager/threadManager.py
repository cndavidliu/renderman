# -*- coding: utf-8 -*-
from threading import Thread, Semaphore
from Queue import Queue
import os
from .dbManager import checkJob, insert, selectJob, update, commit
import datetime
from ..cloudComputing import config

semaphore = Semaphore(0)
logKeyWord = ['complete', 'complete']

class JobExcutor(Thread):
	"""
	The thread used to excute job
	"""

	def __init__(self, job, name = 'job-excutor'):
		Thread.__init__(self)
		self.__excutedJob = job
		self.name = name

	@property
	def excutedJob(self):
		return self.__excutedJob

	@excutedJob.setter
	def setExcutedJob(self, job):
		self.__excutedJob = job

	def run(self):
		exceptionOccured = 0
		try:
			self.rmLog()
			self.__excutedJob.startTime = datetime.datetime.now()
			self.__excutedJob = update(self.__excutedJob, 'Running')
			os.system(self.getCommand(self.__excutedJob.jobType))
			self.__excutedJob.finishTime = datetime.datetime.now()
		except BaseException, e:
			exceptionOccured = 1
			print e
		if exceptionOccured != 1 and self.detectLog(self.__excutedJob.jobType):
			self.rmLog()
			self.__excutedJob.state = 'Success'
		else:
			self.__excutedJob.state = 'Retry'
		update(self.__excutedJob, self.__excutedJob.state, True)
		global semaphore
		semaphore.release()

	def getCommand(self, jobType):
		command = ''
		if jobType == -1:
			command = command + 'ls -l'
			return command
		elif jobType == 1:
			command = command + 'fair'
			return command
		elif jobType == 0:
			jobName = self.getJobName()
			#print self.__excutedJob.getConfig()
			mapTaskCount = self.__excutedJob.getConfig().mapTaskCount
			height = self.__excutedJob.getConfig().height
			width = self.__excutedJob.getConfig().width
			command = "python /home/mfkiller/code/spark_cloud/src/cloudComputing/render.py %s %d %d %d" % \
			(jobName, mapTaskCount, width, height)
			return command

	def getJobName(self):
		return self.__excutedJob.name + '-' + str(self.__excutedJob.id)

	def rmLog(self):
		os.system('rm -f ' + config.logFolder + self.getJobName() + '.log >/dev/null 2>&1')

	def detectLog(self, jobType):
		"""
		use log detection to check job state need to change or fix
		"""
		global logKeyWord
		return True


class Dispatcher(Thread):
	"""
	the defined Thread ued to manage jobQueue and failedQueue
	"""

	def __init__(self, running = True, maxStore = 100, threadCount = 3, failedCount = 2, retryCount = 10, retryTimes = 3):
		Thread.__init__(self)
		self.__running = running
		self.__maxStore = maxStore
		self.__threadCount = threadCount
		self.__failedCount = failedCount
		self.__retryCount  = retryCount
		self.__jobQueue = Queue(self.__maxStore)
		self.__retryQueue = Queue()
		self.__failedQueue = Queue()
		self.__retryTimes = retryTimes
		self.__handleThreads = []
		checkJob()


	def run(self):
		while self.__running:
			try:
				global semaphore
				semaphore.acquire()
				self.dispatch()
			except BaseException, e:
				print e

	def dispatch(self):
		jobCount = self.__maxStore - self.__jobQueue.qsize()
		if jobCount > 0:
			pushJobs = selectJob(jobCount)
			#print pushJobs
			self.addJobInBatch(pushJobs)
		idleCount = self.getIdlePos()
		if idleCount == 0:
			return
		# need to handle failedCount
		while idleCount/2 > 0 and self.__retryQueue.qsize() > 0:
			excutedJob = self.__retryQueue.get()
			if excutedJob.retryTimes >= self.retryTimes:
				excutedJob.state = 'Failed'
				self.__failedQueue.put(excutedJob)
				update(excutedJob, excutedJob.state)
				if self.__failedQueue.qsize() > self.__failedCount:
					print 'the failed Queue is full, too many failed job'
			else:
				if not self.excuteJob(excutedJob):
					self.__retryQueue.put(excuteJob, 1, 2)
					return
				idleCount -= 1
		while idleCount > 0 and self.__jobQueue.qsize() > 0:
			excutedJob = self.__jobQueue.get()
			if not self.excuteJob(excutedJob):
				try:
					self.__jobQueue.put(excutedJob, 1, 2)
				except BaseException, e:
					print 'job excutes failed, need to handle'
				return
			idleCount -= 1

	def getIdlePos(self):
		idleCount = self.__threadCount - len(self.__handleThreads)
		finishedThreads = []
		for i in xrange(len(self.__handleThreads)):
			if not self.__handleThreads[i].isAlive():
				if self.__handleThreads[i].excutedJob.state == 'Retry':
					retryJob = self.__handleThreads[i].excutedJob
					self.__retryQueue.put(retryJob)
				finishedThreads.append(self.__handleThreads[i])
				idleCount += 1
			elif self.__handleThreads[i].excutedJob.isOverTime():
				self.__handleThreads[i].excutedJob[i].excutedJob.finishTime = datetime.datetime.now()
				retryJob = update(self.__handleThreads[i].excutedJob, 'Retry', True)
				self.__retryQueue.put(retryJob)
				self.__handleThreads[i].join()
				finishedThreads.append(self.__handleThreads[i])
				idleCount += 1
		for finishedThread in finishedThreads:
			self.__handleThreads.remove(finishedThread)
		return idleCount
	
	def excuteJob(self, job):
		if len(self.__handleThreads) == self.__maxStore:
			return False
		self.__handleThreads.append(JobExcutor(job))
		self.__handleThreads[-1].start()
		return True

	def addFailedJob(self, job):
		self.__failedQueue.put(job)

	def addRetryJob(self, job):
		self.__retryQueue.put(job)

	#todo need to test
	def addJobInBatch(self, jobs):
		for job in jobs:
			try:
				job.state = 'Wait'
				self.__jobQueue.put(job,1,1)
			except BaseException, e:
				job.state = 'Create'
				break
			commit(job)

	def submitJob(self, job, jobConfig = None):
		"""
		here just insert the job into db and release the semaphore
		when the expection about db happends, failed
		"""
		try:
			insert(job)
			if jobConfig is not None:
				jobConfig.job_id = job.id
				insert(jobConfig)
		except BaseException, e:
			return False
		global semaphore
		semaphore.release()
		return True


	def cleanJob(self):
		global semaphore
		semaphore.release()

	@property
	def running(self):
		return self.__running

	@running.setter
	def setRunning(self, running):
		self.__running = running

	@property
	def maxStore(self):
		return self.__maxStore
	
	@maxStore.setter
	def setMaxStore(self, maxStore):
		self.__maxStore = maxStore

	@property
	def threadCount(self):
		return self.__threadCount

	@threadCount.setter
	def setThreadCount(self, threadCount):
		self.__threadCount = threadCount

	@property
	def failedCount(self):
		return self.__failedCount

	@failedCount.setter
	def setFailedCount(self, failedCount):
		self.__failedCount = failedCount

	@property
	def retryCount(self):
		return self.__retryCount

	@retryCount.setter
	def setRetryCount(self, retryCount):
		self.__retryCount = retryCount

	@property
	def jobQueue(self):
		return self.__jobQueue

	@jobQueue.setter
	def setJobQueue(self, jobQueue):
		self.__jobQueue = jobQueue

	@property
	def failedQueue(self):
		return self.__failedQueue

	@failedQueue.setter
	def setFailedQueue(self, failedQueue):
		self.__failedQueue = failedQueue

	@property
	def retryQueue(self):
		return self.__retryQueue

	@retryQueue.setter
	def setRetryQueue(self, retryQueue):
		self.__retryQueue = retryQueue

	@property
	def retryTimes(self):
		return self.__retryTimes

	@retryTimes.setter
	def setRetryTimes(self, retryTimes):
		self.__retryTimes = retryTimes

	@property
	def handleThreads(self):
		return self.__handleThreads

	@handleThreads.setter
	def setHandleThreads(self, handleThreads):
		self.__handleThreads = handleThreads

	'''
	def submitJob(self, job):
		"""
		the api used to submit job here is the return value's instruction:
		-1: the waitiing Job Queue is full!
		0: put into the waiting Job Queue
		1: catch some other exception, such as db excetion or semaphore excetion Fail!
		"""
		try:
			dbManager.insert(job)
			if self.__jobQueue.qsize() == self.__maxStore:
				return -1
			else:
				try:
					job.state = 'Wait'				
					self.__jobQueue.put(job,1,1)
				except BaseException, e:
					job.state = 'Create'
					return -1
				dbManager.update(job)
			global semaphore
			semaphore.release()
			return 0
		except BaseException, e:
			print e
			return 1
		'''