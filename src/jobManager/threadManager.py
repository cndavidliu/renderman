# -*- coding: utf-8 -*-
from threading import Thread, Semaphore
from Queue import Queue
import os
import .dbManager
import datetime

semaphore = Semaphore(1)

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
			self.__excutedJob.startTime = datetime.datetime.now()
			self.__excutedJob.state = 'Running'
			dbManager.updateJob(self.__excutedJob)
			os.system(self.getCommand(self.__excutedJob.jobType))
		except BaseException, e:
			self.__excutedJob.state = 'Retry'
			exceptionOccured = 1
		global semaphore
		semaphore.release()
		if exceptionOccured != 1:
			self.__excutedJob.state = 'Success'
		sef.__excutedJob.finishTime = datetime.datetime.now()

	def getCommand(self, state):
		if state == -1:
			return 'ls'
		elif state = 1:
			return 'fair'
		else:
			return 'render'

	def isOverTime(self):
		now = datetime.datetime.now()
		return (now - self.__excutedJob.startTime).seconds > self.__excutedJob.overTime



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


	def run(self):
		while self.__running:
			try:
				global semaphore
				semaphore.acquire()
				self.dispatch()
			except BaseException, e:
				print e

	def dispatch(self):
		idleCount = self.getIdlePos()
		if idleCount == 0:
			return
		# need to handle failedCount
		while idleCount/2 > 0 and self.__retryQueue.qsize() > 0:
			excutedJob = self.__retryQueue.get()
			if excutedJob.retryTimes >= self.retryTimes:
				excutedJob.state = 'Failed'
				self.__failedQueue.put(excutedJob)
				dbManager.updateJob(excutedJob)
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
		idleCount = self.__threadCount - len(handleThreads)
		finishedThreads = []
		for i in xrange(len(self.__handleThreads)):
			if not self.__handleThreads[i].isAlive():
				finishedThreads.append(i)
				if self.__handleThreads[i].excutedJob.state == 'Retry':
					retryJob = self.__handleThreads[i].excutedJob
					retryJob.retryTimes += 1
					self.__retryQueue.put(retryJob)
				dbManager.updateJob(retryJob)
				idleCount += 1
			elif self.__handleThreads[i].isOverTime:
				finishedThreads.append(i)
				self.__handleThreads[i].retry()
				self.__handleThreads[i].join()
				idleCount += 1
		for index in finishedThreads:
			del self.__handleThreads[i]
		return idleCount
	
	def excuteJob(self, job):
		if len(handleThreads) == maxStore:
			return False
		handleThreads.append(JobExcutor(job))
		handleThreads[-1].start()
		return True

	def addFailedJob(self, job):
		self.__failedQueue.put(job)

	def addRetryJob(self, job):
		self.__retryQueue.put(job)

	def submitJob(self, job):
		if self.__jobQueue.qsize() == self.__maxStore:
			return False
		try:
			self.__jobQueue.put(job,1,1)
			global semaphore
			semaphore.release()
			return True
		except BaseException, e:
			print e
			return False

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
