# -*- coding: utf-8 -*-
from threading import Thread, Semaphore
from Queue import Queue
import os
import .dbManager
import datetime

semaphore = Semaphore(1)

class JobExcutor(Thread):
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
		try:
			self.__excutedJob.startTime = datetime.datetime.now()
			os.system(self.getCommand())
			global semaphore
			semaphore.release()
			self.__excutedJob.state = 'Success'
			sef.__excutedJob.finishTime = datetime.datetime.now()
		except BaseException, e:
			self.__excutedJob.state = 'Retry'
		#update job

	def getCommand(self):
		return None

	def isOverTime(self):
		now = datetime.datetime.now()
		return (now - self.__excutedJob.startTime).seconds > self.__excutedJob.overTime



class Dispatcher(Thread):
	"""
	the defined Thread ued to manage jobQueue and failedQueue
	"""

	def __init__(self, running = True, maxStore = 100, threadCount = 3, failedCount = 2, retryCount = 10):
		Thread.__init__(self)
		self.__running = running
		self.__maxStore = maxStore
		self.__threadCount = threadCount
		self.__failedCount = failedCount
		self.__retryCount  = retryCount
		self.__jobQueue = Queue(self.__maxStore)
		self.__retryQueue = Queue()
		self.__failedQueue = Queue()
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
			if excutedJob.retryTimes >= 3:
				excutedJob.state = 'Failed'
				self.__failedQueue.put(excutedJob)
				dbManager.updateJob(excutedJob)
				if self.__failedQueue.qsize() > self.__failedCount:
					print 'the failed Queue is full, too many failed job'
			else:
				idleCount -= 1
				self.excuteJob(excutedJob)
		while idleCount > 0 and self.__jobQueue.qsize() > 0:
			excutedJob = self.__jobQueue.get()
			self.excuteJob(excutedJob)
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
			return
		handleThreads.append(JobExcutor(job))
		handleThreads[-1].start()

	@property
	def running(self):
		return self.__running

	@running.setter
	def setRunning(self, running):
		self.__running = running

	@staticmethod
	def addFailedJob(job):
		self.__failedQueue.put(job)

	@staticmethod
	def addRetryJob(job):
		self.__retryQueue.put(job)

	def submitJob(self, job):
		if self.__jobQueue.qsize() == self.__maxStore:
			return False
		try:
			self.__jobQueue.put(job,1,1)
			return True
		except BaseException, e:
			print e
			return False