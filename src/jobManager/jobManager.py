# -*- coding: utf-8 -*-
from threading import Thread, Semaphore
from Queue import Queue
from ..models import job, meta
import os
import datetime

# todo need to add logger

# here are some

semaphore = Semaphore(1)

def update(job):
	updateJob = Job.query(id = job.id).all()[0]
	updateJob = job
	meta.db_session.commit()

class JobExcutor(Thread):
	def __init__(self, job, name = 'job-excutor'):
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
			os.system()
		except BaseException, e:
			self.__excutedJob.state = 'Retry'
		global semaphore
		semaphore.release()
		#update job

	def getCommand(self):

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


class JobManager(object):
	"""
	Manage job using queue and multi-threads
	"""

	def __init__(self):
		self.__running = False
		self.__dispatcher = None

	def __init__(self, maxStore = 100, threadCount = 3, failedCount = 2, retryCount = 10):
		self.__init__()
		self.start(maxStore, threadCount, failedCount, retryCount)

	@property
	def running(self):
		return self.__running

	@running.setter
	def setRunning(self, running):
		self.__running = running

	@property
	def dispatcher(self):
		return this.__dispatcher

	@dispatcher.setter
	def setDispatcher(self, dispatcher):
		this.__dispatcher = dispatcher

	def start(self, maxStore, threadCount, failedCount, retryCount):
		if self.dispatcher is not None:
			return
		self.__running = True
		self.__dispatcher =  Dispatcher(self.__running, maxStore, threadCount, failedCount, retryCount)
		self.__dispatcher.setDaemon(True)
		sef.__dispatcher.setName('job-dispatcher')
		self.__dispatcher.start()

	def stop(self):
		if self.dispatcher is None:
			return
		self.disposeDispatcher()
		self.__dispatcher = None

	def disposeDispatcher(self):
		this.__running = False
		semaphore.release()
		try:
			this.__dispatcher.join()
		except BaseException, e:
			print e