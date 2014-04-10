# -*- coding: utf-8 -*-
from .threadManager import JobExcutor, Dispatcher

# todo need to add logger

# here are some
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

	def submitJob(self, job):
		return self.__dispatcher.submitJob(job)