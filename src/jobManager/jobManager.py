# -*- coding: utf-8 -*-
from .threadManager import JobExcutor, Dispatcher

# todo need to add logger

# here are some
class JobManager(object):
	"""
	Manage job using queue and multi-threads
	"""

	def __init__(self, maxStore, threadCount = 3, failedCount = 2, retryCount = 10):
		self.__running = False
		self.__dispatcher =  Dispatcher(self.__running, maxStore, threadCount, failedCount, retryCount)

	@property
	def running(self):
		return self.__running

	@running.setter
	def setRunning(self, running):
		self.__running = running

	@property
	def dispatcher(self):
		return self.__dispatcher

	@dispatcher.setter
	def setDispatcher(self, dispatcher):
		self.__dispatcher = dispatcher

	def start(self):
		if self.__running:
			return
		if self.__dispatcher is None:
			self.__dispatcher =  Dispatcher(self.__running, 100, 3, 2, 10)
		self.__running = True
		self.__dispatcher.setRunning = True
		self.__dispatcher.setDaemon(True)
		self.__dispatcher.setName('job-dispatcher')
		self.__dispatcher.start()

	def stop(self):
		if self.dispatcher is None:
			return
		self.disposeDispatcher()
		self.__dispatcher = None

	def disposeDispatcher(self):
		this.__running = False
		self.__dispatcher.setRunning = False
		semaphore.release()
		try:
			this.__dispatcher.join()
		except BaseException, e:
			print e

	def submitJob(self, job):
		return self.__dispatcher.submitJob(job)