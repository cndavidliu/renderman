from sqlalchemy import Column, Integer, Sequence, String, DateTime, ForeignKey, desc
from sqlalchemy.orm import relationship, backref
from .meta import Model
import datetime
import os

jobTypes = ['Render', 'Fair']
jobStates = ['Create', 'Wait', 'Running', 'Failed', 'Success', 'Retry', 'Killed']
resultFileSuffix = ['.png', '.obj']
resultFloder = ['/img/', '/obj/']

class Job(Model):
	"""
	Model for the spark job  and used in JobManager
	"""

	# TODO  need a new class for JobManager?

	__tablename__ = 'job'

	id = Column(Integer, Sequence('job_id_seq'), primary_key = True)

	name = Column(String(255), nullable = False)
	state = Column(String(255), default = 'Create')
	sourceFile = Column(String(255), nullable = False, default = '/')
	jobType = Column(Integer, default = 0)

	startTime = Column(DateTime)
	finishTime = Column(DateTime)
	totalTime = Column(Integer, default = 0)
	overTime = Column(Integer, default = 1200)

	description = Column(String(255), default = '')
	extraInfo = Column(String(255), default = '')

	retryTimes = Column(Integer, default = 0)

	#Owner
	user_id = Column(Integer, ForeignKey('user.id'))
	user = relationship('User', backref = backref('jobs', order_by = id.desc()))

	isDownloaded = Column(String(1), default = '0')


	def __repr__(self):
		global jobTypes
		return "Job.name = %s, Job.type = %s, Job.description = %s, Job.user = %s, Job.config = %s" %\
		 (self.name, jobTypes[self.jobType], self.description, self.user, self.getConfig())

	def isFinished(self):
		return self.state in ['Failed', 'Success', 'Killed']

	def getConfig(self):
		if len(self.configs) == 0:
			return None
		return self.configs[-1]

	def __init__(self, name, sourceFile, jobType = -1):
		self.name = name
		self.sourceFile = sourceFile
		self.jobType = jobType

	def setTotalTime(self):
		self.totalTime = (self.finishTime - self.startTime).seconds

	def isOverTime(self):
		now = datetime.datetime.now()
		if self.state == 'Running':
			return (now - self.startTime).seconds > self.overTime
		return False

	def getJobName(self):
		global jobTypes
		return jobTypes[self.jobType] + '_' + self.name + '-' + str(self.user_id)

	def isSuccess(self):
		global jobStates
		return self.state == jobStates[4]

	def getResultName(self):
		global resultFileSuffix
		return self.getJobName() + resultFileSuffix[0] 

	def getJobType(self):
		global jobTypes
		return jobTypes[self.jobType]

	def isFailed(self):
		global jobStates
		return self.state == jobStates[3] or self.state == jobStates[6]

	def isRetry(self):
		global jobStates
		return self.state == jobStates[5]

	def isRender(self):
		return self.jobType == 0

	def isKilled(self):
		global jobStates
		return self.state == jobStates[6] 

	def downloadFile(self, downloadPath):
		jobName = self.getJobName()
		global resultFileSuffix, resultFloder
		redirectCommand = " >/dev/null 2>&1"
		downloadCommand = "$HADOOP_HOME/bin/hadoop fs -get " + jobName + resultFloder[self.jobType] + jobName \
		+ resultFileSuffix[self.jobType] + " " + downloadPath + "/"
		os.system(downloadCommand + redirectCommand)
		self.isDownloaded = '1'

	def ifDownloaded(self):
		return self.isDownloaded == '1'

	def removeFile(self, downloadPath):
		jobName = self.getJobName()
		redirectCommand = " >/dev/null 2>&1"
		global resultFileSuffix
		os.system("rm -f " + downloadPath + "/" + jobName + resultFileSuffix[self.jobType] + redirectCommand)
		self.isDownloaded = '0'