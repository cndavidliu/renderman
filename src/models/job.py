from sqlalchemy import Column, Integer, Sequence, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship, backref
from .meta import Model
import datetime

jobTypes = ['render', 'fair']
jobStates = ['Create', 'Wait', 'Running', 'Failed', 'Success', 'Retry']

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
	user = relationship('User', backref = backref('jobs', order_by = id))


	def __repr__(self):
		global jobTypes
		return "Job.name = %s, Job.type = %s, Job.description = %s, Job.user = %s, Job.config = %s" %\
		 (self.name, jobTypes[self.jobType], self.description, self.user, self.getConfig())

	def isFinished(self):
		return self.state in ['Failed', 'Success']

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