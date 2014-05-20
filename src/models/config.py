from sqlalchemy import Column, Integer, Sequence, String, DateTime, ForeignKey, Float
from sqlalchemy.orm import relationship, backref
from .meta import Model

class Config(Model):
	"""
	config for each job
	"""

	__tablename__ = 'config'

	id = Column(Integer, Sequence('config_id_seq'), primary_key = True)

	#config info
	instanceMem = Column(Integer, default = 2)
	instanceCores = Column(Integer, default = 6)

	#parameters for rendering
	mapTaskCount = Column(Integer)
	height = Column(Integer)
	width = Column(Integer)
	
	#parameters for fairing
	repeatTimes = Column(Integer)
	objLambda = Column(Float)


	extraConfig = Column(String(255), default = '')

	#Owner
	job_id = Column(Integer, ForeignKey('job.id'), unique = True)
	job = relationship('Job', backref = backref('configs', order_by = id))


	def __repr__(self):
		return "instanceMem = %d, instanceCores = %d" %(self.instanceCores, self.instanceMem)

	def getRenderConfig(self):
		return self.mapTaskCount, self.width, self.height

	def setRenderConfig(self, mapTaskCount, width, height):
		self.mapTaskCount = mapTaskCount
		self.width = width
		self.height = height

	def __init__(self, instanceMem = 2, instanceCores = 6):
		self.instanceMem = instanceMem
		self.instanceCores = instanceCores

	def getFairConfig(self):
		return self.objLambda, self.repeatTimes

	def setFairConfig(self, objLambda, repeatTimes):
		self.objLambda = objLambda
		self.repeatTimes = repeatTimes
