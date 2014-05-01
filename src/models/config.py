from sqlalchemy import Column, Integer, Sequence, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship, backref
from .meta import Model

class Config(Model):
	"""
	config for each job
	"""

	__tablename__ = 'config'

	id = Column(Integer, Sequence('config_id_seq'), primary_key = True)

	#config info
	instanceMem = Column(Integer, default = 200)
	instanceCores = Column(Integer, default = 6)

	#parameters for rendering
	mapTaskCount = Column(Integer)
	height = Column(Integer)
	width = Column(Integer)

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

	def __init__(self, instanceMem = 200, instanceCores = 6):
		self.instanceMem = instanceMem
		self.instanceCores = instanceCores