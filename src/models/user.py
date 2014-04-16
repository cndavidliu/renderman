from sqlalchemy import Column, Integer, Sequence, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property

from passlib.hash import sha256_crypt

from meta import Model

class User(Model):
	"""
	Model for user info
	"""

	__tablename__ = 'user'

	id = Column(Integer, Sequence('user_id_seq'), primary_key = True)
	name = Column(String(255), unique = True, nullable = False)
	_password = Column('password', String(255), nullable = False)
	sex = Column(Integer, default = 0)
	email = Column(String(255), nullable = False, unique = True)
	age = Column(Integer, default = 18)
	description = Column(String(255))

	@hybrid_property
	def password(self):
		return self._password

	@password.setter
	def setPassword(self, password):
		if password is not None:
			self._password = sha256_crypt.encrypt(password)
			
	def checkPassword(self, password):
		return sha256_crypt.verify(password, self.password)

	def __init__(self, name, password, email):
		self.name = name
		self.setPassword = password
		self.email = email

	def __repr__(self):
		return "(User.id = %s, User.name = %s, User.email = %s)" % (self.id, self.name, self.email)

	@