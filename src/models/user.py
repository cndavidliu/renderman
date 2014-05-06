from sqlalchemy import Column, Integer, Sequence, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property
import re

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

	@staticmethod
	def judgeName(userName):
		if len(userName) < 4 or len(userName) > 10:
			return False
		else:
			for i in xrange(0, len(userName)):
				if (userName[i] >= 'a' and userName[i] <= 'z') or (userName[i] >= 'A' and userName[i] <= 'Z') or (userName[i] >= '0' and userName[i] <= '9'):
					pass
				else:
					return False
			return True

	@staticmethod
	def judgePassword(userPassword):
		isReasonable = (len(userPassword) >= 6 and len(userPassword) <= 12)
		return isReasonable

	@staticmethod
	def judgeEmail(userEmail):
		return re.match("^.+\\@(\\[?)[a-zA-Z0-9\\-\\.]+\\.([a-zA-Z]{2,3}|[0-9]{1,3})(\\]?)$", userEmail)