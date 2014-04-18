# -*- coding: utf-8 -*-
from datetime import datetime
from sqlalchemy import create_engine, Column, DateTime
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import sessionmaker, scoped_session

db_session = scoped_session(sessionmaker(autocommit = False, autoflush = False))

class Model(object):
	"""
	Common colums are defined here
	"""

	@declared_attr
	def __tablename__(cls):
		return cls.__name__.lower()

	query = db_session.query_property()

	created_at = Column(DateTime, default = datetime.utcnow)
	updated_at = Column(DateTime, default = datetime.utcnow, onupdate = datetime.utcnow)

from sqlalchemy.ext.declarative import declarative_base

Model = declarative_base(cls = Model, name = 'Model')

def init_models(database_uri):
	"""
	This method must be called before using models.
	"""
	engine = create_engine(database_uri, connect_args = {'check_same_thread': False}, convert_unicode = True)
	db_session.configure(bind = engine)

def init_db():
	"""
	This method is used to init tables
	"""
	Model.metadata.create_all(bind = db_session.bind)
