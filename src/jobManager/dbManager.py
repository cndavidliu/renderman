# -*- coding: utf-8 -*-
"""
some function about database here needed by jobManager
"""
from sqlalchemy import or_

from ..models import job, meta, user
from .config import *
from .fileManager import cleanFiles

def init():
	meta.init_models(SQL_URL)
	meta.init_db()

def update(target, state, finished = False):
	# need to select? need to test
	updateJob = job.Job.query.filter_by(id = target.id).first()
	updateJob.state = state
	if state == 'Running':
		updateJob.startTime = target.startTime
		if target.state == 'Retry':
			updateJob.retryTimes += 1
			updateJob.totalTime = 0
			updateJob.finishTime = None
	if finished:
		updateJob.finishTime = target.finishTime
		#print updateJob.finishTime, updateJob.startTime
		updateJob.setTotalTime()
	meta.db_session.commit()
	return updateJob

def commit(target = None):
	meta.db_session.commit()

def insert(target):
	meta.db_session.add(target)
	meta.db_session.commit()

def selectJob(num):
	jobs = job.Job.query.filter_by(state = 'Create')\
	           .order_by(job.Job.created_at).limit(num).all()
	return jobs

def checkJob():
	jobs = job.Job.query.filter(or_(job.Job.state == 'Create', job.Job.state == 'Wait', \
		job.Job.state == 'Running', job.Job.state == 'Retry' ))\
	           .order_by(job.Job.created_at).all()
	for updateJob in jobs:
		#here need to change this operator to other places maybe
		cleanFiles(updateJob)
		updateJob.state = job.jobStates[0]		
	meta.db_session.commit()