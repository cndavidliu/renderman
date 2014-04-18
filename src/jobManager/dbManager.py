# -*- coding: utf-8 -*-
"""
some function about database here needed by jobManager
"""
from ..models import job, meta, user
from sqlalchemy import or_

def updateJob(job):
	# need to select? need to test
	meta.db_session.commit()

def insertJob(job):
	meta.db_session.add(job)
	meta.db_session.commit()

def selectJob(num):
	jobs = job.Job.query.filter_by(state = 'Create')\
	           .order_by(job.Job.created_at).limit(num).all()
	return jobs

def checkJob():
	jobs = job.Job.query.filter(or_(job.Job.state == 'Create', job.Job.state == 'Wait', job.Job.state == 'Running'))\
	           .order_by(job.Job.created_at).all()
	for updateJob in jobs:
		updateJob.state = job.jobStates[0]
	meta.db_session.commit()