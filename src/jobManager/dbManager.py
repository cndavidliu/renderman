# -*- coding: utf-8 -*-
"""
some function about database here needed by jobManager
"""
from ..models import job, meta, user

def update(job):
	# need to select? need to test
	updateJob = Job.query.filter_by(id = job.id).first()
	updatJob = job
	meta.db_session.commit()