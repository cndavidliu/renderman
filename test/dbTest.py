# -*- coding: utf-8 -*-
from ..src import models
from datetime import datetime
from time import sleep
from .clean import cleanDatabase
from ..src.jobManager import dbManager
# init db test

def main():
	cleanDatabase()

	models.init_models('sqlite:////home/mfkiller/code/spark_cloud/database/test.db')
	models.init_db()

	# insert db test
	user = models.User('dave', '123456', 'test@qq.com')
	models.db_session.add(user)
	models.db_session.commit()

	job = models.Job('testJob', '/home/mfkiller/test.rc', 1)
	job.user_id = user.id
	models.db_session.add(job)
	models.db_session.commit()

	getUser = models.User.query.filter_by(id = 1).first()
	getJob = models.Job.query.filter_by(id = 1).first()

	print getUser
	print "####job insert and relationship test result:"
	print 'job inserted: ', getUser.jobs[0] == getJob, getJob == job
	print "####user insert test result:"
	print 'user inserted:', getUser.name == user.name, getUser.email == user.email

	# update db test
	getJob.name = 'updateJob'
	getUser.name = 'davis'
	models.db_session.commit()

	getUser = models.User.query.filter_by(id = 1).first()
	getJob = models.Job.query.filter_by(id = 1).first()

	print "###update test result:"
	print 'user.name changed:', getUser.name == 'davis', user == getUser, user.name == 'davis'
	print 'job.name changed:', getJob.name == 'updateJob', getUser.jobs[0].name == 'updateJob'

	#set datetime test
	print '###datetime insert, just used to confirm'
	startTime = datetime.now()
	sleep(2)
	finishTime = datetime.now()
	getJob.startTime = startTime
	getJob.finishTime = finishTime
	models.db_session.commit()

	getJob = models.Job.query.filter_by(id = 1).first()
	print getJob.startTime, getJob.finishTime
	print getJob.startTime == startTime, getJob.finishTime == finishTime 
	getJob.setTotalTime()
	print getJob.totalTime == 2
	models.db_session.commit()
	# relationship test


	# check password test
	print '###check password test:'
	print getUser.password, getUser.checkPassword('123456')

	#job function test
	print getJob.created_at
	print getJob
	getJob.state = 'Running'
	getJob.overTime = 1
	print 'whether overTime:', getJob.isOverTime()


def dbManagerTest():
	job = models.Job('testJob', '/home/mfkiller/test.rc', 1)
	dbManager.insert(job)
	sleep(2)
	anotherJob = models.Job('anotherJob', '/home/mfkiller/test.rc', 1)
	dbManager.insert(anotherJob)
	jobs = dbManager.selectJob(5)
	print jobs
	dbManager.checkJob()
	checkJobs = dbManager.selectJob(5)
	print checkJobs
	# use db select sentence instead of 1
	print 'check num:', len(checkJobs) - len(jobs) == 1

if __name__ == '__main__':
	main()
	dbManagerTest()