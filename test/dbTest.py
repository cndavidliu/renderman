# -*- coding: utf-8 -*-
from ..src import models
import os
from datetime import datetime
from time import sleep
# init db test

def cleanDatabase():
	os.system('''
		sqlite3 /home/mfkiller/code/spark_cloud/database/test.db 'drop table job'
		''')
	os.system('''
		sqlite3 /home/mfkiller/code/spark_cloud/database/test.db 'drop table user'
		''')

def main():
	cleanDatabase()

	models.init_models('sqlite:////home/mfkiller/code/spark_cloud/database/test.db')
	models.init_db()

	# insert db test
	user = models.user.User('dave', '123456', 'test@qq.com')
	models.meta.db_session.add(user)
	models.meta.db_session.commit()

	job = models.job.Job('testJob', '/home/mfkiller/test.rc', 1)
	job.user_id = user.id
	models.meta.db_session.add(job)
	models.meta.db_session.commit()

	getUser = models.user.User.query.filter_by(id = 1).first()
	getJob = models.job.Job.query.filter_by(id = 1).first()

	print getUser
	print "####job insert and relationship test result:"
	print 'job inserted: ', getUser.jobs[0] == getJob, getJob == job
	print "####user insert test result:"
	print 'user inserted:', getUser.name == user.name, getUser.email == user.email

	# update db test
	getJob.name = 'updateJob'
	getUser.name = 'davis'
	models.meta.db_session.commit()

	getUser = models.user.User.query.filter_by(id = 1).first()
	getJob = models.job.Job.query.filter_by(id = 1).first()

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
	models.meta.db_session.commit()

	getJob = models.job.Job.query.filter_by(id = 1).first()
	print getJob.startTime, getJob.finishTime
	print getJob.startTime == startTime, getJob.finishTime == finishTime 
	getJob.setTotalTime()
	print getJob.totalTime == 2
	models.meta.db_session.commit()
	# relationship test


	# check password test
	print '###check password test:'
	print getUser.password, getUser.checkPassword('123456')

if __name__ == '__main__':
	main()