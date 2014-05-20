import os

from ..src.jobManager.config import hostNames, systemLogFile, cleanLogFile
from ..src.cloudComputing.config import hdfsFolder, hdfsLogFolder
from ..src.config import serverFolder, downloadFolder, DATABASE_URL
from ..src.models import meta, user, job, config

#redirectCommand = ' >>' + cleanLogFile + ' 2>>' + cleanLogFile

def initDb():
	meta.init_models(DATABASE_URL)
	#meta.init_db()

def cleanJobFiles(job):
	if job.isRender():
		jobName = job.getJobName()
		os.system("hadoop fs -rmr " + jobName)
		# here the input suffix needs to change to config file maybe
		os.system('rm -rf ' + hdfsFolder + jobName + '-input')
		os.system('rm -rf ' + hdfsLogFolder + jobName + '.log')
		for hostName in hostNames:
			cleanCommand = ('''ssh %s "rm -rf %s%s"''') % (hostName, hdfsFolder, jobName)
			cleanOutput = ('''ssh %s "rm -rf %s%s-output"''') % (hostName, hdfsFolder, jobName)
			os.system(cleanCommand)
			os.system(cleanOutput)
	elif job.isFair():
		jobName = job.getJobName()
		os.system("hadoop fs -rmr " + jobName)
		os.system('rm -rf ' + hdfsFolder + jobName)
		os.system('rm -rf ' + hdfsLogFolder + jobName + '.log')


def cleanUserFiles(user):
	userName = user.name
	os.system('rm -rf ' + serverFolder + userName)
	os.system('rm -rf ' + downloadFolder + userName)


def selectJobs():
	 jobs = job.Job.query.all()
	 return jobs

def selectUsers():
	users = user.User.query.all()
	return users

def cleanLogFiles():
	os.system('rm -f ' + systemLogFile)
	os.system('rm -f ' + cleanLogFile)

if __name__ == '__main__':
	initDb()
	cleanLogFiles()
	users = selectUsers()
	jobs = selectJobs()
	for removeJob in jobs:
		cleanJobFiles(removeJob)
	for removeUser in users:
		cleanUserFiles(removeUser)