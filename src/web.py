from flask import Flask
from flask import abort, redirect, url_for, request, render_template, session
from sqlalchemy import or_
from werkzeug.utils import secure_filename
import os

from .config import SERVER_IP, DATABASE_URL, mapTaskCounts, pixels, maxStore, threadCount, failedCount, retryCount, ALLOWED_EXTENSIONS, serverFolder, redirectCommand
from .models import meta, user, job, config
from ..test.clean import cleanDatabase
from .jobManager import jobManager, dbManager


app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

isRegisterSuccess = False
manager = None

def init():
	#cleanDatabase()
	meta.init_models(DATABASE_URL)
	#meta.init_db()
	global manager
	manager = jobManager.JobManager(maxStore, threadCount, failedCount, retryCount)

def checkFile(filename):
	if '.' in filename:
		fileSuffix = filename.rsplit('.', 1)[1]
		if fileSuffix in ALLOWED_EXTENSIONS:
			return fileSuffix
	return None


def getPixel(pixel):
	strPixels = pixel.split('x')
	intPixel = []
	for strPixel in strPixels:
		intPixel.append(int(strPixel.strip()))
	return intPixel

@app.route('/login', methods = ['GET', 'POST'])
def login():
	userName = None
	if request.method == 'GET':
		global isRegisterSuccess
		if isRegisterSuccess:
			isRegisterSuccess = False
			return render_template('login.html', userName = userName, registerInfo = "Regiser successfully, login please")
		return render_template('login.html', userName = userName)
	if request.method == 'POST':
		identification = ''
		password = ''
		if 'password' in request.form:
			password = request.form['password']
		if 'identification' in request.form:
			identification = request.form['identification']
		errorMessage = None
		if identification == '' or password == '':
			errorMessage = "The password or ID is empty."
			return render_template('login.html', identification = identification, password = password, errorMessage = errorMessage)
		users =  user.User.query.filter(or_(user.User.name == identification, user.User.email == identification)).all()
		if len(users) == 0:
			errorMessage = "The email or name is not existed."
			return render_template('login.html', errorMessage = errorMessage)
		if not users[0].checkPassword(password):
			errorMessage = "The password is wrong!"
			return render_template('login.html', errorMessage = errorMessage, identification = identification)
		session['username'] = users[0].name
		session['userid'] = users[0].id
		return redirect(url_for('home'))

@app.route('/register', methods = ['GET', 'POST'])
def register():
	ages = [i for i in xrange(18,80)]
	if request.method == 'GET':
		return render_template('register.html', ages = ages)
	if request.method == 'POST':
		userName = ''
		userEmail = ''
		userPassword = ''
		modifyPassword = ''
		flag = False
		errorMessage = ['', '', '', '']
		if 'name' in request.form:
			userName = request.form['name']
		if 'email' in request.form:
			userEmail = request.form['email']
		if 'password' in request.form:
			userPassword = request.form['password']
		if 'modifyPassword' in request.form:
			modifyPassword = request.form['modifyPassword']
		registerUser = user.User(userName, userPassword, userEmail)
		registerUser.age = int(request.form['age'])
		registerUser.sex = int(request.form['sex'])
		if userName == '' or userPassword == '' or userEmail == '' or modifyPassword == '':
			errorMessage[0] = 'The field that signed by * must be filled!'
			flag = True
		else:
			if not user.User.judgeName(userName):
				errorMessage[1] = "Name's length must between 4 and 10 and be consisted by eng or num"
				flag = True
			else:
				users = user.User.query.filter_by(name = userName).all()
				if len(users) != 0:
					errorMessage[1] = "This name has been registered!"
					flag = True
			if user.User.judgeEmail(userEmail) is None:
				errorMessage[2] = 'The email address you input is not valid'
				flag = True
			else:
				users = user.User.query.filter_by(email = userEmail).all()
				if len(users) != 0:
					errorMessage[2] = "This email has been registered!"
					flag = True
			if not user.User.judgePassword(userPassword):
				errorMessage[3] = "Password's length must between 6 and 12"
				flag = True
			elif modifyPassword != userPassword:
				errorMessage[3] = 'The password you input twice is not equal'
				flag = True
		if flag:
			return render_template('register.html', ages = ages, errorMessage = errorMessage, modifyPassword = modifyPassword, name = userName, email = userEmail, \
				password = userPassword, age = registerUser.age, sex = registerUser.sex)
		else:
			os.system('mkdir ' + serverFolder + userName + redirectCommand)
			dbManager.insert(registerUser)
			global isRegisterSuccess
			isRegisterSuccess = True
			return redirect(url_for('login'))

@app.route('/')
@app.route('/home')
def home():
	userName = None
	userId = None
	if 'username' in session:
		userName = session['username']
		userId = int(session['userid'])
	return render_template('home.html', userName = userName, userId = userId)

@app.route('/logout')
def logout():
	if 'username' in session:
		session.pop('username', None)
		session.pop('userid', None)
	return redirect(url_for('home'))

@app.route('/projectCenter')
def projectCenter():
	if 'userid' not in session:
		return redirect(url_for('login'))
	userName = session['username']
	userId = int(session['userid'])
	return render_template('projectCenter.html', userName = userName, userId = userId)

@app.route('/createProject', methods = ['GET', 'POST'])
def createProject():
	if 'userid' not in session:
		return redirect(url_for('login'))
	userName = session['username']
	userId = int(session['userid'])
	if request.method == 'GET':
		return render_template('createProject.html', userName = userName, userId = userId, mapTaskCounts = mapTaskCounts, pixels = pixels)
	if request.method == 'POST':
		jobName = ''
		sourceFile = None
		description = ''
		jobType = int(request.form['jobType'])
		errorMessage = ['', '']
		flag = False
		if 'jobName' in request.form:
			jobName = request.form['jobName']
		if 'description' in request.form:
			description = request.form['description']
		if jobName == '':
			errorMessage[0] = "JobName is necessary!"
			flag = True
		elif len(jobName) < 4 or len(jobName) > 12:
			errorMessage[0] = "The  length of jobName is illegal!"
			flag = True
		else:
			jobs =  job.Job.query.filter(or_(job.Job.name == jobName, job.Job.jobType == jobType, job.Job.user_id == userId)).all()
			if len(jobs) != 0:
				flag = True
				errorMessage[0] = 'You have used this name in this kind of job!'
		if 'file' in request.files:
			sourceFile = request.files['file']
		if  not sourceFile:
			errorMessage[1] = "Source file is necessary!"
			flag = True
		else: 
			fileSuffix = checkFile(sourceFile.filename)
			if not fileSuffix:
				errorMessage[1] = "Source file's type is illegal!"
				flag = True
		if flag:		
			return render_template('createProject.html', userName = userName, userId = userId, jobName = jobName, description = description, \
				jobType = jobType, mapTaskCount = int(request.form['mapTaskCount']), pixel = request.form['pixel'], \
				mapTaskCounts = mapTaskCounts, pixels = pixels, errorMessage = errorMessage)
		else:
			newJob = job.Job(jobName, sourceFile.filename, jobType)
			newJob.user_id = userId
			sourceFilePath = serverFolder + userName + '/' + newJob.getJobName() + '.' + fileSuffix
			newJob.sourceFile = sourceFilePath
			sourceFile.save(sourceFilePath)
			#jobConfig = config.Config(int(request.form['memory']), int(request.form['cores']))
			jobConfig = config.Config()
			if jobType == 0:
				pixelInfo = getPixel(request.form['pixel'])
				jobConfig.setRenderConfig(int(request.form['mapTaskCount']), pixelInfo[0], pixelInfo[1])
			# submit job
			global manager
			manager.submitJob(newJob, jobConfig)
			return redirect(url_for('projectCenter'))


if __name__  == '__main__':
	init()
	app.debug == True
	app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'
	app.run(host = SERVER_IP)