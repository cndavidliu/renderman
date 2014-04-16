from ..src import jobManager
from .dbTest import cleanDatabase

jobManager = jobManager.JobManager()

def init():
	cleanDatabase()
	models.init_models('sqlite:////home/mfkiller/code/spark_cloud/database/test.db')
	models.init_db()
	user = User('davis', 'davis', 'test@gmail.com')
	sampleJob = Job('testJob', '/usr/lib/test.mat', -1)