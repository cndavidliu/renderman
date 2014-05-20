"""
here are some configs for web server or for all modules
"""

SERVER_IP = '10.76.2.38'
DATABASE_URL = 'sqlite:////home/mfkiller/code/spark_cloud/database/test.db'

#project config parameters
mapTaskCounts = [2, 4, 8, 16, 32, 64]
pixels = ['600 x 800', '800 x 1200', '1000 x 1400', '1200 x 1600', '1600 x 2000', \
'1600 x 2400', '1600 x 2800', '2400 x 3600', '3600 x 4800', '4800 x 6400', '6400 x 8000', '8000 x 12000']
overTimes = [1800, 600, 1200, 2400, 3000, 3600]

memorys = ['4', '6', '8', '10', '15', '20', '25', '30']
cores = ['10', '2', '4', '6', '8']

objLambdas = ['0.5', '0.25', '0.4', '0.6', '0.75',  '0.8']
repeatTimes = ['2', '5', '10', '15', '20']

#jobmanager config
maxStore = 3
threadCount = 2	
failedCount = 2
retryCount = 2

# upload file config
ALLOWED_EXTENSIONS = ['zip', 'obj']
serverFolder = '/home/mfkiller/code/spark_cloud/static/'
downloadFolder = '/home/mfkiller/code/spark_cloud/src/static/downloads/'
relativeDownloadFolder = 'downloads/'

redirectCommand = " >/dev/null 2>&1"