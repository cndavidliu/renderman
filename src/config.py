"""
here are some configs for web server or for all modules
"""

SERVER_IP = '10.76.2.38'
DATABASE_URL = 'sqlite:////home/mfkiller/code/spark_cloud/database/test.db'

#project config parameters
mapTaskCounts = [2, 4, 8, 16, 32, 64]
pixels = ['320 x 480', '600 x 800', '800 x 1200', '1000 x 1400', '1200 x 1600', '1600 x 2000', \
'1600 x 2400', '1600 x 2800']
overTimes = [1200, 60, 300, 600, 900, 1500, 1800]

#jobmanager config
maxStore = 3
threadCount = 2	
failedCount = 2
retryCount = 2

# upload file config
ALLOWED_EXTENSIONS = set(['zip'])
serverFolder = '/home/mfkiller/code/spark_cloud/static/'
downloadFolder = '/home/mfkiller/code/spark_cloud/src/static/downloads/'
relativeDownloadFolder = 'downloads/'

redirectCommand = " >/dev/null 2>&1"