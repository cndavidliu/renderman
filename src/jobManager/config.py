"""
	here are some configs about jobManager
"""

SQL_URL = 'sqlite:////home/mfkiller/code/spark_cloud/database/test.db'

systemLogFolder = '/home/mfkiller/code/spark_cloud/logs/'

systemLogFile = '/home/mfkiller/code/spark_cloud/logs/system.log'
cleanLogFile = '/home/mfkiller/code/spark_cloud/logs/clean.log'
systemLogConf = '/home/mfkiller/code/spark_cloud/src/jobManager/systemLog.conf'

##cluster information
hostNames = ['Master', 'Slave1']

#config used for log detection
logKeyWord = ['Streaming Command Failed!', 'complete']