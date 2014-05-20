'''
some configs for hadoop and spark
'''

handleFileName = 'segmentInfo'
renderFile = '/home/mfkiller/code/spark_cloud/src/cloudComputing/render.py'
mapperFilePath = '/home/mfkiller/code/spark_cloud/src/cloudComputing/mapper.py'
reducerFilePath = '/home/mfkiller/code/spark_cloud/src/cloudComputing/reducer.py'
configFilePath = '/home/mfkiller/code/spark_cloud/src/cloudComputing/config.py'

fileSuffix = '.zip'
hdfsFolder = '/home/mfkiller/code/spark_cloud/hdfs/'

hdfsLogFolder = '/home/mfkiller/code/spark_cloud/logs/'

sparkUrl = 'spark://Master:7077'
beforeFairFile = '/home/mfkiller/code/spark_cloud/src/cloudComputing/beforeFair.py'
fairFile = '/home/mfkiller/code/spark_cloud/src/cloudComputing/fair.py'
afterFairFile = '/home/mfkiller/code/spark_cloud/src/cloudComputing/afterFair.py'
fairFileSuffix = '.obj'