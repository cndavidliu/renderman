#! /usr/bin/env python
import sys
import Image
import os
from config import hdfsFolder

'''
reduce python code used to handle rendering reduce 
'''
'''
jobName = os.environ['jobName']
width = int(os.environ['width'])
height = int(os.environ['height'])
mapTaskCount = int(os.environ['mapTaskCount'])
'''

jobName = sys.argv[1]
width = int(sys.argv[2])
height = int(sys.argv[3])
mapTaskCount = int(sys.argv[4])

os.system("mkdir " +  hdfsFolder + jobName + "-output")

resultInfo = []
# here used to test
#lines = ['output_000.png', 'output_001.png']
#for line in lines:
for line in sys.stdin:
	line = line.strip()
	resultInfo.append(line)
	os.system("$HADOOP_HOME/bin/hadoop fs -get " + jobName + "/img/" + line + " " + hdfsFolder + jobName + "-output/")

resultInfo.sort()
renderImg = Image.new('RGB', (width, height))

for i in xrange(mapTaskCount):
	partImage = Image.open(hdfsFolder + jobName + "-output/" + resultInfo[i])
	partBox = (0, i * height / mapTaskCount, width, (i + 1) * height / mapTaskCount)
	partRegion = partImage.crop(partBox)
	renderImg.paste(partRegion, partBox)

renderImg.save(hdfsFolder + jobName + "-output/" + jobName + ".png")
os.system("$HADOOP_HOME/bin/hadoop fs -put " + hdfsFolder + jobName + "-output/" + jobName + ".png " + jobName + "/img")
os.system("rm -rf " + hdfsFolder + jobName + "-output")