"""
reduce python code used to handle rendering reduce 
"""

import sys
import Image
import os

jobName = os.environ["jobName"]
width = os.environ["width"]
height = os.environ["height"]
mapTaskCount = os.environ["mapTaskCount"]

"""
jobName = sys.argv[1]
width = sys.argv[2]
height = sys.argv[3]
mapTaskCount = sys.argv[4]
"""

srcFile = jobName + config.fileSuffix

os.system("mkdir " + jobName)

for line in sys.stdin:
	line = line.strip()
	resultInfo.append(line)
	os.system("hadoop fs -get " + jobName + "/img/" + line + " ./" + jobName + "/")

resultInfo.sort()
renderImg = Image.new('RGB', (width, height))
box = (0, 0, width, height / mapTaskCount)

for i in xrange(mapTaskCount):
	partImage = Image.open("./" + jobName + "/" + resultInfo[i])
	partBox = (0, i * height / mapTaskCount, width, (i + 1) * height / mapTaskCount)
	partRegion = partImage.crop(box)
	renderImg.paste(partRegion, partBox)

renderImg.save(jobName + "/result.png")
os.system("hadoop fs -put " + jobName + "/result.png " + jobName + "/img")