'''
The module used to prapare fair part
'''
import sys
import os

objFileName = sys.argv[1]

handleObjFileName = objFileName + '-handle'

objFile = open(objFileName, 'r')
handleObjFile = open(handleObjFileName, 'w+')

line = objFile.readline()
i = 1
while line != '':
	if line[0:2] == 'v ':
		line = str(i) + ' ' + line
		i += 1
	handleObjFile.write(line)
	line = objFile.readline()

objFile.close()
handleObjFile.close()

#change obj file content
os.system('mv ' + handleObjFileName + ' ' + objFileName)