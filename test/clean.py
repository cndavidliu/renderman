"""
	clean db and maybe other things
"""
import os

def cleanDatabase():
	os.system('''
		sqlite3 /home/mfkiller/code/spark_cloud/database/test.db 'drop table job'
		''')
	os.system('''
		sqlite3 /home/mfkiller/code/spark_cloud/database/test.db 'drop table user'
		''')
	os.system('''
		sqlite3 /home/mfkiller/code/spark_cloud/database/test.db 'drop table config'
		''')

if __name__ == '__main__':
	cleanDatabase()