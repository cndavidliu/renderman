# -*- coding: utf-8 -*-
import os
if __name__ == '__main__':
	for dir, folders, files in os.walk('..'):
		for file in files:
			root, ext = os.path.splitext(file)
			if ext == '.pyc':
				print 'Remove: ' + os.path.abspath(os.path.join( dir, file ))
				os.remove( os.path.abspath(os.path.join( dir, file ) ) )