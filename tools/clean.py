# -*- coding: utf-8 -*-
import os

for dir, folders, files in os.walk('..'):
    for file in files:
        root, ext = os.path.splitext(file)
        if ext == '.pyc':
            print os.path.abspath(os.path.join( dir, file ))
            os.remove( os.path.abspath(os.path.join( dir, file ) ) )