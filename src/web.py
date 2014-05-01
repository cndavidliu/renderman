from flask import Flask
from flask import abort, redirect, url_for


app = Flask(__name__)

@app.route('/login', methods = ['GET', 'POST'])
def login():

