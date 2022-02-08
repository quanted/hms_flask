import os
import sys

# Change working directory so relative paths (and template lookup) work again
os.chdir(os.path.dirname(__file__))
curr_wd = os.getcwd()
print("WSGI Current working directory: {}".format(curr_wd))

print(sys.path)
import flask_hms

application = flask_hms.app
