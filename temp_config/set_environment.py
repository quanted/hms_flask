import os
import sys
import logging
from dotenv import load_dotenv
import requests

logging.warning("set_environment.py")


class DeployEnv(object):
	"""
	Class for determining deploy env for running
	QED-CTS, et al. with.
	"""

	def __init__(self, language=None):
		self.docker_hostname = os.environ.get('DOCKER_HOSTNAME')
		self.hostname = os.environ.get('HOSTNAME')
		self.test_url = os.environ.get('EPA_ACCESS_TEST_URL')
		# self.test_url = 'https://134.67.114.2'
		if not self.test_url:
			self.test_url = 'https://134.67.114.2'
		self.language = language  # default assumption is python, could be nodejs though

	def load_deployment_environment(self):
		"""
		Determines if QED-CTS components are being deployed
		inside/outside epa network, with/without docker, prod, etc.
		"""

		logging.warning("DOCKER_HOSTNAME: {}".format(self.docker_hostname))
		logging.warning("HOSTNAME: {}".format(self.hostname))

		_env_file = ''  # environment file name
		_is_public = False

		if self.docker_hostname == "ord-uber-vm001" or self.docker_hostname == "ord-uber-vm003":
			# deploy with docker_prod.env on cgi servers
			logging.warning("Deploying on prod server...")
			_env_file = 'docker_prod.env'

			if self.docker_hostname == "ord-uber-vm003":
				logging.info("Deploying on public server.. Setting IS_PUBLIC to True..")
				_is_public = True  # only case to set True

		else:
			# determine if inside or outside epa network
			internal_request = None
			try:
				# simple request to qed internal page to see if inside epa network:
				logging.warning("Testing for epa network access..")
				internal_request = requests.get(self.test_url, verify=False, timeout=1)
			except Exception as e:
				logging.warning("Exception making request to qedinternal server...")
				logging.warning("User has no access to cgi servers at 134 addresses...")

			logging.warning("Response: {}".format(internal_request))

			if internal_request and internal_request.status_code == 200:
				logging.warning("Inside epa network...")
				if not self.docker_hostname:
					logging.warning("DOCKER_HOSTNAME not set, assumming local deployment...")
					logging.warning("Deploying with local epa environment...")
					# self.read_env_file('local_epa')
					_env_file = 'local_epa.env'
				else:
					# logging.warning("DOCKER_HOSTNAME: {}, Deploying with epa docker environment...")
					# self.read_env_file('docker_epa')
					_env_file = 'docker_epa.env'
			else:
				logging.warning("Assuming outside epa network...")
				if not self.docker_hostname:
					logging.warning("DOCKER_HOSTNAME not set, assumming local deployment...")
					logging.warning("Deploying with local non-epa environment...")
					# self.read_env_file('local_outside')
					_env_file = 'local_outside.env'
				else:
					logging.warning("DOCKER_HOSTNAME: {}, Deploying with non-epa docker environment...")
					# self.read_env_file('docker_outside')
					_env_file = 'docker_outside.env'

		os.environ['IS_PUBLIC'] = str(_is_public)  # Add public bool to env for login if public

		logging.warning("loading env vars from: {}".format(_env_file))
		return self.read_env_file(_env_file)

	def read_env_file(self, env_file):
		"""
		Loads .env file env vars to be access with os.environ.get
		"""
		# logging.warning("Looking for .env file at {}".format(env_file))
		logging.warning("env file: " + env_file)

		if self.language != "nodejs":
			# set env vars with python-dotenv
			dotenv_path = 'temp_config/' + env_file
			load_dotenv(dotenv_path)

		return env_file
		

# if __name__ == '__main__':
# 	"""
# 	Handling calls to set_environment as main.
# 	Example case: called by nodejs
# 	"""

# 	additional_arg = sys.argv[1]
# 	if additional_arg == "nodejs":
# 		# return env vars to nodejs, print statements are read 
# 		# by nodejs via output stream
# 		runtime_env = DeployEnv("nodejs")
# 		print runtime_env.load_deployment_environment()  # sent to cts_nodejs via stdout
# 	elif additional_arg == "python":
# 		runtime_env = DeployEnv()
# 		runtime_env.load_deployment_environment()  # set env vars for python env

