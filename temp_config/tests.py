import platform
import subprocess
# from django.test import TestCase
from unittest import TestCase
from set_environment import DeployEnv


print("temp_config/tests.py")


class TempConfigTestCase(TestCase):
	"""
	Unit tests for temp_config repo.
	Tests set_environment.py, set_env_vars.sh,
	and set_env_vars.bat for any errors, then
	validate to see env vars are set.
	"""
	def __init__(self):
		self.batch_filename = "set_env_vars.bat"
		self.shell_filename = "set_env_vars.sh"
		self.test_env_filename = "local_outside.env"


	def setUp(self):
		"""
		Set up a default test scenario for temp_config
		"""
		print("setUp")


	def test_set_environment(self):
		"""
		Runs load_deployment_environment() function from
		set_environment.py and checks for errors.
		"""
		runtime_env = DeployEnv()
		func_response = runtime_env.load_deployment_environment()
		if '.env' in func_response:
			# todo: more robust test, like checking if an env var is set
			print("set_environment test passed! Using {} env file".format(func_response))
			return True
		else:
			print("set_enviornemtn test failed... Using {} env file".format(func_response))
			return False


	def test_scripts(self):
		"""
		Tests set_env_vars, .sh or .bat, depending on platform
		code is running on (windows or unix)
		"""
		os_type = platform.system()
		print("system platform: ".format(os_type))

		try:
			if os_type == "Windows":
				return self.eval_test_scripts(self.batch_filename, self.test_env_filename, os_type)
			elif os_type == "Linux":
				return self.eval_test_scripts(self.shell_filename, self.test_env_filename, os_type)
			else:
				# raise hell, only testing for above two cases (sorry, mac)
				print("Test failed, os_type {} undetermined...".format(os_type))
				return False
		except Exception as e:
			print("test_scripts test failed... exception: {}".format(e))
			return False


	def eval_test_scripts(self, script_filename, env_filename, os_type):
		"""
		Function for test_scripts, does subprocess.call for running
		script file depending on OS platform
		"""
		try:
			print("testing subprocess.call for {} script and {} env var file".format(script_filename, env_filename))
			p = subprocess.call([script_filename, env_filename])
		except Exception as e:
			print("Test failed at eval_test_scripts making subprocess.call")
			return False

		print("returned value from subprocess.call: {}".format(p))

		if p == 0:
			print("set_env_vars test for {} passed!".format(os_type))
			return True
		else:
			print("set_env_vars test for {} failed...".format(os_type))
			return False
