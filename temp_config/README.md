Environment variables for QED-CTS

Two main types:

	+ docker_*.env -- docker-related environment variables
	+ local_*.env -- local dev environment variables

Three types within docker and local env vars:

	+ *_epa.env -- env vars when working with 80/443 access to cgi servers.
	+ *_outside.env -- env vars when working without access to cgi servers.
	+ *_prod.env -- env vars when deployed on production server in cgi network.

To manually set .env, run:

	+ linux: . set_env_vars.sh filename.env
	+ windows: config\set_env_vars.bat filename.env

Dynamically set env vars in python code:

	from temp_config.set_environment import DeployEnv

	runtime_env = DeployEnv()
	runtime_env.load_deployment_environment()  # set env vars based on network access