# Development container

This is a Dockerfile/devcontainer.json for a uniform development environment.

The idea is that rather than managing python/pip madness on your local
development machine, run your development environment inside a container that
can be configured independently. This setup fits nicely into VSCode, but the
Dockerfile can be used without it.

Read more about VSCode development inside containers:
https://code.visualstudio.com/docs/remote/containers

# Configuring environment variables

Create a file at `civis-jobs-public/.devcontainer/devcontainer.env` with this
structure:
```
REDSHIFT_HOST=<db host>
REDSHIFT_DB=<db name>
REDSHIFT_PORT=<port>
PGHOST=<db host>
PGPORT=<port>
PGUSER=<your database username>
PGWORD_CREDENTIAL_PASSWORD=<your database password>
CIVIS_API_KEY=<your civis API key>
```

Notes: 
	* you must create the env file for the container to start
	* the `devcontainer.env` file needs to live at the root of your repo, so if this setup is used with a different repo you will need to change file location
	* the structure of the `devcontainer.env` file may need to be altered to fit a dfferent database setup


# Configuring credentials

AWS and SSH credentials should be bridged into the container by sharing the
.aws/.ssh directories.
