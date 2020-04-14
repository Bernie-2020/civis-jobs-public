FROM python:3.7

# Avoid warnings by switching to noninteractive
ENV DEBIAN_FRONTEND=noninteractive

# This Dockerfile adds a non-root user with sudo access. Use the "remoteUser"
# property in devcontainer.json to use it. On Linux, the container user's GID/UIDs
# will be updated to match your local UID/GID (when using the dockerFile property).
# See https://aka.ms/vscode-remote/containers/non-root-user for details.
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

ENV PIP_TARGET=/usr/local/share/pip-global
ENV PYTHONPATH=${PYTHONPATH}:${PIP_TARGET}:/workspaces/civis-jobs
ENV PATH=${PATH}:${PIP_TARGET}/bin

# Uncomment the following COPY line and the corresponding lines in the `RUN` command if you wish to
# include your requirements in the image itself. It is suggested that you only do this if your
# requirements rarely (if ever) change.
# COPY requirements.txt /tmp/pip-tmp/

# Configure apt and install packages
RUN apt-get update \
    && apt-get -y install --no-install-recommends apt-utils dialog 2>&1 \
    #
    # Verify git, process tools, lsb-release (common in install instructions for CLIs) installed
    && apt-get -y install git iproute2 procps lsb-release \
    #
    # Install pylint
    && pip --disable-pip-version-check --no-cache-dir install pylint \
    #
    # Install libraries we will need
    && pip --disable-pip-version-check --no-cache-dir install psycopg2 \
    && pip --disable-pip-version-check --no-cache-dir install awscli \
    && pip --disable-pip-version-check --no-cache-dir install jinjasql \
    && pip --disable-pip-version-check --no-cache-dir install pandas \
    && pip --disable-pip-version-check --no-cache-dir install requests \
    && pip --disable-pip-version-check --no-cache-dir install parsons \
    && pip --disable-pip-version-check --no-cache-dir install yapf \
    #
    # Telnet
    && apt-get -y install telnet \
    # Postgres Client
    && apt-get -y install postgresql-client \
    #
    # Create a non-root user to use if preferred - see https://aka.ms/vscode-remote/containers/non-root-user.
    && groupadd --gid $USER_GID $USERNAME \
    && useradd -s /bin/bash --uid $USER_UID --gid $USER_GID -m $USERNAME \
    # [Optional] Add sudo support for the non-root user
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME\
    && chmod 0440 /etc/sudoers.d/$USERNAME \
    #
    # Create alternate global install location that both uses have rights to access
    && mkdir -p /usr/local/share/pip-global \
    && chown ${USERNAME}:root /usr/local/share/pip-global \
    #
    # Install fish
    && echo 'deb http://download.opensuse.org/repositories/shells:/fish:/release:/3/Debian_10/ /' > /etc/apt/sources.list.d/shells:fish.list \
    && curl -L -sS https://download.opensuse.org/repositories/shells:fish:release:3/Debian_10/Release.key | apt-key add - 2>/dev/null \
    && apt-get install -y fish \
    && chsh -s /usr/bin/fish $USERNAME \
    #
    # Set up a link from /app to the workspace
    && mkdir /workspaces \
    && ln -s /app /workspaces/civis-jobs \
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

# Set up persistent data home dir
# Map this to a
RUN mkdir /datahome \
    && chown $USERNAME /datahome
ENV XDG_DATA_HOME=/datahome
ENV EDITOR="code -w"

# Switch back to dialog for any ad-hoc use of apt-get
ENV DEBIAN_FRONTEND=dialog
