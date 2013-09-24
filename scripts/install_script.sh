#!/usr/bin/env bash

# update apt packages
sudo apt-get update -qq

# install mysql-python connector
sudo apt-get install -y python-mysqldb

# install redwood man page
sudo cp docs/_build/man/redwood.1 /usr/share/man/man1/redwood.1.gz
