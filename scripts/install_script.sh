#!/usr/bin/env bash

# update apt packages
sudo apt-get update -qq

# install required python packages
sudo apt-get install -y python-matplotlib
sudo apt-get install -y python-mysqldb
sudo apt-get install -y python-numpy
sudo apt-get install -y python-scipy

# install redwood man page
sudo cp docs/_build/man/redwood.1 /usr/share/man/man1/redwood.1.gz
