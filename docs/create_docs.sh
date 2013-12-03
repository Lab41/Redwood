#!/bin/bash

sphinx-apidoc -o . ../redwood -F
make html
make man

