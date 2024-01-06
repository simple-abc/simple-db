#!/bin/bash

set -eu

PACKAGE_NAME=com.northeastern.edu.simpledb.client.Launcher

mvn exec:java -Dexec.mainClass="$PACKAGE_NAME"