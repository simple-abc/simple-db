#!/bin/bash

set -eu

PACKAGE_NAME=com.northeastern.edu.simpledb.backend.Launcher

mvn clean compile
mvn exec:java -Dexec.mainClass="$PACKAGE_NAME" -Dexec.args="-create simple-db"
mvn exec:java -Dexec.mainClass="$PACKAGE_NAME" -Dexec.args="-open simple-db"