#!/usr/bin/env sh

# Download and install MongoDB
MONGODB_VERSION=$1
MONGODB_FILE=mongodb-osx-x86_64-${MONGODB_VERSION}

wget http://fastdl.mongodb.org/osx/${MONGODB_FILE}.tgz
tar xfz ${MONGODB_FILE}.tgz
export PATH=`pwd`/${MONGODB_FILE}/bin:$PATH
