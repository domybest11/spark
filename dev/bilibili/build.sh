#!/usr/bin/env bash

source /etc/profile
set -e

_info() { echo ">>> $@" >&2 ; }

# This script must be started at source root.
[[ -f CONTRIBUTING.md ]] || { echo "*** ERROR: not in source root" >&2 && exit 1; }

cd build
echo ${pwd}
_info "workdir ${pwd}"
_info "Downloading SDKs..."
curl -sO http://cypress.bilibili.co/sdk/scala/apache-maven-3.5.4-bin.tar.gz
curl -sO http://cypress.bilibili.co/sdk/scala/scala-2.11.12.tgz
curl -sO http://cypress.bilibili.co/sdk/scala/zinc-0.3.15.tgz

_info "Extracting SDKs..."
tar fx apache-maven-3.5.4-bin.tar.gz
tar fx scala-2.11.12.tgz
tar fx zinc-0.3.15.tgz

cd ..
_info "Starting build...   dir ${pwd}"
echo ${pwd}
./dev/make-distribution.sh --name hadoop2.8 --tgz -PR -P2.8.4.1-bili-SNAPSHOT -Phive -Phive-thriftserver -Pyarn
rm -rf ./dist/bilibili
cp -r ./dev/bilibili ./dist/bilibili
