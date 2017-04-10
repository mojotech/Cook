#!/bin/bash
set -ev

export PROJECT_DIR=`pwd`
$PROJECT_DIR/../travis/start_scheduler.sh
sleep 60

cd $PROJECT_DIR
python setup.py nosetests

$PROJECT_DIR/../travis/show_executor_logs.sh
