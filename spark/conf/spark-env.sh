#!/usr/bin/env bash

export SPARK_MASTER_HOST=spark-master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_WEBUI_PORT=8081
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=2g
export SPARK_DAEMON_MEMORY=1g
export SPARK_PID_DIR=/tmp/spark-pid
export SPARK_LOG_DIR=/tmp/spark-logs
export SPARK_LOCAL_DIRS=/tmp/spark-local
export HADOOP_CONF_DIR=/spark/conf
export YARN_CONF_DIR=/spark/conf
