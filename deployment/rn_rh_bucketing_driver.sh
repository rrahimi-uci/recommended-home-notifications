#!/usr/bin/env bash
set -e
set -x
echo "task_token:$1"
echo "env:$2"

task_token=$1
env=$2

spark-submit --deploy-mode cluster \
            --verbose \
            --conf spark.yarn.maxAppAttempts=1 \
            --conf spark.sql.autoBroadcastJoinThreshold=-1 \
            --conf spark.executor.memoryOverhead=6g \
            --conf yarn.nodemanager.vmem-check-enabled=false \
            --conf yarn.nodemanager.pmem-check-enabled=false \
            --py-files /mnt/rdc-recommended-notifications/rdc-recommended-notifications.zip  \
            /mnt/rdc-recommended-notifications/src/recommended_homes/rh_bucketing.py --task_token $task_token --env $env &

PID=$! #catch the last PID, here from the above command
echo "PID of the spark-submit job:${PID}"
wait ${PID} #wait for the above command