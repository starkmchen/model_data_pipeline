#!/bin/bash

now_day=$1
now_hour=$now_day$2

function alert()
{
    ret=$1
    script=$2
    if [ $ret -ne 0 ];then
        python send_message.py "AD $script error"
    fi
}

function submit_task()
{
    script_name=$1
    spark-submit \
    --deploy-mode cluster \
    --driver-memory 15G \
    --master yarn \
    --conf spark.speculation.quantile=0.9 \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.yarn.maxAppAttempts=1 \
    --conf spark.driver.maxResultSize=15G \
    --conf spark.executor.memoryOverhead=1G \
    --conf spark.sql.files.maxPartitionBytes=134217728  \
    --executor-memory 15G \
    --conf spark.executor.instances=100 \
    --conf spark.shuffle.memoryFraction=0.4 \
    --executor-cores 1 \
    --py-files feature_conf.py \
    $script_name \
    $now_day \
    $now_hour
}

function main()
{

submit_task get_ad_counter_hourly.py
ret=$?
alert $ret get_ad_counter_hourly.py

}

main > log/get_ad_counter_hourly.$now_hour 2>&1
