#!/bin/bash
set -x

now_day=$(date -d "1 day ago" +%Y%m%d)

function alert()
{
    ret=$1
    script=$2
    if [ $ret -ne 0 ];then
        python send_message.py "AD $script error"
    fi
}

function file_exist()
{
dname=$1
while [ 1 ];do
  f_succ=$(aws s3 ls $dname/_SUCCESS)
  if [ -n "$f_succ" ];then
    echo $dname, $f_succ
    break
  fi
  sleep 60
done
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
    $now_day

    ret=$?
    alert $ret $script_name
}

function tf_submit_task()
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
    --archives lib.zip#lib \
    --conf spark.executor.extraLibraryPath=lib \
    $script_name \
    $now_day

    ret=$?
    alert $ret $script_name
}


function get_train_data()
{
  file_exist s3://sprs.push.us-east-1.prod/data/warehouse/sprs_ad_dwd/imp_click_install_temp/dt=$now_day
  submit_task join_train_feature.py
  tf_submit_task tf_feature_analysis.py
}

get_train_data
