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
    feature_date=$2
    label_date=$3
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
    $feature_date \
    $label_date

    ret=$?
    alert $ret $script_name
}

function tf_submit_task()
{
    script_name=$1
    day=$2
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
    $day

    ret=$?
    alert $ret $script_name
}


function extract_feature()
{
  feature_date=$now_day
  submit_task get_pkg_category.py $feature_date

  submit_task get_ad_counter.py $feature_date

  submit_task get_user_counter.py $feature_date

  submit_task get_user_profile.py $feature_date
}

function get_train_data()
{
  label_date=$now_day
  feature_date=$(date -d "1 day ago $label_date" +%Y%m%d)
  submit_task get_train_data.py $feature_date $label_date

  tf_submit_task tf_feature_analysis.py $now_day
}

function main()
{
file_exist s3://sprs.push.us-east-1.prod/data/warehouse/sprs_ad_dwd/user_app_list_temp
extract_feature
}

main > log/model_data_log.$now_day 2>&1
