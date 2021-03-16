#!/bin/bash
set -x

now_date=$(date -d "1 hour ago" +%Y%m%d%H)
now_day=$(date -d "1 hour ago" +%Y%m%d)
now_hour=$(date -d "1 hour ago" +%H)

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
    --conf spark.executor.extraLibraryPath=lib \
    $script_name \
    $now_date

    ret=$?
    alert $ret $script_name
}

function repostback_install()
{
file_exist s3://shareit.ads.ap-southeast-1/dw/table/dwd/dwd_adcs_adping_logs_detail_hour/dt=$now_day/hour=$now_hour/flag=repostback
submit_task parse_repostback_install.py
}

function imp_click()
{
file_exist s3://shareit.ads.ap-southeast-1/dw/table/dwb/dwb_adcs_cpi_logs_hour/dt=$now_day/hour=$now_hour
submit_task parse_imp_click.py
}

repostback_install &
imp_click &
