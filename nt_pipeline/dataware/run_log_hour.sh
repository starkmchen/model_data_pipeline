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
    $script_name \
    $now_date

    ret=$?
    alert $ret $script_name
}

function build_partition()
{
  sql="alter table sprs_ad_dwd.dwd_nt_rec_log_inc_hourly  add if not exists partition (dt='$now_day', hour='$now_hour')"
  hive -S -e "$sql"
  sql="alter table sprs_ad_dwd.dwd_nt_mixer_log_inc_hourly  add if not exists partition (dt='$now_day', hour='$now_hour')"
  hive -S -e "$sql"
  sql="alter table sprs_ad_dwd.dwd_nt_train_feature_hourly  add if not exists partition (dt='$now_day', hour='$now_hour')"
  hive -S -e "$sql"
}

submit_task parse_rec_log_hour.py
submit_task parse_mixer_log_hour.py
submit_task parse_train_feature_hourly.py
submit_task parse_req_ads_log_hour.py
build_partition
