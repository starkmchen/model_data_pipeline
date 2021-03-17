#!/bin/bash
set -x
work_dir=$(pwd)

feature_date=$(date -d "1 day ago" +%Y%m%d)

function alert()
{
    ret=$1
    script=$2
    if [ $ret -ne 0 ];then
        python ${work_dir}/send_message.py "AD $script error"
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

function write_ad_counter()
{
  file_exist s3://sprs.push.us-east-1.prod/data/warehouse/model/ad_counter/$feature_date
  aws s3 sync s3://sprs.push.us-east-1.prod/data/warehouse/model/ad_counter/$feature_date ad_counter_dir
  cat ad_counter_dir/* > ad_counter
  rm -r ad_counter_dir

  python write_ad_counter.py
  ret=$?
  alert $ret write_ad_counter.py
  if [ $ret -ne 0 ];then
      return
  fi

  md5sum ad_counter.pb | awk '{print $1}' > ad_counter.pb.md5
  aws s3 cp ad_counter.pb s3://sprs-ads-sg1/pre/feature/ad_counter/$feature_date/ad_counter.pb
  aws s3 cp ad_counter.pb.md5 s3://sprs-ads-sg1/pre/feature/ad_counter/$feature_date/ad_counter.pb.md5
  aws s3 cp ad_counter.pb s3://sprs-ads-sg1/prod/feature/ad_counter/$feature_date/ad_counter.pb
  aws s3 cp ad_counter.pb.md5 s3://sprs-ads-sg1/prod/feature/ad_counter/$feature_date/ad_counter.pb.md5

  aws s3 cp ad_counter.pb s3://sprs-ads-sg1/pre/stochastic_model/ad_counter/$feature_date/ad_counter.pb
  aws s3 cp ad_counter.pb.md5 s3://sprs-ads-sg1/pre/stochastic_model/ad_counter/$feature_date/ad_counter.pb.md5
  aws s3 cp ad_counter.pb s3://sprs-ads-sg1/prod/stochastic_model/ad_counter/$feature_date/ad_counter.pb
  aws s3 cp ad_counter.pb.md5 s3://sprs-ads-sg1/prod/stochastic_model/ad_counter/$feature_date/ad_counter.pb.md5
}

function write_ad_info()
{
  file_exist s3://sprs.push.us-east-1.prod/data/warehouse/model/pkg_info/$feature_date
  aws s3 sync s3://sprs.push.us-east-1.prod/data/warehouse/model/pkg_info/$feature_date ad_info_dir
  cat ad_info_dir/* > ad_info
  rm -r ad_info_dir

  python write_ad_info.py
  ret=$?
  alert $ret write_ad_info.py
  if [ $ret -ne 0 ];then
      return
  fi

  md5sum ad_info.pb | awk '{print $1}' > ad_info.pb.md5
  aws s3 cp ad_info.pb s3://sprs-ads-sg1/pre/feature/pkg_info/$feature_date/pkg_info.pb
  aws s3 cp ad_info.pb.md5 s3://sprs-ads-sg1/pre/feature/pkg_info/$feature_date/pkg_info.pb.md5
  aws s3 cp ad_info.pb s3://sprs-ads-sg1/prod/feature/pkg_info/$feature_date/pkg_info.pb
  aws s3 cp ad_info.pb.md5 s3://sprs-ads-sg1/prod/feature/pkg_info/$feature_date/pkg_info.pb.md5
}

function write_user_counter()
{
    file_exist s3://sprs.push.us-east-1.prod/data/warehouse/model/user_counter/$feature_date
    aws s3 sync s3://sprs.push.us-east-1.prod/data/warehouse/model/user_counter/$feature_date user_counter

    python write_user_counter.py
    ret=$?
    alert $ret write_user_counter.py

    rm -r user_counter
}

function write_user_profile()
{
    file_exist s3://sprs.push.us-east-1.prod/data/warehouse/model/user_profile/$feature_date
    aws s3 sync s3://sprs.push.us-east-1.prod/data/warehouse/model/user_profile/$feature_date user_profile

    python write_user_profile.py
    ret=$?
    alert $ret write_user_profile.py

    rm -r user_profile
}

function write_data()
{
  cd write_pb
  write_ad_counter &
  write_ad_info &
  write_user_profile &
  write_user_counter &
}


write_data
