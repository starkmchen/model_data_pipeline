#!/bin/bash
set -x

now_hour=$(date -d "1 hour ago" +%Y%m%d%H)
now_day=$(date -d "1 hour ago" +%Y%m%d)

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

function ad_counter_day_file()
{
  fname=$(aws s3 ls s3://sprs.push.us-east-1.prod/data/warehouse/model/ad_counter/ | tail -n 1 | awk '{print $2}')
  full_fname=s3://sprs.push.us-east-1.prod/data/warehouse/model/ad_counter/$fname
  aws s3 sync $full_fname ad_counter_day_dir_$now_hour
  cat ad_counter_day_dir_$now_hour/* > ad_counter_day.$now_hour
  rm -r ad_counter_day_dir_$now_hour
}

function download_new_file()
{
file_exist s3://sprs.push.us-east-1.prod/data/warehouse/model/nt_cpi_imp_click_hourly/$now_hour
aws s3 sync s3://sprs.push.us-east-1.prod/data/warehouse/model/nt_cpi_imp_click_hourly/$now_hour hour_data/imp_click/$now_hour

file_exist s3://sprs.push.us-east-1.prod/data/warehouse/model/repostback_install/$now_hour
aws s3 sync s3://sprs.push.us-east-1.prod/data/warehouse/model/repostback_install/$now_hour hour_data/attr_install/$now_hour
}

function merge_hourly_file()
{
rm -f attr_install.$now_hour
for fname in $(ls hour_data/attr_install | tail -n 24);do
  cat hour_data/attr_install/$fname/* | awk -F '\t' '{if($5 != "com.lenovo.anyshare.gps") print $0}' >> attr_install.$now_hour
done

rm -f imp_click.$now_hour
for fname in $(ls hour_data/imp_click | tail -n 24);do
  cat hour_data/imp_click/$fname/* >> imp_click.$now_hour
done
}

function rm_file()
{
  rm ad_counter_day.$now_hour
  rm attr_install.$now_hour
  rm imp_click.$now_hour
  rm ad_counter_$now_hour.pb
  rm ad_counter_$now_hour.pb.md5
  find hour_data/imp_click/* -mtime +1 | xargs rm -r
  find hour_data/attr_install/* -mtime +1 | xargs rm -r
}

function process()
{
python update_ad_counter_hourly.py ad_counter_day.$now_hour imp_click.$now_hour attr_install.$now_hour ad_counter_$now_hour.pb
ret=$?
alert $ret update_ad_counter_hourly.py

if [ $ret -ne 0 ];then
   return
fi

md5sum ad_counter_$now_hour.pb | awk '{print $1}' > ad_counter_$now_hour.pb.md5
aws s3 cp ad_counter_$now_hour.pb s3://sprs-ads-sg1/prod/feature/ad_counter/$now_day/ad_counter_$now_hour.pb
aws s3 cp ad_counter_$now_hour.pb.md5 s3://sprs-ads-sg1/prod/feature/ad_counter/$now_day/ad_counter_$now_hour.pb.md5
aws s3 cp ad_counter_$now_hour.pb s3://sprs-ads-sg1/prod/feature/ad_counter/$now_day/ad_counter.pb
aws s3 cp ad_counter_$now_hour.pb.md5 s3://sprs-ads-sg1/prod/feature/ad_counter/$now_day/ad_counter.pb.md5
}

download_new_file
ad_counter_day_file
merge_hourly_file
process
rm_file
