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

function download_pkg_category()
{
  fname=$(aws s3 ls s3://sprs.push.us-east-1.prod/data/warehouse/model/pkg_info/ | tail -n 1 | awk '{print $2}')
  full_fname=s3://sprs.push.us-east-1.prod/data/warehouse/model/pkg_info/$fname
  aws s3 sync $full_fname pkg_info_dir.${now_hour}
  cat pkg_info_dir.${now_hour}/* > pkg_info.${now_hour}
  rm -r pkg_info_dir.${now_hour}
}

function process()
{
  python update_ad_info.py pkg_info.${now_hour} pkg_info.pb.${now_hour}

  ret=$?
  alert $ret update_ad_info.py

  if [ $ret -ne 0 ];then
      return
  fi

  md5sum pkg_info.pb.${now_hour} | awk '{print $1}' > pkg_info.pb.${now_hour}.md5
  aws s3 cp pkg_info.pb.${now_hour} s3://sprs-ads-sg1/pre/feature/pkg_info/$now_day/pkg_info.pb
  aws s3 cp pkg_info.pb.${now_hour}.md5 s3://sprs-ads-sg1/pre/feature/pkg_info/$now_day/pkg_info.pb.md5
  aws s3 cp pkg_info.pb.${now_hour} s3://sprs-ads-sg1/prod/feature/pkg_info/$now_day/pkg_info.pb
  aws s3 cp pkg_info.pb.${now_hour}.md5 s3://sprs-ads-sg1/prod/feature/pkg_info/$now_day/pkg_info.pb.md5
}


function rmfile()
{
  rm pkg_info.pb.${now_hour}
  rm pkg_info.${now_hour}
  rm pkg_info.pb.${now_hour}.md5
}


download_pkg_category
process
rmfile
