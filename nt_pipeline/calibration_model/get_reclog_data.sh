#!/bin/bash
day=$(date -d "1 day ago" +%Y%m%d)


sql="select a.beyla_id, a.ad_package_name, a.ad_id, a.c_id, b.pctr, b.pcvr, a.bid_price, a.is_click, a.is_attr_install from
(select rid as request_id, beyla_id, ad_package_name, cast(ad_id as string) as ad_id, cast(c_id as string) as c_id, is_click, is_attr_install, bid_price from sprs_ad_dwd.imp_click_install_temp where dt = '$day' and  package_name = 'video.watchit') a
join (select request_id, creative_id as c_id, camp_id as ad_id, app_id as ad_package_name, pctr, pcvr from sprs_ad_dwd.dwd_nt_rec_log_inc_hourly where dt = '$day' and  package_name = 'video.watchit')b
on a.request_id = b.request_id and a.c_id = b.c_id and a.ad_id = b.ad_id and a.ad_package_name =     
b.ad_package_name"


hive -S -e "$sql" > rec_log_data/$day
