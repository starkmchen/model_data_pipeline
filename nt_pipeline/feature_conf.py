ad_count_feature_conf = (
'c_id',
'ad_id',
'ad_package_name',
'ad_package_category',
'pos_id#c_id',
'pos_id#ad_id',
'pos_id#ad_package_name',
'pos_id#ad_package_category',
)


user_count_feature_conf = (
'user_id',
'user_id#ad_id',
'user_id#c_id',
'user_id#ad_package_name',
'user_id#ad_package_category',
'user_id#pos_id#ad_id',
'user_id#pos_id#c_id',
'user_id#pos_id#ad_package_name',
'user_id#pos_id#ad_package_category',
)
