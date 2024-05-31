{% set common_fields = get_common_fields() %}
{% set exp_detail_day = 'ecom.app_lgt_us_exp_detail_day' %}

SELECT
        {% for field in common_fields %}
            t2.{{ field }},
        {% endfor %}
        t1.is_altogether_damage_old,
        IF(t1.is_cancel_before_rts != '发货前取消', 1, 0) AS pkg_cnt,
        from_unixtime(unix_timestamp(t1.stat_date, 'yyyyMMdd'), 'yyyy-MM-dd') AS stat_date
    FROM ecom.app_lgt_us_exp_detail_day t1
    JOIN base_data t2
        ON t1.package_id = t2.package_id
    WHERE t1.date = MAX_PT({{exp_detail_day}})