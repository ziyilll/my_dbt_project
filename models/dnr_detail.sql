{% set common_fields = get_common_fields() %}
SELECT
    {% for field in common_fields %}
    t1.{{ field }},
    {% endfor %}
    t1.main_order_id,
    t1.stats_date,
    t2.main_order_id AS DNR_main_order_id
FROM pkg_dtl_main_order t1
LEFT JOIN dnr_tmp t2
    ON t1.main_order_id = t2.main_order_id