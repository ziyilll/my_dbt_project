{% set common_fields = get_common_fields() %}

SELECT
        {% for field in common_fields %}
            t2.{{ field }},
        {% endfor %}
        concat(cast(t1.main_order_id as string), '_', cast(t1.sku_id as string)) AS sku_main_order_id,
        date_add(adj_stocking_date_timezone, 15) AS stocking_date_plus_15,
        is_cancel,
        is_negative_review_t_x,
        is_reviewed_t_x,
        top_review_v3_lv1_label,
        top_review_v3_lv2_label,
        top_review_v3_lv3_label
    FROM ecom.app_order_product_reviews_info_df_ttp2va_fordes t1
    LEFT JOIN pkg_dtl_main_order t2
        ON t1.main_order_id = t2.main_order_id
    WHERE t1.date = '${date}'
      AND NOT (t1.seller_type_name = 'Crossborder' AND t1.shipment_wh_sub_type != 2)