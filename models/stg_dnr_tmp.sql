SELECT
    main_order_id,
    from_unixtime(lgt_enquiry_time, 'yyyy-MM-dd') AS DNR_claim_time,
    lgt_enquiry_type AS DNR_type,
    lgt_enquiry_id AS LER_id,
    level1_label_name,
    level2_label_name,
    level3_label_name,
    NULL AS NRR_type,
    NULL AS top_review_v3_lv1_label,
    NULL AS top_review_v3_lv2_label,
    NULL AS top_review_v3_lv3_label,
    is_refund_success
FROM ecom.dm_lgt_omnichannel_enquiry_day_ttp2va_fordes
WHERE date = '{{ date }}'
  AND (LCASE(level3_label_name) IN (
            'delivered but not received arbitration',
            'delivered but not received',
            'delivered but not received order',
            'delivered but not received order'
        )
        OR first_qf_reason IN ('ecom_order_delivered_refund_reason_not_received')
      )

UNION ALL

SELECT
    main_order_id,
    from_unixtime(
        unix_timestamp(user_review_review_date, 'yyyyMMdd'),
        'yyyy-MM-dd'
    ) AS DNR_claim_time,
    'NRR' AS DNR_type,
    NULL AS LER_id,
    NULL AS level1_label_name,
    NULL AS level2_label_name,
    NULL AS level3_label_name,
    is_negative_review_t_x AS NRR_type,
    top_review_v3_lv1_label,
    top_review_v3_lv2_label,
    top_review_v3_lv3_label,
    NULL AS is_refund_success
FROM ecom.app_order_product_reviews_info_df_ttp2va_fordes
WHERE date = '{{ date }}'
  AND is_negative_review_t_x = 1
  AND top_review_v3_lv3_label IN ('Not received', 'WrongAddress')
