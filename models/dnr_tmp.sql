SELECT
    main_order_id,
    MAX(DNR_claim_time) AS DNR_claim_time,
    MAX(DNR_type) AS DNR_type,
    MAX(LER_id) AS LER_id,
    MAX(level1_label_name) AS level1_label_name,
    MAX(level2_label_name) AS level2_label_name,
    MAX(level3_label_name) AS level3_label_name,
    MAX(NRR_type) AS NRR_type,
    MAX(top_review_v3_lv1_label) AS top_review_v3_lv1_label,
    MAX(top_review_v3_lv2_label) AS top_review_v3_lv2_label,
    MAX(top_review_v3_lv3_label) AS top_review_v3_lv3_label,
    MAX(is_refund_success) AS is_refund_success
FROM (
    {{ ref('stg_dnr_tmp') }}
)
GROUP BY main_order_id