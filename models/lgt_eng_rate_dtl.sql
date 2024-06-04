{% set common_fields = get_common_fields() %}
{% set omnichannel_enquiry_day_ttp2va_fordes = 'ecom.dm_lgt_omnichannel_enquiry_day_ttp2va_fordes' %}

SELECT
    cast(t2.main_order_id as bigint) main_order_id,
    {% for field in common_fields %}
        t2.{{ field }}{% if not loop.last %},{% endif %}
    {% endfor %},
    FROM_UNIXTIME(adj_biz_time_15_30, 'yyyy-MM-dd') AS adj_biz_date_15_30,
    {{ calculate_tx_field("(lgt_enquiry_time - stocking_time) / 3600 / 24 <= IF(seller_type_name = 'Local', 15, 30)", "concat(lgt_enquiry_id, lgt_enquiry_type)", "NULL", "tx_lgt_enquiry") }},
    {{ calculate_tx_field("lgt_enquiry_type = 'Ticket' AND (lgt_enquiry_time - stocking_time) / 3600 / 24 <= IF(seller_type_name = 'Local', 15, 30)", "lgt_enquiry_id", "NULL", "tx_lgt_ticket") }},
    {{ calculate_tx_field("(lgt_enquiry_type = 'Ticket' AND user_type = 1 OR lgt_enquiry_type = 'Aftersale') AND (lgt_enquiry_time - stocking_time) / 3600 / 24 <= IF(seller_type_name = 'Local', 15, 30)", "concat(lgt_enquiry_id, lgt_enquiry_type)", "NULL", "tx_buyer_lgt_enquiry") }},
    {{ calculate_tx_field("lgt_enquiry_type = 'Ticket' AND user_type = 1 AND (lgt_enquiry_time - stocking_time) / 3600 / 24 <= IF(seller_type_name = 'Local', 15, 30)", "lgt_enquiry_id", "NULL", "tx_buyer_lgt_ticket") }},
    {{ calculate_tx_field("lgt_enquiry_type = 'Aftersale' AND (lgt_enquiry_time - stocking_time) / 3600 / 24 <= IF(seller_type_name = 'Local', 15, 30)", "lgt_enquiry_id", "NULL", "tx_lgt_aftersale") }}
FROM {{omnichannel_enquiry_day_ttp2va_fordes}} t1
LEFT JOIN pkg_dtl_main_order t2
    ON t1.main_order_id = t2.main_order_id
WHERE t1.date = '${date}'
  AND NOT (t1.seller_type_name = 'Crossborder' AND t1.shipment_wh_sub_type != 2)
