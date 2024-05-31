{% set common_fields = get_common_fields() %}

    SELECT
        t1.ticket_channel,
        t1.user_type,
        {{ calculate_survey_fields(
            't1.ticket_channel',
            't1.rate',
            't1.im_survey_rate',
            't1.create_im_ticket_type',
            't1.is_pending72h_2_close',
            survey_submit_date_field('t1.ticket_channel', 't1.rate', 't1.im_survey_rate', 't1.create_im_ticket_type', 't1.is_pending72h_2_close'),
            survey_submit_volume_field('t1.ticket_channel', 't1.rate', 't1.im_survey_rate', 't1.create_im_ticket_type', 't1.is_pending72h_2_close'),
            survey_satisfied_volume_field('t1.ticket_channel', 't1.rate', 't1.im_survey_rate', 't1.create_im_ticket_type', 't1.is_pending72h_2_close')
        ) }},
        {% for field in common_fields %}
        t3.{{ field }}{% if not loop.last %},{% endif %}
    {% endfor %}
    FROM ies_cs.dwm_cs_ticket_ticket_detail_df_global t1
    LEFT JOIN ecom.dim_cs_ticket_us_logistics_label_df_va2ttp_fordes t2
        ON t2.date = '{{ date }}'
        AND lower(t1.level1_label_name) = lower(t2.level1_label_name)
        AND lower(t1.level2_label_name) = lower(t2.level2_label_name)
        AND lower(t1.level3_label_name) = lower(t2.level3_label_name)
    LEFT JOIN pkg_dtl_main_order t3
        ON t1.main_order_id = t3.main_order_id
    WHERE t1.p_date = '{{ date }}'
        AND t1.idc_code = 'TTP'
        AND t1.ticket_type_new IN (1, 3)
        AND t1.user_type IN (1, 2)
        AND t1.ticket_channel IN (1, 3)
        AND (t2.is_logistics = 1 OR (t3.delivery_option_party_v2 = 'FBT' AND t2.is_fbt_only = 1))
        AND NOT (t1.seller_type_name = 'Crossborder' AND t1.shipment_wh_sub_type != 2) -- filter cross border non_overseas