{% set ts_pairs = [
    ('pickup_success_back_ts', 'pickup_success_ts'),
    ('drop_off_intransit_back_ts', 'drop_off_intransit_ts'),
    ('drop_off_success_back_ts', 'drop_off_success_ts'),
    ('trans_sign_back_ts', 'trans_sign_ts'),
    ('sc_inbound_back_ts', 'sc_inbound_ts'),
    ('sc_operate_back_ts', 'sc_operate_ts'),
    ('sc_outbound_back_ts', 'sc_outbound_ts'),
    ('station_in_back_ts', 'station_in_ts'),
    ('station_out_back_ts', 'station_out_ts'),
    ('delivery_start_back_ts', 'delivery_start_ts'),
    ('pop_arrived_back_ts', 'pop_arrived_ts'),
    ('confirm_back_ts', 'confirm_time'),
    ('sc_sign_back_ts', 'sc_sign_ts'),
    ('transport_intransit_back_ts', 'transport_intransit_ts')
] %}

{% set usps_pairs = [
    ('delivery_start_back_ts', 'delivery_start_ts'),
    ('confirm_back_ts', 'confirm_time'),
    ('sc_sign_back_ts', 'sc_sign_ts')
] %}
select
        a.package_id as asc_rank,
        a.package_id,
        main_order_id_list,
        global_seller_type,
        operation_country,
        provider_ids,
        CASE WHEN provider_ids IN (
                 '7117858858072016686',
                 '7117859084333745966',
                 '7129720299146184490',
                 '7132721393761781550',
                 '7248600717110282027',
                 '7137498126482409259',
                 '7212608507307099946'
             ) THEN provider_ids
             ELSE '9999999999'
        END AS new_provider_ids,
        provider_names,
        biz_source,
        is_cod,
        is_pre_sale,
        shop_id,
        shop_name,
        global_seller_id,
        shipment_province_name,
        null as recipient_province_name,
        delivery_option,
        delivery_option_name,
        pickup_type,
        domestics_lane_name_list,
        coalesce(fbt_sub_warehouse_id, shipment_warehouse_id) as warehouse_id,
        from_unixtime(create_time, 'yyyy-MM-dd') as create_date,
        from_unixtime(create_time, 'yyyy-MM-dd HH') as create_date_hour,
        from_unixtime(send_time, 'yyyy-MM-dd') as ship_date,
        from_unixtime(adjust_pickup_ts, 'yyyy-MM-dd') as collect_date,
        from_unixtime(adjust_pickup_ts_v2, 'yyyy-MM-dd') as collect_date_v2,
        from_unixtime(confirm_time, 'yyyy-MM-dd') as delivery_date,
        date_add(from_unixtime(create_time, 'yyyy-MM-dd'), 15) as stats_date,
        from_unixtime(finish_time, 'yyyy-MM-dd') as finish_date,
        from_unixtime(confirm_fail_time, 'yyyy-MM-dd') as confirm_fail_date,
        from_unixtime(predict_delivery_time_min, 'yyyy-MM-dd') as predict_delivery_date_min,
        from_unixtime(predict_delivery_time_max, 'yyyy-MM-dd') as predict_delivery_date_max,
        from_unixtime(pay_predict_delivery_time_min, 'yyyy-MM-dd') as pay_predict_delivery_date_min,
        from_unixtime(pay_predict_delivery_time_max, 'yyyy-MM-dd') as pay_predict_delivery_date_max,

        (send_time - create_time) / 3600 as create_to_ship_hrs,
        case when adjust_pickup_ts > create_time then (adjust_pickup_ts - create_time) / 3600 end as create_to_pickup_hrs,
        case when confirm_time > create_time then (confirm_time - create_time) / 3600 end as create_to_delivery_hrs,
        (finish_time - create_time) / 3600 as create_to_finish_hrs,
        case when send_time > 0 then (adjust_pickup_ts - send_time) / 3600 end as ship_to_pickup_hrs,
        case when confirm_time > send_time then (confirm_time - send_time) / 3600 end as ship_to_delivery_hrs,
        case when confirm_time > adjust_pickup_ts then (confirm_time - adjust_pickup_ts) / 3600 end as pickup_to_delivery_hrs,

        case
            when (package_status_l1 in (1100, 1200, 1300) or fulfill_status_name = 'CLOSE') and coalesce(send_time, 0) = 0
            then '发货前取消'
            when (package_status_l1 in (1100, 1200, 1300) or fulfill_status_name = 'CLOSE') and send_time > 0
            then '发货后取消'
            else '正常'
        end as is_cancel_before_rts,
        case
            when finish_time > 0 or package_status_l1_name in ('package_completed')
            then 1
            else 0
        end as is_finish,
        
        case when provider_names in ('USPS') then
            {{ calculate_trace_ontime_node_cnt(usps_pairs, false) }}
        when provider_names in ('Amazon Logistics') then
            {{ calculate_trace_ontime_node_cnt(ts_pairs, true) }}
        else
            {{ calculate_trace_ontime_node_cnt(ts_pairs, false) }}
        end as trace_ontime_node_cnt,
        
        case when provider_names in ('USPS') then
            {{ calculate_trace_ontime_node_cnt_fail(usps_pairs, false) }}
        when provider_names in ('Amazon Logistics') then
            {{ calculate_trace_ontime_node_cnt_fail(ts_pairs, true) }}
        else
            {{ calculate_trace_ontime_node_cnt_fail(ts_pairs, false) }}
        end as trace_ontime_node_cnt_fail,
        
        case when provider_names in ('USPS') then
            {{ calculate_trace_ontime_node_cnt_fail_seller(usps_pairs) }}
        when provider_names in ('Amazon Logistics') then
            0
        else
            {{ calculate_trace_ontime_node_cnt_fail_seller(ts_pairs) }}
        end as trace_ontime_node_cnt_fail_seller,
        
        case when provider_names in ('USPS') then
            {{ calculate_trace_back_node_cnt(usps_pairs, false) }}
        when provider_names in ('Amazon Logistics') then
            {{ calculate_trace_back_node_cnt(ts_pairs, true) }}
        else
            {{ calculate_trace_back_node_cnt(ts_pairs, false) }}
        end as trace_back_node_cnt,
        case
            when (back_time > 0 OR unreachable_returning_time > 0 OR unreachable_returned_wh_ts > 0)
            and coalesce(confirm_time, 0) = 0
            and coalesce(lost_time, 0) = 0
            and coalesce(broken_time, 0) = 0
            --  and coalesce(is_buyer_fault_cancellation, 0) = 0
            then 1
            else 0
        end as is_delivery_failed,
        -- is_buyer_fault_cancellation,
        lost_time,
        broken_time,
        unreachable_returned_wh_ts,
        first_lgt_complain_level3_name,
        finish_time,
        package_status_l1,
        pickup_success_ts,
        drop_off_success_ts,
        sc_sign_ts,
        sc_outbound_ts,

        fbt_create_to_outbound_ontime,
        fbt_notify_success_to_outbound_ontime,
        fbt_create_to_outbound_sla_date,
        fbt_notify_success_to_outbound_sla_date,

        seller_payable_amt,
        vendor_freight_amt,
        user_pay_really,
        total_subsidy_amt,
        paid_price_amt_usd,
        shipping_fee_shipper,
        shipping_fee_sale_price,

        tracking_nos,
        actual_weight,
        estimate_weight_kg,
        unreachable_returning_time,
        confirm_time,
        create_time,
        send_time,
        adjust_pickup_ts,
        adjust_pickup_ts_v2,
        book_pickup_start_ts,
        book_pickup_end_ts,
        pickup_fail_time,
        is_delivery_ontime_old,
        confirm_fail_time,
        promise_date_max_old,
        back_time,
        business_typ2 as business_type,
        delivery_option_party,
        delivery_option_party as delivery_option_party_v2,
        shipment_country_name,
        package_status_l1_name,
        sku_weight,
        is_test,
        is_fake_l2l,
        is_fake_l2l_gne,
        ubo_type,
        is_pop_la_tag,
        used_shipping_partner,
        carrier_service_level,
        zone,
        governance_domain,
        violation_label_code,
        violation_create_date,
        if(handover_manifest_id is not null, 1, 0) as used_manifest,
        is_ful_with_closest_all_fbt_wh,
        is_ful_with_closest_ety_fbt_wh,
        b.is_mall_order
    from ecom.app_lgt_package_detail_day_ttp2va_fordes a
    left join order_dims b
        on a.package_id = b.package_id
    where
        date = '{{ date }}'
        and logistics_type = 1 -- forward logistics
        and is_test = 0
        and biz_source <> 2 -- sample orders
        and shop_name not in ('Mooncake Test Shop', 'Discount Dudes', 'EK Creative', 'Heather Munion Jewelry', 'e.l.f. Cosmetics DEV')