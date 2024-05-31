{% macro set_order_table() %}
    {% set order_table = 'ecom.dm_order_cl_info_limited_df_ttp2va_fordes' %}
    {{ order_table }}
{% endmacro %}

{% macro get_common_fields() %}
    {{ return ([
        "package_id",
        "provider_ids",
        "provider_names",
        "shipment_province_name",
        "recipient_province_name",
        "delivery_option_party_v2",
        "domestics_lane_name_list",
        "shop_id",
        "shop_name",
        "global_seller_type",
        "global_seller_id",
        "is_test",
        "delivery_option",
        "is_fake_l2l",
        "is_fake_l2l_gne",
        "ubo_type",
        "is_pop_la_tag",
        "used_shipping_partner",
        "warehouse_id",
        "zone",
        "governance_domain",
        "violation_label_code",
        "violation_create_date",
        "used_manifest",
        "is_ful_with_closest_all_fbt_wh",
        "is_ful_with_closest_ety_fbt_wh",
        "is_mall_order"
    ]) }}
{% endmacro %}
