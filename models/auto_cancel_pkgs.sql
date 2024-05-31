{% set common_fields = get_common_fields() %}
{% set order_day_ttp2va_fordes = 'ecom.dm_ful_sku_order_day_ttp2va_fordes' %}

SELECT
    {% for field in common_fields %}
        t1.{{ field }}{% if not loop.last %},{% endif %}
    {% endfor %},
    t2.platform_non_tts_cancel_sla_date,
    t2.cancel_type
FROM base_data t1
JOIN (
    SELECT
        package_id,
        MAX(CASE
            WHEN cancel_reverse_from_code IN ('4') AND cancel_reason_text = 'Automatically canceled due to collection time out' THEN 'AC'
            WHEN cancel_reverse_from_code IN ('0') THEN 'N'
            ELSE 'Cancel'
        END) AS cancel_type,
        MAX(from_unixtime(platform_non_tts_cancel_sla, 'yyyy-MM-dd')) AS platform_non_tts_cancel_sla_date
    FROM {{ order_day_ttp2va_fordes }}
    WHERE date = MAX_PT({{order_day_ttp2va_fordes}})
    GROUP BY package_id
) t2
    ON t1.package_id = t2.package_id
