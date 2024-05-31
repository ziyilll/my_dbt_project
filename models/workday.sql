{% set table_name = 'ecom.dim_lgt_us_holiday_df' %}
{% set columns = ['in2days', 'in3days', 'in5days', 'in7days', 'in8days', 'in10days', 'in13days'] %}
    select
        regexp_replace(p_date,'#','') as p_date,
        p_day,
        regexp_replace(provider_id,'#','') as provider_ids,
        provider_name,
        holiday,
        is_skip,
        {% for col in columns %}
        regexp_replace({{ col }},'#','')  as datetime_{{ col }},
        {% endfor %}
        {% for col in columns %}
        regexp_replace({{ col }},'#|23:59:59| ','')  as date_{{ col }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ table_name }}
    where date=max_pt({{ table_name }})